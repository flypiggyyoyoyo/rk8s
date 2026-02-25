use std::{fmt::Debug, pin::Pin, sync::Arc};

use engine::SnapshotAllocator;
use futures::StreamExt;
use tokio::sync::broadcast;
use tonic::transport::ClientTlsConfig;
use tracing::instrument;
use utils::{config::CurpConfig, task_manager::TaskManager};
use self::curp_node::CurpNode;
pub use self::{
    conflict::{spec_pool_new::SpObject, uncommitted_pool::UcpObject},
    raw_curp::RawCurp,
};
use crate::rpc::{OpResponse, RecordRequest, RecordResponse};
use crate::{
    cmd::{Command, CommandExecutor},
    members::{ClusterInfo, ServerId},
    role_change::RoleChange,
    rpc::{
        AppendEntriesRequest, AppendEntriesResponse, FetchClusterRequest, FetchClusterResponse,
        FetchReadStateRequest, FetchReadStateResponse, InstallSnapshotRequest,
        InstallSnapshotResponse, LeaseKeepAliveMsg, MoveLeaderRequest, MoveLeaderResponse,
        ProposeConfChangeRequest, ProposeConfChangeResponse, ProposeRequest, PublishRequest,
        PublishResponse, ShutdownRequest, ShutdownResponse, TriggerShutdownRequest,
        TriggerShutdownResponse, TryBecomeLeaderNowRequest, TryBecomeLeaderNowResponse,
        VoteRequest, VoteResponse,
    },
};
use crate::rpc::{ReadIndexRequest, ReadIndexResponse};

/// Command worker to do execution and after sync
mod cmd_worker;

/// Raw Curp
mod raw_curp;

/// Command board is the buffer to store command execution result
mod cmd_board;

/// Conflict pools
pub mod conflict;

/// Background garbage collection for Curp server
mod gc;

/// Curp Node
mod curp_node;

/// Storage
mod storage;

/// Lease Manager
mod lease_manager;

/// Curp metrics
mod metrics;

pub use storage::{StorageApi, StorageError, db::DB};

/// The Rpc Server to handle rpc requests
///
/// This Wrapper is introduced due to the `MadSim` rpc lib
#[derive(Debug)]
pub struct Rpc<C: Command, CE: CommandExecutor<C>, RC: RoleChange> {
    /// The inner server is wrapped in an Arc so that its state can be shared while cloning the rpc wrapper
    inner: Arc<CurpNode<C, CE, RC>>,
}

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> Clone for Rpc<C, CE, RC> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

// ============================================================================
// Tonic Protocol/InnerProtocol adapter impls
//
// These delegate to CurpService/InnerCurpService and will be removed in Phase 4
// when xline migrates to QuicGrpcServer.
// ============================================================================

#[tonic::async_trait]
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::Protocol for Rpc<C, CE, RC> {
    type ProposeStreamStream = Pin<Box<dyn futures::Stream<Item = Result<OpResponse, tonic::Status>> + Send>>;

    #[instrument(skip_all, name = "propose_stream")]
    async fn propose_stream(
        &self,
        request: tonic::Request<ProposeRequest>,
    ) -> Result<tonic::Response<Self::ProposeStreamStream>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        let req = request.into_inner();
        let stream = crate::rpc::CurpService::propose_stream(self, req, meta)
            .await
            .map_err(tonic::Status::from)?;
        let mapped = stream.map(|r| r.map_err(tonic::Status::from));
        Ok(tonic::Response::new(Box::pin(mapped)))
    }

    #[instrument(skip_all, name = "record")]
    async fn record(
        &self,
        request: tonic::Request<RecordRequest>,
    ) -> Result<tonic::Response<RecordResponse>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        Ok(tonic::Response::new(
            crate::rpc::CurpService::record(self, request.into_inner(), meta)
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "read_index")]
    async fn read_index(
        &self,
        request: tonic::Request<ReadIndexRequest>,
    ) -> Result<tonic::Response<ReadIndexResponse>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        Ok(tonic::Response::new(
            crate::rpc::CurpService::read_index(self, meta)
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_shutdown")]
    async fn shutdown(
        &self,
        request: tonic::Request<ShutdownRequest>,
    ) -> Result<tonic::Response<ShutdownResponse>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        Ok(tonic::Response::new(
            crate::rpc::CurpService::shutdown(self, request.into_inner(), meta)
                .await
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_propose_conf_change")]
    async fn propose_conf_change(
        &self,
        request: tonic::Request<ProposeConfChangeRequest>,
    ) -> Result<tonic::Response<ProposeConfChangeResponse>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        Ok(tonic::Response::new(
            crate::rpc::CurpService::propose_conf_change(self, request.into_inner(), meta)
                .await
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_publish")]
    async fn publish(
        &self,
        request: tonic::Request<PublishRequest>,
    ) -> Result<tonic::Response<PublishResponse>, tonic::Status> {
        let meta = crate::rpc::Metadata::from_tonic_metadata(request.metadata());
        Ok(tonic::Response::new(
            crate::rpc::CurpService::publish(self, request.into_inner(), meta)
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_fetch_cluster")]
    async fn fetch_cluster(
        &self,
        request: tonic::Request<FetchClusterRequest>,
    ) -> Result<tonic::Response<FetchClusterResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            crate::rpc::CurpService::fetch_cluster(self, request.into_inner())
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_fetch_read_state")]
    async fn fetch_read_state(
        &self,
        request: tonic::Request<FetchReadStateRequest>,
    ) -> Result<tonic::Response<FetchReadStateResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            crate::rpc::CurpService::fetch_read_state(self, request.into_inner())
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_move_leader")]
    async fn move_leader(
        &self,
        request: tonic::Request<MoveLeaderRequest>,
    ) -> Result<tonic::Response<MoveLeaderResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            crate::rpc::CurpService::move_leader(self, request.into_inner())
                .await
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "lease_keep_alive")]
    async fn lease_keep_alive(
        &self,
        request: tonic::Request<tonic::Streaming<LeaseKeepAliveMsg>>,
    ) -> Result<tonic::Response<LeaseKeepAliveMsg>, tonic::Status> {
        let stream = request.into_inner();
        let curp_stream: Box<dyn futures::Stream<Item = Result<LeaseKeepAliveMsg, crate::rpc::CurpError>> + Send + Unpin> =
            Box::new(stream.map(|r| r.map_err(crate::rpc::CurpError::from)));
        Ok(tonic::Response::new(
            crate::rpc::CurpService::lease_keep_alive(self, curp_stream)
                .await
                .map_err(tonic::Status::from)?,
        ))
    }
}

#[tonic::async_trait]
impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::InnerProtocol
    for Rpc<C, CE, RC>
{
    #[instrument(skip_all, name = "curp_append_entries")]
    async fn append_entries(
        &self,
        request: tonic::Request<AppendEntriesRequest>,
    ) -> Result<tonic::Response<AppendEntriesResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            crate::rpc::InnerCurpService::append_entries(self, request.into_inner())
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_vote")]
    async fn vote(
        &self,
        request: tonic::Request<VoteRequest>,
    ) -> Result<tonic::Response<VoteResponse>, tonic::Status> {
        Ok(tonic::Response::new(
            crate::rpc::InnerCurpService::vote(self, request.into_inner())
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_trigger_shutdown")]
    async fn trigger_shutdown(
        &self,
        _request: tonic::Request<TriggerShutdownRequest>,
    ) -> Result<tonic::Response<TriggerShutdownResponse>, tonic::Status> {
        crate::rpc::InnerCurpService::trigger_shutdown(self)
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(TriggerShutdownResponse {}))
    }

    #[instrument(skip_all, name = "curp_install_snapshot")]
    async fn install_snapshot(
        &self,
        request: tonic::Request<tonic::Streaming<InstallSnapshotRequest>>,
    ) -> Result<tonic::Response<InstallSnapshotResponse>, tonic::Status> {
        let stream = request.into_inner();
        let curp_stream: Box<dyn futures::Stream<Item = Result<InstallSnapshotRequest, crate::rpc::CurpError>> + Send + Unpin> =
            Box::new(stream.map(|r| r.map_err(crate::rpc::CurpError::from)));
        Ok(tonic::Response::new(
            crate::rpc::InnerCurpService::install_snapshot(self, curp_stream)
                .await
                .map_err(tonic::Status::from)?,
        ))
    }

    #[instrument(skip_all, name = "curp_try_become_leader_now")]
    async fn try_become_leader_now(
        &self,
        _request: tonic::Request<TryBecomeLeaderNowRequest>,
    ) -> Result<tonic::Response<TryBecomeLeaderNowResponse>, tonic::Status> {
        crate::rpc::InnerCurpService::try_become_leader_now(self)
            .await
            .map_err(tonic::Status::from)?;
        Ok(tonic::Response::new(TryBecomeLeaderNowResponse {}))
    }
}

// ============================================================================
// Rpc constructors and methods
// ============================================================================

impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> Rpc<C, CE, RC> {
    /// New `Rpc`
    ///
    /// # Panics
    ///
    /// Panic if storage creation failed
    #[inline]
    #[allow(clippy::too_many_arguments)] // TODO: refactor this use builder pattern
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
    ) -> Self {
        Self::new_inner(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
            crate::rpc::TransportConfig::default(),
        )
    }

    /// Create a new `Rpc` with QUIC transport
    ///
    /// This only creates the `Rpc` instance. To start the QUIC server,
    /// call `QuicGrpcServer::new(rpc).serve(listeners)` separately.
    #[allow(dead_code)] // Will be used in integration tests
    pub fn new_with_quic(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
        quic_client: Arc<gm_quic::prelude::QuicClient>,
    ) -> Self {
        Self::new_inner(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
            crate::rpc::TransportConfig::Quic(
                quic_client,
                crate::rpc::quic_transport::channel::DnsFallback::Disabled,
            ),
        )
    }

    /// Create a new `Rpc` with QUIC transport and localhost DNS fallback (test only)
    ///
    /// Same as `new_with_quic` but enables localhost fallback for fake hostnames.
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn new_with_quic_for_test(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
        quic_client: Arc<gm_quic::prelude::QuicClient>,
    ) -> Self {
        Self::new_inner(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
            crate::rpc::TransportConfig::Quic(
                quic_client,
                crate::rpc::quic_transport::channel::DnsFallback::LocalhostForTest,
            ),
        )
    }

    /// Internal constructor with explicit transport configuration
    fn new_inner(
        cluster_info: Arc<ClusterInfo>,
        is_leader: bool,
        executor: Arc<CE>,
        snapshot_allocator: Box<dyn SnapshotAllocator>,
        role_change: RC,
        curp_cfg: Arc<CurpConfig>,
        storage: Arc<DB<C>>,
        task_manager: Arc<TaskManager>,
        client_tls_config: Option<ClientTlsConfig>,
        sps: Vec<SpObject<C>>,
        ucps: Vec<UcpObject<C>>,
        transport: crate::rpc::TransportConfig,
    ) -> Self {
        #[allow(clippy::panic)]
        let curp_node = match CurpNode::new_with_transport(
            cluster_info,
            is_leader,
            executor,
            snapshot_allocator,
            role_change,
            curp_cfg,
            storage,
            task_manager,
            client_tls_config,
            sps,
            ucps,
            transport,
        ) {
            Ok(n) => n,
            Err(err) => {
                panic!("failed to create curp service, {err:?}");
            }
        };

        Self {
            inner: Arc::new(curp_node),
        }
    }

    /// Get a subscriber for leader changes
    #[inline]
    #[must_use]
    pub fn leader_rx(&self) -> broadcast::Receiver<Option<ServerId>> {
        self.inner.leader_rx()
    }

    /// Get raw curp
    #[inline]
    #[must_use]
    pub fn raw_curp(&self) -> Arc<RawCurp<C, RC>> {
        self.inner.raw_curp()
    }
}

// ============================================================================
// QUIC transport service trait implementations
// ============================================================================

mod quic_service_impl {
    use std::sync::Arc;

    use async_trait::async_trait;
    use futures::{Stream, StreamExt};
    use utils::tracing::Extract;

    use crate::{
        cmd::{Command, CommandExecutor},
        response::ResponseSender,
        role_change::RoleChange,
        rpc::{
            AppendEntriesRequest, AppendEntriesResponse, CurpError, FetchClusterRequest,
            FetchClusterResponse, FetchReadStateRequest, FetchReadStateResponse,
            InstallSnapshotRequest, InstallSnapshotResponse, LeaseKeepAliveMsg, Metadata,
            MoveLeaderRequest, MoveLeaderResponse, OpResponse, ProposeConfChangeRequest,
            ProposeConfChangeResponse, ProposeRequest, PublishRequest, PublishResponse,
            ReadIndexResponse, RecordRequest, RecordResponse, ShutdownRequest, ShutdownResponse,
            VoteRequest, VoteResponse,
        },
    };

    use super::Rpc;

    #[async_trait]
    impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::CurpService
        for Rpc<C, CE, RC>
    {
        async fn propose_stream(
            &self,
            req: ProposeRequest,
            meta: Metadata,
        ) -> Result<Box<dyn Stream<Item = Result<OpResponse, CurpError>> + Send + Unpin>, CurpError>
        {
            let bypassed = meta.is_bypassed();
            let (tx, rx) = flume::bounded(2);
            let resp_tx = Arc::new(ResponseSender::new(tx));
            self.inner.propose_stream(&req, resp_tx, bypassed).await?;

            let stream = rx.into_stream().map(|r| r.map_err(CurpError::from));
            Ok(Box::new(stream))
        }

        fn record(&self, req: RecordRequest, _meta: Metadata) -> Result<RecordResponse, CurpError> {
            self.inner.record(&req).map_err(CurpError::from)
        }

        fn read_index(&self, _meta: Metadata) -> Result<ReadIndexResponse, CurpError> {
            self.inner.read_index().map_err(CurpError::from)
        }

        async fn shutdown(
            &self,
            req: ShutdownRequest,
            meta: Metadata,
        ) -> Result<ShutdownResponse, CurpError> {
            let bypassed = meta.is_bypassed();
            meta.extract_span();
            self.inner
                .shutdown(req, bypassed)
                .await
                .map_err(CurpError::from)
        }

        async fn propose_conf_change(
            &self,
            req: ProposeConfChangeRequest,
            meta: Metadata,
        ) -> Result<ProposeConfChangeResponse, CurpError> {
            let bypassed = meta.is_bypassed();
            meta.extract_span();
            self.inner
                .propose_conf_change(req, bypassed)
                .await
                .map_err(CurpError::from)
        }

        fn publish(
            &self,
            req: PublishRequest,
            meta: Metadata,
        ) -> Result<PublishResponse, CurpError> {
            let bypassed = meta.is_bypassed();
            meta.extract_span();
            self.inner.publish(req, bypassed).map_err(CurpError::from)
        }

        fn fetch_cluster(
            &self,
            req: FetchClusterRequest,
        ) -> Result<FetchClusterResponse, CurpError> {
            self.inner.fetch_cluster(req).map_err(CurpError::from)
        }

        fn fetch_read_state(
            &self,
            req: FetchReadStateRequest,
        ) -> Result<FetchReadStateResponse, CurpError> {
            self.inner.fetch_read_state(req).map_err(CurpError::from)
        }

        async fn move_leader(
            &self,
            req: MoveLeaderRequest,
        ) -> Result<MoveLeaderResponse, CurpError> {
            self.inner.move_leader(req).await.map_err(CurpError::from)
        }

        async fn lease_keep_alive(
            &self,
            stream: Box<dyn Stream<Item = Result<LeaseKeepAliveMsg, CurpError>> + Send + Unpin>,
        ) -> Result<LeaseKeepAliveMsg, CurpError> {
            // CurpNode::lease_keep_alive is generic over E: Error + 'static.
            // xlinerpc::Status implements Error, so convert CurpError → xlinerpc::Status.
            let status_stream =
                stream.map(|r| r.map_err(xlinerpc::status::Status::from));
            self.inner
                .lease_keep_alive(status_stream)
                .await
                .map_err(CurpError::from)
        }
    }

    #[async_trait]
    impl<C: Command, CE: CommandExecutor<C>, RC: RoleChange> crate::rpc::InnerCurpService
        for Rpc<C, CE, RC>
    {
        fn append_entries(
            &self,
            req: AppendEntriesRequest,
        ) -> Result<AppendEntriesResponse, CurpError> {
            self.inner.append_entries(&req).map_err(CurpError::from)
        }

        fn vote(&self, req: VoteRequest) -> Result<VoteResponse, CurpError> {
            self.inner.vote(&req).map_err(CurpError::from)
        }

        async fn install_snapshot(
            &self,
            stream: Box<
                dyn Stream<Item = Result<InstallSnapshotRequest, CurpError>> + Send + Unpin,
            >,
        ) -> Result<InstallSnapshotResponse, CurpError> {
            // CurpNode::install_snapshot is generic over E: Error + 'static.
            // xlinerpc::Status implements Error, so convert CurpError → xlinerpc::Status.
            let status_stream =
                stream.map(|r| r.map_err(xlinerpc::status::Status::from));
            self.inner
                .install_snapshot(status_stream)
                .await
                .map_err(CurpError::from)
        }

        fn trigger_shutdown(&self) -> Result<(), CurpError> {
            use crate::rpc::TriggerShutdownRequest;
            let _resp = self.inner.trigger_shutdown(TriggerShutdownRequest {});
            Ok(())
        }

        async fn try_become_leader_now(&self) -> Result<(), CurpError> {
            use crate::rpc::TryBecomeLeaderNowRequest;
            let _ = self
                .inner
                .try_become_leader_now(&TryBecomeLeaderNowRequest {})
                .await
                .map_err(CurpError::from)?;
            Ok(())
        }
    }
}
