//! QUIC gRPC server implementation
//!
//! This module provides `QuicGrpcServer` which accepts QUIC connections
//! and dispatches RPC calls to the appropriate service methods.

use std::{marker::PhantomData, sync::Arc};

use gm_quic::prelude::{Connection, QuicListeners};
use prost::Message;
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, error};

use crate::{
    cmd::{Command, CommandExecutor},
    role_change::RoleChange,
    rpc::{
        CurpError, CurpErrorWrapper,
        quic_transport::codec::{
            Frame, FrameReader, FrameWriter, read_request_header, status_error, status_ok,
        },
    },
    server::Rpc,
};

use super::super::{CurpService, InnerCurpService, Metadata};

/// QUIC gRPC server
///
/// Generic over the same type parameters as `Rpc<C, CE, RC>`.
/// Internally erases to `Arc<dyn CurpService>` and `Arc<dyn InnerCurpService>`.
pub struct QuicGrpcServer<C, CE, RC>
where
    C: Command,
    CE: CommandExecutor<C>,
    RC: RoleChange,
{
    /// Service for external protocol
    service: Arc<dyn CurpService>,
    /// Service for internal protocol
    inner_service: Arc<dyn InnerCurpService>,
    /// Phantom data for type parameters
    _phantom: PhantomData<(C, CE, RC)>,
}

impl<C, CE, RC> QuicGrpcServer<C, CE, RC>
where
    C: Command,
    CE: CommandExecutor<C>,
    RC: RoleChange,
{
    /// Create a new QUIC gRPC server from an `Rpc` instance
    #[inline]
    pub fn new(rpc: Rpc<C, CE, RC>) -> Self {
        Self {
            service: Arc::new(rpc.clone()),
            inner_service: Arc::new(rpc),
            _phantom: PhantomData,
        }
    }

    /// Start serving on the given listeners
    ///
    /// This method runs the accept loop and dispatches incoming streams
    /// to the appropriate RPC handlers.
    pub async fn serve(self, listeners: QuicListeners) -> Result<(), CurpError> {
        let service = self.service;
        let inner_service = self.inner_service;

        loop {
            match listeners.accept().await {
                Ok((conn, server_name, pathway, link)) => {
                    debug!(
                        "Accepted QUIC connection from {:?} to server {}",
                        pathway, server_name
                    );
                    let _ = link; // Link not needed for our use case

                    let svc = Arc::clone(&service);
                    let inner_svc = Arc::clone(&inner_service);

                    let _handle = tokio::spawn(async move {
                        Self::handle_connection(conn, svc, inner_svc).await;
                    });
                }
                Err(e) => {
                    error!("Listeners shutdown: {e}");
                    break;
                }
            }
        }

        Ok(())
    }

    /// Handle a single connection
    async fn handle_connection(
        conn: Connection,
        service: Arc<dyn CurpService>,
        inner_service: Arc<dyn InnerCurpService>,
    ) {
        loop {
            match conn.accept_bi_stream().await {
                Ok((stream_id, (recv, send))) => {
                    let _ = stream_id;
                    let svc = Arc::clone(&service);
                    let inner_svc = Arc::clone(&inner_service);

                    let _handle = tokio::spawn(async move {
                        if let Err(e) = Self::handle_stream(send, recv, svc, inner_svc).await {
                            debug!("stream handler error: {e:?}");
                        }
                    });
                }
                Err(e) => {
                    debug!("accept stream error: {e}");
                    break;
                }
            }
        }
    }

    /// Handle a single bidirectional stream
    async fn handle_stream<S, R>(
        send: S,
        mut recv: R,
        service: Arc<dyn CurpService>,
        inner_service: Arc<dyn InnerCurpService>,
    ) -> Result<(), CurpError>
    where
        S: AsyncWrite + Unpin + Send + 'static,
        R: AsyncRead + Unpin + Send + 'static,
    {
        // Read request header
        let (path, meta_pairs) = read_request_header(&mut recv).await?;
        let meta = Metadata::from_pairs(meta_pairs);

        debug!("QUIC RPC: {path}");

        // Dispatch based on path
        let result = Self::dispatch(&path, recv, &meta, &service, &inner_service).await;

        // Write response
        let mut writer = FrameWriter::new(send);

        match result {
            Ok(response_bytes) => {
                writer.write_frame(&Frame::Data(response_bytes)).await?;
                writer
                    .write_frame(&Frame::Status {
                        code: status_ok(),
                        details: vec![],
                    })
                    .await?;
            }
            Err(e) => {
                let wrapper = CurpErrorWrapper { err: Some(e) };
                let details = wrapper.encode_to_vec();
                writer
                    .write_frame(&Frame::Status {
                        code: status_error(),
                        details,
                    })
                    .await?;
            }
        }

        Ok(())
    }

    /// Dispatch RPC call based on path
    async fn dispatch<R>(
        path: &str,
        recv: R,
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
        inner_service: &Arc<dyn InnerCurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin + Send + 'static,
    {
        // Protocol service paths
        match path {
            "/commandpb.Protocol/FetchCluster" => {
                Self::handle_fetch_cluster(recv, service).await
            }
            "/commandpb.Protocol/FetchReadState" => {
                Self::handle_fetch_read_state(recv, service).await
            }
            "/commandpb.Protocol/Record" => Self::handle_record(recv, meta, service).await,
            "/commandpb.Protocol/ReadIndex" => Self::handle_read_index(meta, service).await,
            "/commandpb.Protocol/Shutdown" => {
                Self::handle_shutdown(recv, meta, service).await
            }
            "/commandpb.Protocol/ProposeConfChange" => {
                Self::handle_propose_conf_change(recv, meta, service).await
            }
            "/commandpb.Protocol/Publish" => Self::handle_publish(recv, meta, service).await,
            "/commandpb.Protocol/MoveLeader" => Self::handle_move_leader(recv, service).await,
            // Inner protocol service paths
            "/inner_messagepb.InnerProtocol/AppendEntries" => {
                Self::handle_append_entries(recv, inner_service).await
            }
            "/inner_messagepb.InnerProtocol/Vote" => {
                Self::handle_vote(recv, inner_service).await
            }
            "/inner_messagepb.InnerProtocol/TriggerShutdown" => {
                Self::handle_trigger_shutdown(inner_service).await
            }
            "/inner_messagepb.InnerProtocol/TryBecomeLeaderNow" => {
                Self::handle_try_become_leader_now(inner_service).await
            }
            // Streaming RPCs need special handling
            "/commandpb.Protocol/ProposeStream" => {
                Err(CurpError::internal("ProposeStream requires streaming handler"))
            }
            "/commandpb.Protocol/LeaseKeepAlive" => {
                Err(CurpError::internal("LeaseKeepAlive requires streaming handler"))
            }
            "/inner_messagepb.InnerProtocol/InstallSnapshot" => {
                Err(CurpError::internal("InstallSnapshot requires streaming handler"))
            }
            _ => Err(CurpError::internal(format!("unknown path: {path}"))),
        }
    }

    /// Read request data from stream
    async fn read_request<R, Req>(recv: R) -> Result<Req, CurpError>
    where
        R: AsyncRead + Unpin,
        Req: Message + Default,
    {
        let mut reader = FrameReader::new_unary_request(recv);
        let frame = reader.read_frame().await?;

        let data = match frame {
            Frame::Data(d) => d,
            _ => return Err(CurpError::internal("expected DATA frame")),
        };

        // Read END frame
        let end_frame = reader.read_frame().await?;
        if !matches!(end_frame, Frame::End) {
            return Err(CurpError::internal("expected END frame"));
        }

        Req::decode(data.as_slice())
            .map_err(|e| CurpError::internal(format!("decode request error: {e}")))
    }

    // Handler implementations

    async fn handle_fetch_cluster<R>(
        recv: R,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::FetchClusterRequest;

        let req: FetchClusterRequest = Self::read_request(recv).await?;
        let resp = service.fetch_cluster(req)?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_fetch_read_state<R>(
        recv: R,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::FetchReadStateRequest;

        let req: FetchReadStateRequest = Self::read_request(recv).await?;
        let resp = service.fetch_read_state(req)?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_record<R>(
        recv: R,
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::RecordRequest;

        let req: RecordRequest = Self::read_request(recv).await?;
        let resp = service.record(req, meta.clone())?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_read_index(
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError> {
        let resp = service.read_index(meta.clone())?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_shutdown<R>(
        recv: R,
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::ShutdownRequest;

        let req: ShutdownRequest = Self::read_request(recv).await?;
        let resp = service.shutdown(req, meta.clone()).await?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_propose_conf_change<R>(
        recv: R,
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::ProposeConfChangeRequest;

        let req: ProposeConfChangeRequest = Self::read_request(recv).await?;
        let resp = service.propose_conf_change(req, meta.clone()).await?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_publish<R>(
        recv: R,
        meta: &Metadata,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::PublishRequest;

        let req: PublishRequest = Self::read_request(recv).await?;
        let resp = service.publish(req, meta.clone())?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_move_leader<R>(
        recv: R,
        service: &Arc<dyn CurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::MoveLeaderRequest;

        let req: MoveLeaderRequest = Self::read_request(recv).await?;
        let resp = service.move_leader(req).await?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_append_entries<R>(
        recv: R,
        service: &Arc<dyn InnerCurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::AppendEntriesRequest;

        let req: AppendEntriesRequest = Self::read_request(recv).await?;
        let resp = service.append_entries(req)?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_vote<R>(
        recv: R,
        service: &Arc<dyn InnerCurpService>,
    ) -> Result<Vec<u8>, CurpError>
    where
        R: AsyncRead + Unpin,
    {
        use crate::rpc::VoteRequest;

        let req: VoteRequest = Self::read_request(recv).await?;
        let resp = service.vote(req)?;
        Ok(resp.encode_to_vec())
    }

    async fn handle_trigger_shutdown(
        service: &Arc<dyn InnerCurpService>,
    ) -> Result<Vec<u8>, CurpError> {
        use crate::rpc::proto::inner_messagepb::TriggerShutdownResponse;

        service.trigger_shutdown()?;
        Ok(TriggerShutdownResponse::default().encode_to_vec())
    }

    async fn handle_try_become_leader_now(
        service: &Arc<dyn InnerCurpService>,
    ) -> Result<Vec<u8>, CurpError> {
        use crate::rpc::proto::inner_messagepb::TryBecomeLeaderNowResponse;

        service.try_become_leader_now().await?;
        Ok(TryBecomeLeaderNowResponse::default().encode_to_vec())
    }
}

impl<C, CE, RC> std::fmt::Debug for QuicGrpcServer<C, CE, RC>
where
    C: Command,
    CE: CommandExecutor<C>,
    RC: RoleChange,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicGrpcServer").finish_non_exhaustive()
    }
}
