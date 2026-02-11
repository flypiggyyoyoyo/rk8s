//! QUIC channel implementation
//!
//! This module provides `QuicChannel` which manages QUIC connections
//! and provides RPC call methods (unary, server-streaming, client-streaming).

use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use futures::{Stream, future::BoxFuture};
use gm_quic::prelude::{Connection, QuicClient, StreamReader, StreamWriter};
use prost::Message;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

use crate::rpc::CurpError;

use super::codec::{Frame, FrameReader, FrameWriter, status_error, status_ok};

/// QUIC channel for managing connections and RPC calls
pub struct QuicChannel {
    /// QUIC client for creating connections
    client: Arc<QuicClient>,
    /// Address list for round-robin selection
    addrs: Arc<RwLock<Vec<String>>>,
    /// Round-robin index for load balancing
    index: Arc<AtomicUsize>,
}

impl std::fmt::Debug for QuicChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuicChannel")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl QuicChannel {
    /// Create a new QUIC channel
    #[inline]
    pub fn new(client: Arc<QuicClient>) -> Self {
        Self {
            client,
            addrs: Arc::new(RwLock::new(Vec::new())),
            index: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Add an address to the connection pool
    ///
    /// The address format should be "host:port" or "quic://host:port"
    pub async fn add_addr(&self, addr: &str) -> Result<(), CurpError> {
        let mut addrs = self.addrs.write().await;
        if !addrs.contains(&addr.to_owned()) {
            addrs.push(addr.to_owned());
        }
        Ok(())
    }

    /// Remove an address from the connection pool
    pub async fn remove_addr(&self, addr: &str) {
        let mut addrs = self.addrs.write().await;
        addrs.retain(|a| a != addr);
    }

    /// Update addresses in the connection pool
    pub async fn update_addrs(&self, new_addrs: Vec<String>) -> Result<(), CurpError> {
        let mut addrs = self.addrs.write().await;
        *addrs = new_addrs;
        Ok(())
    }

    /// Get a connection using round-robin selection
    async fn get_connection(&self) -> Result<Connection, CurpError> {
        let addrs = self.addrs.read().await;
        if addrs.is_empty() {
            return Err(CurpError::RpcTransport(()));
        }

        let idx = self.index.fetch_add(1, Ordering::Relaxed) % addrs.len();
        let addr = addrs
            .get(idx)
            .ok_or_else(|| CurpError::internal("connection index out of bounds"))?
            .clone();
        drop(addrs);

        // Connect each time (gm-quic handles connection reuse internally)
        let addr_str = addr.strip_prefix("quic://").unwrap_or(&addr);

        // Try connect() first (does DNS resolution + connect).
        // If DNS fails, fall back to connected_to() with localhost resolution.
        // This supports both production (real DNS) and testing (fake hostnames).
        match self.client.connect(addr_str).await {
            Ok(conn) => Ok(conn),
            Err(e) => {
                // Parse server_name and port for fallback
                let (server_name, port_str) = addr_str.rsplit_once(':').ok_or_else(|| {
                    CurpError::internal(format!("invalid address format: {addr_str}"))
                })?;
                let port: u16 = port_str.parse().map_err(|_| {
                    CurpError::internal(format!("invalid port in address: {addr_str}"))
                })?;

                // Try resolving to localhost (127.0.0.1) as fallback
                let fallback_addr =
                    std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), port);
                tracing::warn!(
                    "DNS lookup failed for {server_name}:{port} ({e}), \
                     falling back to {fallback_addr} with SNI {server_name}"
                );
                self.client
                    .connected_to(server_name, [fallback_addr])
                    .map_err(|e2| {
                        CurpError::internal(format!("QUIC connect error: {e2} (DNS: {e})"))
                    })
            }
        }
    }

    /// Open a bidirectional stream on the connection
    async fn open_bi_stream(
        conn: &Connection,
    ) -> Result<(StreamReader, StreamWriter), CurpError> {
        let result = conn
            .open_bi_stream()
            .await
            .map_err(|e| CurpError::internal(format!("open stream error: {e}")))?;

        match result {
            Some((_stream_id, (reader, writer))) => Ok((reader, writer)),
            None => Err(CurpError::internal("stream concurrency limit reached")),
        }
    }

    /// Perform a unary RPC call
    pub async fn unary_call<Req, Resp>(
        &self,
        path: &str,
        req: Req,
        meta: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Resp, CurpError>
    where
        Req: Message,
        Resp: Message + Default,
    {
        let conn = self.get_connection().await?;

        tokio::time::timeout(timeout, async {
            // Open bidirectional stream
            let (recv_stream, send_stream) = Self::open_bi_stream(&conn).await?;

            let mut writer = FrameWriter::new(send_stream);
            let mut reader = FrameReader::new_unary_response(recv_stream);

            // Write request header
            writer.write_request_header(path, &meta).await?;

            // Write request data
            let req_bytes = req.encode_to_vec();
            writer.write_frame(&Frame::Data(req_bytes)).await?;
            writer.write_frame(&Frame::End).await?;

            // Shutdown write side
            let mut send_stream: StreamWriter = writer.into_inner();
            send_stream
                .shutdown()
                .await
                .map_err(|e| CurpError::internal(format!("shutdown stream error: {e}")))?;

            // Read response
            let frame = reader.read_frame().await?;
            let resp_bytes = match frame {
                Frame::Data(data) => data,
                Frame::Status { code, details } if code == status_error() => {
                    return Err(Self::decode_error(&details)?);
                }
                _ => {
                    return Err(CurpError::internal("unexpected frame in unary response"));
                }
            };

            // Read status
            let status_frame = reader.read_frame().await?;
            match status_frame {
                Frame::Status { code, details } => {
                    if code != status_ok() {
                        return Err(Self::decode_error(&details)?);
                    }
                }
                _ => {
                    return Err(CurpError::internal("expected STATUS frame"));
                }
            }

            // Decode response
            Resp::decode(resp_bytes.as_slice())
                .map_err(|e| CurpError::internal(format!("decode response error: {e}")))
        })
        .await
        .map_err(|_| CurpError::RpcTransport(()))?
    }

    /// Perform a server-streaming RPC call
    pub async fn server_streaming_call<Req, Resp>(
        &self,
        path: &str,
        req: Req,
        meta: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<Resp, CurpError>> + Send>>, CurpError>
    where
        Req: Message,
        Resp: Message + Default + Send + Unpin + 'static,
    {
        let conn = self.get_connection().await?;

        let (recv_stream, send_stream): (StreamReader, StreamWriter) =
            tokio::time::timeout(timeout, Self::open_bi_stream(&conn))
                .await
                .map_err(|_| CurpError::RpcTransport(()))??;

        let mut writer = FrameWriter::new(send_stream);

        // Write request header and data
        writer.write_request_header(path, &meta).await?;
        let req_bytes = req.encode_to_vec();
        writer.write_frame(&Frame::Data(req_bytes)).await?;
        writer.write_frame(&Frame::End).await?;

        // Shutdown write side
        let mut send_stream: StreamWriter = writer.into_inner();
        send_stream
            .shutdown()
            .await
            .map_err(|e| CurpError::internal(format!("shutdown stream error: {e}")))?;

        // Return stream that reads responses
        let reader = FrameReader::new_server_streaming(recv_stream);
        Ok(Box::pin(ServerStreamingResponse::<Resp>::new(reader, conn)))
    }

    /// Perform a client-streaming RPC call
    pub async fn client_streaming_call<Req, Resp>(
        &self,
        path: &str,
        stream: Pin<Box<dyn Stream<Item = Req> + Send>>,
        meta: Vec<(String, String)>,
        timeout: Duration,
    ) -> Result<Resp, CurpError>
    where
        Req: Message + 'static,
        Resp: Message + Default,
    {
        use futures::StreamExt;

        let conn = self.get_connection().await?;

        tokio::time::timeout(timeout, async {
            let (recv_stream, send_stream) = Self::open_bi_stream(&conn).await?;

            let mut writer = FrameWriter::new(send_stream);
            let mut reader = FrameReader::new_unary_response(recv_stream);

            // Write request header
            writer.write_request_header(path, &meta).await?;

            // Spawn a task to write all request messages concurrently.
            // The server may respond before the client finishes sending
            // (e.g., lease_keep_alive returns a client_id after the first message).
            let send_handle = tokio::spawn(async move {
                let mut stream = stream;
                while let Some(req) = stream.next().await {
                    let req_bytes = req.encode_to_vec();
                    if writer.write_frame(&Frame::Data(req_bytes)).await.is_err() {
                        break;
                    }
                }
                let _ = writer.write_frame(&Frame::End).await;
                let mut send_stream: StreamWriter = writer.into_inner();
                let _ = send_stream.shutdown().await;
            });

            // Read response (may arrive before sending completes)
            let frame = reader.read_frame().await?;
            let resp_bytes = match frame {
                Frame::Data(data) => data,
                Frame::Status { code, details } if code == status_error() => {
                    send_handle.abort();
                    return Err(Self::decode_error(&details)?);
                }
                _ => {
                    send_handle.abort();
                    return Err(CurpError::internal(
                        "unexpected frame in client-streaming response",
                    ));
                }
            };

            // Read status
            let status_frame = reader.read_frame().await?;
            send_handle.abort(); // Cancel sending once we have the full response
            match status_frame {
                Frame::Status { code, details } => {
                    if code != status_ok() {
                        return Err(Self::decode_error(&details)?);
                    }
                }
                _ => {
                    return Err(CurpError::internal("expected STATUS frame"));
                }
            }

            // Decode response
            Resp::decode(resp_bytes.as_slice())
                .map_err(|e| CurpError::internal(format!("decode response error: {e}")))
        })
        .await
        .map_err(|_| CurpError::RpcTransport(()))?
    }

    /// Connect to a single address (for discovery)
    pub async fn connect_single(addr: &str, client: Arc<QuicClient>) -> Result<Self, CurpError> {
        let channel = Self::new(client);
        channel.add_addr(addr).await?;
        Ok(channel)
    }

    /// Decode error from STATUS frame details
    fn decode_error(details: &[u8]) -> Result<CurpError, CurpError> {
        use crate::rpc::CurpErrorWrapper;

        if details.is_empty() {
            return Ok(CurpError::internal("unknown error"));
        }

        let wrapper = CurpErrorWrapper::decode(details)
            .map_err(|e| CurpError::internal(format!("decode error details: {e}")))?;

        Ok(wrapper
            .err
            .unwrap_or_else(|| CurpError::internal("missing error in wrapper")))
    }
}

/// Server-streaming response wrapper
///
/// Stores the in-flight `read_frame()` future across polls so that progress
/// is not lost when `poll_next` returns `Pending`.
///
/// The future takes ownership of the `FrameReader` and returns it alongside
/// the result, so we can store it back for the next read.
///
/// Also holds the QUIC `Connection` to keep it alive for the duration of the
/// stream. gm-quic's `Connection` closes on drop, so we must prevent that.
struct ServerStreamingResponse<Resp> {
    /// State: either we hold the reader (idle) or a future (reading)
    state: StreamResponseState,
    /// Whether stream has ended
    ended: bool,
    /// Keep the QUIC connection alive while the stream is being consumed
    _conn: Connection,
    /// Phantom for response type
    _phantom: std::marker::PhantomData<Resp>,
}

/// Internal state for `ServerStreamingResponse`
enum StreamResponseState {
    /// Idle — reader is available for the next read
    Idle(FrameReader<StreamReader>),
    /// Reading — a `read_frame` future is in flight
    Reading(BoxFuture<'static, (Result<Frame, CurpError>, FrameReader<StreamReader>)>),
    /// Poisoned — state was taken and not restored (should not happen)
    Poisoned,
}

impl<Resp> ServerStreamingResponse<Resp> {
    /// Create a new server-streaming response
    fn new(reader: FrameReader<StreamReader>, conn: Connection) -> Self {
        Self {
            state: StreamResponseState::Idle(reader),
            ended: false,
            _conn: conn,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<Resp> Stream for ServerStreamingResponse<Resp>
where
    Resp: Message + Default + Unpin + Send + 'static,
{
    type Item = Result<Resp, CurpError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.ended {
            return Poll::Ready(None);
        }

        // If idle, start a new read
        if matches!(this.state, StreamResponseState::Idle(_)) {
            let state = std::mem::replace(&mut this.state, StreamResponseState::Poisoned);
            if let StreamResponseState::Idle(mut reader) = state {
                this.state = StreamResponseState::Reading(Box::pin(async move {
                    let result = reader.read_frame().await;
                    (result, reader)
                }));
            }
        }

        // Poll the in-flight future
        if let StreamResponseState::Reading(ref mut fut) = this.state {
            match fut.as_mut().poll(cx) {
                Poll::Ready((result, reader)) => {
                    this.state = StreamResponseState::Idle(reader);
                    match result {
                        Ok(Frame::Data(data)) => {
                            let resp = Resp::decode(data.as_slice())
                                .map_err(|e| CurpError::internal(format!("decode error: {e}")));
                            Poll::Ready(Some(resp))
                        }
                        Ok(Frame::Status { code, details }) => {
                            this.ended = true;
                            if code == status_ok() {
                                Poll::Ready(None)
                            } else {
                                Poll::Ready(Some(Err(QuicChannel::decode_error(&details)
                                    .unwrap_or_else(|e| e))))
                            }
                        }
                        Ok(Frame::End) => {
                            this.ended = true;
                            Poll::Ready(Some(Err(CurpError::internal(
                                "unexpected END in server-streaming",
                            ))))
                        }
                        Err(e) => {
                            this.ended = true;
                            Poll::Ready(Some(Err(e)))
                        }
                    }
                }
                Poll::Pending => Poll::Pending,
            }
        } else {
            // Poisoned state
            this.ended = true;
            Poll::Ready(Some(Err(CurpError::internal("stream state poisoned"))))
        }
    }
}
