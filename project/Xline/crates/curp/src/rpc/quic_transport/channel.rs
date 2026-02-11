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
    time::Duration,
};

use futures::Stream;
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
        self.client
            .connect(addr_str)
            .await
            .map_err(|e| CurpError::internal(format!("QUIC connect error: {e}")))
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
        Ok(Box::pin(ServerStreamingResponse::<_, Resp>::new(reader)))
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
        Req: Message,
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

            // Write all request messages
            let mut stream = stream;
            while let Some(req) = stream.next().await {
                let req_bytes = req.encode_to_vec();
                writer.write_frame(&Frame::Data(req_bytes)).await?;
            }
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
                    return Err(CurpError::internal(
                        "unexpected frame in client-streaming response",
                    ));
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
struct ServerStreamingResponse<R, Resp>
where
    R: tokio::io::AsyncRead + Unpin,
{
    /// Frame reader
    reader: FrameReader<R>,
    /// Whether stream has ended
    ended: bool,
    /// Phantom for response type
    _phantom: std::marker::PhantomData<Resp>,
}

impl<R, Resp> ServerStreamingResponse<R, Resp>
where
    R: tokio::io::AsyncRead + Unpin,
{
    /// Create a new server-streaming response
    fn new(reader: FrameReader<R>) -> Self {
        Self {
            reader,
            ended: false,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<R, Resp> Stream for ServerStreamingResponse<R, Resp>
where
    R: tokio::io::AsyncRead + Unpin + Send,
    Resp: Message + Default + Unpin,
{
    type Item = Result<Resp, CurpError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;

        let this = self.get_mut();

        if this.ended {
            return Poll::Ready(None);
        }

        // We need to poll the async read_frame
        let fut = this.reader.read_frame();
        tokio::pin!(fut);

        match fut.poll(cx) {
            Poll::Ready(Ok(frame)) => match frame {
                Frame::Data(data) => {
                    let resp = Resp::decode(data.as_slice())
                        .map_err(|e| CurpError::internal(format!("decode error: {e}")));
                    Poll::Ready(Some(resp))
                }
                Frame::Status { code, details } => {
                    this.ended = true;
                    if code == status_ok() {
                        Poll::Ready(None)
                    } else {
                        Poll::Ready(Some(Err(QuicChannel::decode_error(&details)
                            .unwrap_or_else(|e| e))))
                    }
                }
                Frame::End => {
                    this.ended = true;
                    Poll::Ready(Some(Err(CurpError::internal(
                        "unexpected END in server-streaming",
                    ))))
                }
            },
            Poll::Ready(Err(e)) => {
                this.ended = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
