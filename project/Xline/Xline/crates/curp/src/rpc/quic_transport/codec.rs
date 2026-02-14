//! Frame codec for QUIC transport
//!
//! This module implements the binary frame protocol for QUIC streams.
//! Each bidirectional stream carries one RPC call with the following format:
//!
//! Request header (client → server):
//! ```text
//! [2 bytes] path length (big-endian u16)
//! [N bytes] path string
//! [2 bytes] metadata entry count
//! For each entry: [2B key_len][key][2B val_len][val]
//! ```
//!
//! Frame types (after request header):
//! ```text
//! [1 byte] frame_type:
//!   0x01 = DATA   → [4 bytes length] + [N bytes protobuf data]
//!   0x02 = END    → no payload (stream end marker)
//!   0x03 = STATUS → [1 byte status_code] + [4 bytes details_len] + [N bytes CurpErrorWrapper]
//! ```

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::rpc::CurpError;

/// Maximum frame length: 16 MB
const MAX_FRAME_LEN: u32 = 16 * 1024 * 1024;

/// Maximum path length: 256 bytes
const MAX_PATH_LEN: u16 = 256;

/// Maximum metadata entries: 64
const MAX_METADATA_ENTRIES: u16 = 64;

/// Maximum metadata key/value length: 4 KB
const MAX_METADATA_KV_LEN: u16 = 4 * 1024;

/// Frame type constants
const FRAME_TYPE_DATA: u8 = 0x01;
const FRAME_TYPE_END: u8 = 0x02;
const FRAME_TYPE_STATUS: u8 = 0x03;

/// Status code constants
const STATUS_OK: u8 = 0x00;
const STATUS_ERROR: u8 = 0x01;

/// Frame types for QUIC protocol
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Frame {
    /// DATA frame containing protobuf-encoded message
    Data(Vec<u8>),
    /// END frame marking stream completion (application layer)
    End,
    /// STATUS frame with status code and optional error details
    Status {
        /// Status code (0x00 = OK, 0x01 = Error)
        code: u8,
        /// Error details (CurpErrorWrapper protobuf if code != 0)
        details: Vec<u8>,
    },
}

/// Stream state machine for frame sequence validation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Some variants used only in tests or future features
pub(crate) enum StreamState {
    /// Expecting DATA frame (unary request/response first frame)
    ExpectData,
    /// Expecting STATUS frame only (unary response after receiving DATA)
    ExpectStatusOnly,
    /// Expecting END frame only (unary request after receiving DATA)
    ExpectEndOnly,
    /// Expecting DATA or STATUS frame (server-streaming response)
    ExpectDataOrStatus,
    /// Expecting DATA or END frame (client-streaming request)
    ExpectDataOrEnd,
    /// Terminal state, no more frames expected
    Terminal,
}

/// Stateful frame reader with stream state machine
pub(crate) struct FrameReader<R: AsyncRead + Unpin> {
    /// Underlying reader
    reader: R,
    /// Current stream state
    state: StreamState,
    /// Whether this is a response reader (affects STATUS handling in ExpectData)
    is_response: bool,
}

impl<R: AsyncRead + Unpin> FrameReader<R> {
    /// Create a unary response reader (initial state: ExpectData)
    #[inline]
    pub(crate) fn new_unary_response(reader: R) -> Self {
        Self {
            reader,
            state: StreamState::ExpectData,
            is_response: true,
        }
    }

    /// Create a server-streaming response reader (initial state: ExpectDataOrStatus)
    #[inline]
    pub(crate) fn new_server_streaming(reader: R) -> Self {
        Self {
            reader,
            state: StreamState::ExpectDataOrStatus,
            is_response: true,
        }
    }

    /// Create a client-streaming request reader (initial state: ExpectDataOrEnd)
    #[inline]
    #[allow(dead_code)] // Used in server-side handling
    pub(crate) fn new_client_streaming(reader: R) -> Self {
        Self {
            reader,
            state: StreamState::ExpectDataOrEnd,
            is_response: false,
        }
    }

    /// Create a unary request reader (initial state: ExpectData)
    #[inline]
    pub(crate) fn new_unary_request(reader: R) -> Self {
        Self {
            reader,
            state: StreamState::ExpectData,
            is_response: false,
        }
    }

    /// Read next frame with state machine validation
    pub(crate) async fn read_frame(&mut self) -> Result<Frame, CurpError> {
        if self.state == StreamState::Terminal {
            return Err(CurpError::internal(
                "protocol violation: frame received after terminal frame",
            ));
        }

        let frame_type = match self.reader.read_u8().await {
            Ok(ft) => ft,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(CurpError::RpcTransport(()));
            }
            Err(e) => {
                return Err(CurpError::internal(format!("read frame type error: {e}")));
            }
        };

        let frame = match frame_type {
            FRAME_TYPE_DATA => {
                let len = self.reader.read_u32().await.map_err(|e| {
                    CurpError::internal(format!("read DATA frame length error: {e}"))
                })?;
                if len > MAX_FRAME_LEN {
                    return Err(CurpError::internal("frame too large"));
                }
                let mut data = vec![0u8; len as usize];
                let _ = self.reader.read_exact(&mut data).await.map_err(|e| {
                    CurpError::internal(format!("read DATA frame payload error: {e}"))
                })?;
                Frame::Data(data)
            }
            FRAME_TYPE_END => Frame::End,
            FRAME_TYPE_STATUS => {
                let code = self
                    .reader
                    .read_u8()
                    .await
                    .map_err(|e| CurpError::internal(format!("read STATUS code error: {e}")))?;
                if code != STATUS_OK && code != STATUS_ERROR {
                    return Err(CurpError::internal(format!(
                        "protocol violation: unknown status code 0x{code:02X}"
                    )));
                }
                let details_len = self.reader.read_u32().await.map_err(|e| {
                    CurpError::internal(format!("read STATUS details length error: {e}"))
                })?;
                if details_len > MAX_FRAME_LEN {
                    return Err(CurpError::internal(
                        "protocol violation: STATUS details too large",
                    ));
                }
                let mut details = vec![0u8; details_len as usize];
                if details_len > 0 {
                    let _ = self.reader.read_exact(&mut details).await.map_err(|e| {
                        CurpError::internal(format!("read STATUS details error: {e}"))
                    })?;
                }
                Frame::Status { code, details }
            }
            _ => {
                return Err(CurpError::internal(format!(
                    "unknown frame type 0x{frame_type:02X}"
                )));
            }
        };

        // Validate and transition state
        self.validate_and_transition(&frame)?;

        Ok(frame)
    }

    /// Validate frame against current state and transition to next state
    fn validate_and_transition(&mut self, frame: &Frame) -> Result<(), CurpError> {
        match (self.state, frame) {
            // ExpectData state
            (StreamState::ExpectData, Frame::Data(_)) => {
                self.state = if self.is_response {
                    StreamState::ExpectStatusOnly
                } else {
                    StreamState::ExpectEndOnly
                };
            }
            (StreamState::ExpectData, Frame::Status { .. }) if self.is_response => {
                // Server returned error before sending data - valid for response
                self.state = StreamState::Terminal;
            }
            (StreamState::ExpectData, Frame::End) => {
                // Peer closed without sending data
                return Err(CurpError::RpcTransport(()));
            }
            (StreamState::ExpectData, _) => {
                return Err(CurpError::internal(format!(
                    "protocol violation: unexpected frame in state ExpectData"
                )));
            }

            // ExpectStatusOnly state
            (StreamState::ExpectStatusOnly, Frame::Status { .. }) => {
                self.state = StreamState::Terminal;
            }
            (StreamState::ExpectStatusOnly, Frame::Data(_)) => {
                return Err(CurpError::internal(
                    "protocol violation: unexpected DATA in state ExpectStatusOnly",
                ));
            }
            (StreamState::ExpectStatusOnly, _) => {
                return Err(CurpError::internal(format!(
                    "protocol violation: unexpected frame in state ExpectStatusOnly"
                )));
            }

            // ExpectEndOnly state
            (StreamState::ExpectEndOnly, Frame::End) => {
                self.state = StreamState::Terminal;
            }
            (StreamState::ExpectEndOnly, Frame::Data(_)) => {
                return Err(CurpError::internal(
                    "protocol violation: unexpected DATA in state ExpectEndOnly",
                ));
            }
            (StreamState::ExpectEndOnly, _) => {
                return Err(CurpError::internal(format!(
                    "protocol violation: unexpected frame in state ExpectEndOnly"
                )));
            }

            // ExpectDataOrStatus state (server-streaming)
            (StreamState::ExpectDataOrStatus, Frame::Data(_)) => {
                // Stay in same state, more data may come
            }
            (StreamState::ExpectDataOrStatus, Frame::Status { .. }) => {
                self.state = StreamState::Terminal;
            }
            (StreamState::ExpectDataOrStatus, Frame::End) => {
                return Err(CurpError::internal(
                    "protocol violation: unexpected END in state ExpectDataOrStatus",
                ));
            }

            // ExpectDataOrEnd state (client-streaming)
            (StreamState::ExpectDataOrEnd, Frame::Data(_)) => {
                // Stay in same state, more data may come
            }
            (StreamState::ExpectDataOrEnd, Frame::End) => {
                self.state = StreamState::Terminal;
            }
            (StreamState::ExpectDataOrEnd, Frame::Status { .. }) => {
                return Err(CurpError::internal(
                    "protocol violation: unexpected STATUS in state ExpectDataOrEnd",
                ));
            }

            // Terminal state - should not reach here due to early check
            (StreamState::Terminal, _) => {
                return Err(CurpError::internal(
                    "protocol violation: frame received after terminal frame",
                ));
            }
        }
        Ok(())
    }

    /// Check if reader is in terminal state
    #[inline]
    #[allow(dead_code)] // Used in tests
    pub(crate) fn is_terminal(&self) -> bool {
        self.state == StreamState::Terminal
    }
}

/// Frame writer (handles frame encoding only, not stream lifecycle)
pub(crate) struct FrameWriter<W: AsyncWrite + Unpin> {
    /// Underlying writer
    writer: W,
}

impl<W: AsyncWrite + Unpin> FrameWriter<W> {
    /// Create a new frame writer
    #[inline]
    pub(crate) fn new(writer: W) -> Self {
        Self { writer }
    }

    /// Write request header
    pub(crate) async fn write_request_header(
        &mut self,
        path: &str,
        meta: &[(String, String)],
    ) -> Result<(), CurpError> {
        let path_bytes = path.as_bytes();
        if path_bytes.len() > MAX_PATH_LEN as usize {
            return Err(CurpError::internal("header too large: path"));
        }

        #[allow(clippy::cast_possible_truncation)]
        let path_len = path_bytes.len() as u16;
        self.writer
            .write_u16(path_len)
            .await
            .map_err(|e| CurpError::internal(format!("write path length error: {e}")))?;
        self.writer
            .write_all(path_bytes)
            .await
            .map_err(|e| CurpError::internal(format!("write path error: {e}")))?;

        if meta.len() > MAX_METADATA_ENTRIES as usize {
            return Err(CurpError::internal("header too large: metadata entries"));
        }

        #[allow(clippy::cast_possible_truncation)]
        let meta_count = meta.len() as u16;
        self.writer
            .write_u16(meta_count)
            .await
            .map_err(|e| CurpError::internal(format!("write metadata count error: {e}")))?;

        for (key, value) in meta {
            let key_bytes = key.as_bytes();
            let value_bytes = value.as_bytes();
            if key_bytes.len() > MAX_METADATA_KV_LEN as usize
                || value_bytes.len() > MAX_METADATA_KV_LEN as usize
            {
                return Err(CurpError::internal("header too large: metadata kv"));
            }

            #[allow(clippy::cast_possible_truncation)]
            let key_len = key_bytes.len() as u16;
            #[allow(clippy::cast_possible_truncation)]
            let value_len = value_bytes.len() as u16;

            self.writer
                .write_u16(key_len)
                .await
                .map_err(|e| CurpError::internal(format!("write key length error: {e}")))?;
            self.writer
                .write_all(key_bytes)
                .await
                .map_err(|e| CurpError::internal(format!("write key error: {e}")))?;
            self.writer
                .write_u16(value_len)
                .await
                .map_err(|e| CurpError::internal(format!("write value length error: {e}")))?;
            self.writer
                .write_all(value_bytes)
                .await
                .map_err(|e| CurpError::internal(format!("write value error: {e}")))?;
        }

        self.writer
            .flush()
            .await
            .map_err(|e| CurpError::internal(format!("flush header error: {e}")))?;

        Ok(())
    }

    /// Write a frame
    pub(crate) async fn write_frame(&mut self, frame: &Frame) -> Result<(), CurpError> {
        match *frame {
            Frame::Data(ref data) => {
                self.writer
                    .write_u8(FRAME_TYPE_DATA)
                    .await
                    .map_err(|e| CurpError::internal(format!("write DATA type error: {e}")))?;
                #[allow(clippy::cast_possible_truncation)]
                let len = data.len() as u32;
                self.writer
                    .write_u32(len)
                    .await
                    .map_err(|e| CurpError::internal(format!("write DATA length error: {e}")))?;
                self.writer
                    .write_all(data)
                    .await
                    .map_err(|e| CurpError::internal(format!("write DATA payload error: {e}")))?;
            }
            Frame::End => {
                self.writer
                    .write_u8(FRAME_TYPE_END)
                    .await
                    .map_err(|e| CurpError::internal(format!("write END error: {e}")))?;
            }
            Frame::Status { code, ref details } => {
                self.writer
                    .write_u8(FRAME_TYPE_STATUS)
                    .await
                    .map_err(|e| CurpError::internal(format!("write STATUS type error: {e}")))?;
                self.writer
                    .write_u8(code)
                    .await
                    .map_err(|e| CurpError::internal(format!("write STATUS code error: {e}")))?;
                #[allow(clippy::cast_possible_truncation)]
                let details_len = details.len() as u32;
                self.writer.write_u32(details_len).await.map_err(|e| {
                    CurpError::internal(format!("write STATUS details length error: {e}"))
                })?;
                if !details.is_empty() {
                    self.writer.write_all(details).await.map_err(|e| {
                        CurpError::internal(format!("write STATUS details error: {e}"))
                    })?;
                }
            }
        }

        self.writer
            .flush()
            .await
            .map_err(|e| CurpError::internal(format!("flush frame error: {e}")))?;

        Ok(())
    }

    /// Consume writer and return underlying stream
    #[inline]
    pub(crate) fn into_inner(self) -> W {
        self.writer
    }
}

/// Read request header from stream
pub(crate) async fn read_request_header<R: AsyncRead + Unpin>(
    r: &mut R,
) -> Result<(String, Vec<(String, String)>), CurpError> {
    // Read path
    let path_len = r
        .read_u16()
        .await
        .map_err(|e| CurpError::internal(format!("read path length error: {e}")))?;
    if path_len > MAX_PATH_LEN {
        return Err(CurpError::internal("header too large: path"));
    }
    let mut path_bytes = vec![0u8; path_len as usize];
    let _ = r.read_exact(&mut path_bytes)
        .await
        .map_err(|e| CurpError::internal(format!("read path error: {e}")))?;
    let path = String::from_utf8(path_bytes)
        .map_err(|e| CurpError::internal(format!("invalid path encoding: {e}")))?;

    // Read metadata
    let meta_count = r
        .read_u16()
        .await
        .map_err(|e| CurpError::internal(format!("read metadata count error: {e}")))?;
    if meta_count > MAX_METADATA_ENTRIES {
        return Err(CurpError::internal("header too large: metadata entries"));
    }

    let mut meta = Vec::with_capacity(meta_count as usize);
    for _ in 0..meta_count {
        let key_len = r
            .read_u16()
            .await
            .map_err(|e| CurpError::internal(format!("read key length error: {e}")))?;
        if key_len > MAX_METADATA_KV_LEN {
            return Err(CurpError::internal("header too large: metadata key"));
        }
        let mut key_bytes = vec![0u8; key_len as usize];
        let _ = r.read_exact(&mut key_bytes)
            .await
            .map_err(|e| CurpError::internal(format!("read key error: {e}")))?;
        let key = String::from_utf8(key_bytes)
            .map_err(|e| CurpError::internal(format!("invalid key encoding: {e}")))?;

        let value_len = r
            .read_u16()
            .await
            .map_err(|e| CurpError::internal(format!("read value length error: {e}")))?;
        if value_len > MAX_METADATA_KV_LEN {
            return Err(CurpError::internal("header too large: metadata value"));
        }
        let mut value_bytes = vec![0u8; value_len as usize];
        let _ = r.read_exact(&mut value_bytes)
            .await
            .map_err(|e| CurpError::internal(format!("read value error: {e}")))?;
        let value = String::from_utf8(value_bytes)
            .map_err(|e| CurpError::internal(format!("invalid value encoding: {e}")))?;

        meta.push((key, value));
    }

    Ok((path, meta))
}

/// Status code for OK response
#[inline]
pub(crate) const fn status_ok() -> u8 {
    STATUS_OK
}

/// Status code for error response
#[inline]
pub(crate) const fn status_error() -> u8 {
    STATUS_ERROR
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_frame_roundtrip_data() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _write_half) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        let data = b"hello world".to_vec();
        writer.write_frame(&Frame::Data(data.clone())).await.unwrap();

        let mut reader = FrameReader::new_unary_response(read_half);
        let frame = reader.read_frame().await.unwrap();
        assert_eq!(frame, Frame::Data(data));
    }

    #[tokio::test]
    async fn test_frame_roundtrip_end() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        // First send DATA, then END for client-streaming
        writer.write_frame(&Frame::Data(vec![1, 2, 3])).await.unwrap();
        writer.write_frame(&Frame::End).await.unwrap();

        let mut reader = FrameReader::new_client_streaming(read_half);
        let frame1 = reader.read_frame().await.unwrap();
        assert!(matches!(frame1, Frame::Data(_)));
        let frame2 = reader.read_frame().await.unwrap();
        assert_eq!(frame2, Frame::End);
        assert!(reader.is_terminal());
    }

    #[tokio::test]
    async fn test_frame_roundtrip_status() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        // For unary response: DATA then STATUS
        writer.write_frame(&Frame::Data(vec![1, 2, 3])).await.unwrap();
        writer
            .write_frame(&Frame::Status {
                code: status_ok(),
                details: vec![],
            })
            .await
            .unwrap();

        let mut reader = FrameReader::new_unary_response(read_half);
        let frame1 = reader.read_frame().await.unwrap();
        assert!(matches!(frame1, Frame::Data(_)));
        let frame2 = reader.read_frame().await.unwrap();
        assert!(matches!(frame2, Frame::Status { code: 0, .. }));
        assert!(reader.is_terminal());
    }

    #[tokio::test]
    async fn test_request_header_roundtrip() {
        let (client, server) = tokio::io::duplex(1024);
        let (mut read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        let path = "/curp.Protocol/FetchCluster";
        let meta = vec![
            ("bypass".to_owned(), "true".to_owned()),
            ("token".to_owned(), "jwt123".to_owned()),
        ];
        writer.write_request_header(path, &meta).await.unwrap();

        let (read_path, read_meta) = read_request_header(&mut read_half).await.unwrap();
        assert_eq!(read_path, path);
        assert_eq!(read_meta, meta);
    }

    #[tokio::test]
    async fn test_state_machine_unary_response() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        // Valid sequence: DATA -> STATUS
        writer.write_frame(&Frame::Data(vec![1])).await.unwrap();
        writer
            .write_frame(&Frame::Status {
                code: status_ok(),
                details: vec![],
            })
            .await
            .unwrap();

        let mut reader = FrameReader::new_unary_response(read_half);
        assert!(!reader.is_terminal());
        let _ = reader.read_frame().await.unwrap();
        assert!(!reader.is_terminal());
        let _ = reader.read_frame().await.unwrap();
        assert!(reader.is_terminal());
    }

    #[tokio::test]
    async fn test_state_machine_reject_double_data_in_unary() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        // Invalid: two DATA frames in unary response
        writer.write_frame(&Frame::Data(vec![1])).await.unwrap();
        writer.write_frame(&Frame::Data(vec![2])).await.unwrap();

        let mut reader = FrameReader::new_unary_response(read_half);
        let _ = reader.read_frame().await.unwrap(); // First DATA ok
        let result = reader.read_frame().await; // Second DATA should fail
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_server_streaming_multiple_data() {
        let (client, server) = tokio::io::duplex(1024);
        let (read_half, _) = tokio::io::split(server);
        let (_, client_write) = tokio::io::split(client);

        let mut writer = FrameWriter::new(client_write);
        // Valid: multiple DATA frames then STATUS
        writer.write_frame(&Frame::Data(vec![1])).await.unwrap();
        writer.write_frame(&Frame::Data(vec![2])).await.unwrap();
        writer.write_frame(&Frame::Data(vec![3])).await.unwrap();
        writer
            .write_frame(&Frame::Status {
                code: status_ok(),
                details: vec![],
            })
            .await
            .unwrap();

        let mut reader = FrameReader::new_server_streaming(read_half);
        for _ in 0..3 {
            let frame = reader.read_frame().await.unwrap();
            assert!(matches!(frame, Frame::Data(_)));
            assert!(!reader.is_terminal());
        }
        let frame = reader.read_frame().await.unwrap();
        assert!(matches!(frame, Frame::Status { .. }));
        assert!(reader.is_terminal());
    }
}
