//! QUIC transport implementation for Curp RPC
//!
//! This module provides QUIC-based transport as an alternative to tonic gRPC.
//! It implements the same `ConnectApi`/`InnerConnectApi` traits but uses
//! gm-quic streams with prost encoding instead of tonic channels.

pub(crate) mod codec;
pub(crate) mod channel;
pub(crate) mod server;

pub use channel::QuicChannel;
pub use channel::DnsFallback;
pub use server::QuicGrpcServer;
