fn main() {
    // Generate tonic Protocol service trait and ProtocolServer for the
    // client-facing gRPC endpoint (AuthWrapper implements Protocol).
    //
    // Message types are mapped to curp::rpc via extern_path so that the
    // generated Protocol trait uses the same types as the rest of the codebase.
    tonic_build::configure()
        .build_client(false)
        .extern_path(".commandpb.ProposeRequest", "::curp::rpc::ProposeRequest")
        .extern_path(".commandpb.ProposeResponse", "::curp::rpc::ProposeResponse")
        .extern_path(".commandpb.OpResponse", "::curp::rpc::OpResponse")
        .extern_path(".commandpb.RecordRequest", "::curp::rpc::RecordRequest")
        .extern_path(".commandpb.RecordResponse", "::curp::rpc::RecordResponse")
        .extern_path(".commandpb.ReadIndexRequest", "::curp::rpc::ReadIndexRequest")
        .extern_path(".commandpb.ReadIndexResponse", "::curp::rpc::ReadIndexResponse")
        .extern_path(".commandpb.ShutdownRequest", "::curp::rpc::ShutdownRequest")
        .extern_path(".commandpb.ShutdownResponse", "::curp::rpc::ShutdownResponse")
        .extern_path(
            ".commandpb.ProposeConfChangeRequest",
            "::curp::rpc::ProposeConfChangeRequest",
        )
        .extern_path(
            ".commandpb.ProposeConfChangeResponse",
            "::curp::rpc::ProposeConfChangeResponse",
        )
        .extern_path(".commandpb.PublishRequest", "::curp::rpc::PublishRequest")
        .extern_path(".commandpb.PublishResponse", "::curp::rpc::PublishResponse")
        .extern_path(
            ".commandpb.FetchClusterRequest",
            "::curp::rpc::FetchClusterRequest",
        )
        .extern_path(
            ".commandpb.FetchClusterResponse",
            "::curp::rpc::FetchClusterResponse",
        )
        .extern_path(
            ".commandpb.FetchReadStateRequest",
            "::curp::rpc::FetchReadStateRequest",
        )
        .extern_path(
            ".commandpb.FetchReadStateResponse",
            "::curp::rpc::FetchReadStateResponse",
        )
        .extern_path(
            ".commandpb.MoveLeaderRequest",
            "::curp::rpc::MoveLeaderRequest",
        )
        .extern_path(
            ".commandpb.MoveLeaderResponse",
            "::curp::rpc::MoveLeaderResponse",
        )
        .extern_path(
            ".commandpb.LeaseKeepAliveMsg",
            "::curp::rpc::LeaseKeepAliveMsg",
        )
        .compile_protos(
            &["../curp/proto/common/src/curp-command.proto"],
            &["../curp/proto/common/src"],
        )
        .unwrap_or_else(|e| panic!("Failed to compile curp proto for xline: {e:?}"));
}
