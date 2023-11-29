#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SdfsCommand {
    #[prost(
        oneof = "sdfs_command::Type",
        tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 20"
    )]
    pub r#type: ::core::option::Option<sdfs_command::Type>,
}
/// Nested message and enum types in `SDFSCommand`.
pub mod sdfs_command {
    #[allow(clippy::derive_partial_eq_without_eq)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag = "1")]
        PutReq(super::PutReq),
        #[prost(message, tag = "2")]
        PutData(super::PutData),
        #[prost(message, tag = "3")]
        GetReq(super::GetReq),
        #[prost(message, tag = "4")]
        GetData(super::GetData),
        #[prost(message, tag = "5")]
        Del(super::Delete),
        #[prost(message, tag = "6")]
        LsReq(super::LsReq),
        #[prost(message, tag = "7")]
        LsRes(super::LsRes),
        #[prost(message, tag = "8")]
        Ack(super::Ack),
        #[prost(message, tag = "9")]
        Fail(super::Fail),
        #[prost(message, tag = "10")]
        LeaderPutReq(super::LeaderPutReq),
        #[prost(message, tag = "11")]
        LeaderStoreReq(super::LeaderStoreReq),
        #[prost(message, tag = "12")]
        LeaderStoreRes(super::LeaderStoreRes),
        #[prost(message, tag = "13")]
        MultiRead(super::MultiRead),
        #[prost(message, tag = "14")]
        MultiWrite(super::MultiWrite),
        #[prost(message, tag = "15")]
        MapReq(super::MapReq),
        #[prost(message, tag = "16")]
        RedReq(super::ReduceReq),
        #[prost(message, tag = "20")]
        LeaderRedReq(super::LeaderReduceReq),
    }
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutReq {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PutData {
    #[prost(string, tag = "1")]
    pub machine: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderPutReq {
    #[prost(string, tag = "1")]
    pub machine: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub file_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetReq {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetData {
    #[prost(string, tag = "1")]
    pub machine: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub file_name: ::prost::alloc::string::String,
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Delete {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LsReq {
    #[prost(string, tag = "1")]
    pub file_name: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LsRes {
    #[prost(string, repeated, tag = "1")]
    pub machines: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ack {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Fail {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderStoreReq {
    #[prost(string, tag = "1")]
    pub message: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderStoreRes {
    #[prost(string, repeated, tag = "1")]
    pub files: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiRead {
    #[prost(string, tag = "1")]
    pub local_file_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub sdfs_file_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub leader_ip: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MultiWrite {
    #[prost(string, tag = "1")]
    pub local_file_name: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub sdfs_file_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub leader_ip: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct MapReq {
    #[prost(string, tag = "1")]
    pub executable: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub num_workers: u32,
    #[prost(string, tag = "3")]
    pub file_name_prefix: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub input_dir: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReduceReq {
    #[prost(string, tag = "1")]
    pub executable: ::prost::alloc::string::String,
    #[prost(uint32, tag = "2")]
    pub num_workers: u32,
    #[prost(string, tag = "3")]
    pub file_name_prefix: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub output_file: ::prost::alloc::string::String,
    #[prost(bool, tag = "5")]
    pub delete: bool,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaderReduceReq {
    #[prost(map = "string, message", tag = "1")]
    pub key_server_map: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        KeyServers,
    >,
    #[prost(string, tag = "2")]
    pub target_server: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub output_file: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub executable: ::prost::alloc::string::String,
}
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct KeyServers {
    #[prost(string, repeated, tag = "1")]
    pub servers: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
