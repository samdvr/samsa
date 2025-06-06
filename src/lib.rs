pub mod proto {
    tonic::include_proto!("samsa.v1");
    include!(concat!(env!("OUT_DIR"), "/samsa.v1.serde.rs"));
}

pub mod common;
pub mod server;
