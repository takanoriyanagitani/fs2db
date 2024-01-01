#[deny(clippy::unwrap_used)]

pub mod rpc {
    pub mod fs2db {
        pub mod proto {

            #[cfg(feature = "source")]
            pub mod source {
                #[allow(clippy::unwrap_used)]
                pub mod v1 {
                    tonic::include_proto!("fs2db.proto.source.v1");
                }
            }

            #[cfg(feature = "target")]
            pub mod target {
                #[allow(clippy::unwrap_used)]
                pub mod v1 {
                    tonic::include_proto!("fs2db.proto.target.v1");
                }
            }
        }
    }
}

pub mod input;
pub mod output;

pub mod conv;

pub use futures;
pub use futures::StreamExt;
pub use futures::TryStreamExt;
pub use tokio_stream;
pub use tokio_stream::Stream;

pub use tokio;
pub use tonic;
