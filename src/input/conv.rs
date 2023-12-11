pub mod source;

pub mod lines;

#[cfg(any(feature = "gzip_tokio_async"))]
pub mod gzip;
