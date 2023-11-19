use std::io;

use serde_json::Error as Jrror;

use tokio_stream::{Stream, StreamExt};

pub fn from_strings<S, T, C>(strings: S, conv: C) -> impl Stream<Item = Result<T, Jrror>>
where
    S: Stream<Item = Result<String, io::Error>>,
    C: Fn(Result<String, io::Error>) -> Result<T, Jrror>,
{
    strings.map(conv)
}

pub fn from_slices<S, T, C>(slices: S, conv: C) -> impl Stream<Item = Result<T, Jrror>>
where
    S: Stream<Item = Result<Vec<u8>, io::Error>>,
    C: Fn(Result<Vec<u8>, io::Error>) -> Result<T, Jrror>,
{
    slices.map(conv)
}
