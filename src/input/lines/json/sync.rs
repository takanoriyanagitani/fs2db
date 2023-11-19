use std::io;

use serde_json::Error as Jrror;

pub fn from_strings<I, T, C>(strings: I, conv: C) -> impl Iterator<Item = Result<T, Jrror>>
where
    I: Iterator<Item = Result<String, io::Error>>,
    C: Fn(Result<String, io::Error>) -> Result<T, Jrror>,
{
    strings.map(conv)
}

pub fn from_slices<I, T, C>(slices: I, conv: C) -> impl Iterator<Item = Result<T, Jrror>>
where
    I: Iterator<Item = Result<Vec<u8>, io::Error>>,
    C: Fn(Result<Vec<u8>, io::Error>) -> Result<T, Jrror>,
{
    slices.map(conv)
}
