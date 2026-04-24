use anyhow::anyhow;
use bytes::Buf;
use std::io::Cursor;
use std::string::FromUtf8Error;

pub mod rdb;
pub mod resp;

pub trait Frame {
    fn parse(data: &mut Cursor<&[u8]>) -> Result<Self, ParseError>
    where
        Self: Sized;

    fn check(data: &mut Cursor<&[u8]>) -> Result<(), ParseError>;
}

#[derive(Debug)]
pub enum ParseError {
    Incomplete,
    Other(anyhow::Error),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "error parsing frame")
    }
}

impl std::error::Error for ParseError {}

impl From<FromUtf8Error> for ParseError {
    fn from(value: FromUtf8Error) -> Self {
        ParseError::Other(anyhow!("invalid utf8 {:?}", value))
    }
}

fn peek_u8(data: &mut Cursor<&[u8]>) -> Result<u8, ParseError> {
    if !data.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    Ok(data.chunk()[0])
}

fn skip(data: &mut Cursor<&[u8]>, n: usize) -> Result<(), ParseError> {
    if data.remaining() < n {
        return Err(ParseError::Incomplete);
    }

    data.advance(n);
    Ok(())
}

fn get_length(data: &mut Cursor<&[u8]>) -> Result<usize, ParseError> {
    if !data.has_remaining() {
        return Err(ParseError::Incomplete);
    }

    let line = get_line(data)?;

    str::from_utf8(line)
        .map_err(|e| ParseError::Other(e.into()))?
        .parse::<usize>()
        .map_err(|e| ParseError::Other(e.into()))
}

fn get_line<'a>(data: &mut Cursor<&'a [u8]>) -> Result<&'a [u8], ParseError> {
    let start = data.position() as usize;
    let crlf_start_index = data.get_ref()[start..]
        .windows(2)
        .position(|x| x == b"\r\n")
        .ok_or_else(|| ParseError::Incomplete)?
        + start;

    data.set_position((crlf_start_index + 2) as u64);
    Ok(&data.get_ref()[start..crlf_start_index])
}
