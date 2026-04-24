use super::{Frame, ParseError, get_length, skip};
use anyhow::anyhow;
use bytes::Buf;
use std::io::Cursor;
use std::io::Read;
use tracing::warn;

#[derive(Debug)]
pub struct RdbData(pub Vec<u8>);

impl RdbData {
    pub fn new_empty() -> anyhow::Result<Self> {
        let empty_rdb_file_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_data = hex::decode(empty_rdb_file_hex)?;
        Ok(Self(rdb_data))
    }
}

impl Frame for RdbData {
    fn parse(data: &mut Cursor<&[u8]>) -> Result<Self, ParseError> {
        if !data.has_remaining() {
            return Err(ParseError::Incomplete);
        }

        let c = data.get_u8();

        if c != b'$' {
            warn!(c, "read invalid rdb data");
            return Err(ParseError::Other(anyhow!("invalid rdb data: {}", c)));
        }

        let len = get_length(data)?;

        let mut buffer = vec![0; len];

        data.read_exact(&mut buffer)
            .map_err(|e| ParseError::Other(anyhow!("error reading rdb data into buffer: {}", e)))?;

        Ok(RdbData(buffer))
    }

    fn check(data: &mut Cursor<&[u8]>) -> Result<(), ParseError> {
        if !data.has_remaining() {
            return Err(ParseError::Incomplete);
        }

        let c = data.get_u8();

        if c != b'$' {
            warn!(c, "read invalid rdb data");
            return Err(ParseError::Other(anyhow!("invalid rdb data: {}", c)));
        }

        let len = get_length(data)?;
        skip(data, len)
    }
}
