use crate::resp::{ParseError, RespValue};
use anyhow::anyhow;
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;
use tracing::warn;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            // TODO change size
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub fn get_client_addr(&self) -> anyhow::Result<SocketAddr> {
        self.stream
            .get_ref()
            .peer_addr()
            .map_err(|e| anyhow!("error getting client addr: {:?}", e))
    }

    pub async fn write_rdb_data(&mut self, data: &Vec<u8>) -> anyhow::Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream
            .write_all(data.len().to_string().as_bytes())
            .await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(data).await?;

        self.stream.flush().await?;

        Ok(())
    }

    pub async fn read_rdb_data(&mut self) -> anyhow::Result<Vec<u8>> {
        let c = self.stream.read_u8().await?;

        if c != b'$' {
            warn!(c, "read invalid rdb data");
            return Err(anyhow!("invalid rdb data"));
        }

        let mut length_as_bytes = vec![];
        loop {
            let c = self.stream.read_u8().await?;
            if c.is_ascii_digit() {
                length_as_bytes.push(c);
            } else {
                let next = self.stream.read_u8().await?;
                if c == b'\r' && next == b'\n' {
                    break;
                } else {
                    return Err(anyhow!("invalid rdb data"));
                }
            }
        }

        let len: usize = str::from_utf8(&length_as_bytes)?.parse::<usize>()?;

        let mut buffer = vec![0; len];

        self.stream.read_exact(&mut buffer).await?;

        Ok(buffer)
    }

    pub async fn read_value(&mut self) -> anyhow::Result<Option<RespValue>> {
        loop {
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    // TODO
                    return Ok(None);
                } else {
                    return Err(anyhow!("connection reset by peer"));
                }
            }
        }
    }

    fn parse_frame(&mut self) -> anyhow::Result<Option<RespValue>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match RespValue::parse_next(&mut buf) {
            Ok(value) => {
                let len = buf.position() as usize;
                self.buffer.advance(len);

                Ok(Some(value))
            }
            Err(ParseError::Incomplete) => Ok(None),
            Err(ParseError::Other(e)) => Err(anyhow!("failed to parse: {:?}", e)),
        }
    }

    pub async fn write_value(&mut self, value: &RespValue) -> tokio::io::Result<()> {
        match value {
            RespValue::SimpleString(s) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RespValue::BulkString(s) => {
                self.stream.write_u8(b'$').await?;
                self.stream
                    .write_all(s.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                self.stream.write_all(s.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RespValue::NullBulkString => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            RespValue::SimpleError(e) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(e.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RespValue::Integer(i) => {
                self.stream.write_u8(b':').await?;
                self.stream.write_all(i.to_string().as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RespValue::Array(a) => {
                self.stream.write_u8(b'*').await?;
                self.stream
                    .write_all(a.len().to_string().as_bytes())
                    .await?;
                self.stream.write_all(b"\r\n").await?;
                for element in a {
                    Box::pin(self.write_value(element)).await?;
                }
            }
            RespValue::NullArray => {
                self.stream.write_all(b"*-1\r\n").await?;
            }
        }

        self.stream.flush().await?;

        Ok(())
    }
}
