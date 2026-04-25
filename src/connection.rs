use crate::frame::{Frame, ParseError, resp::RespValue};
use anyhow::anyhow;
use bytes::{Buf, BytesMut};
use std::io::Cursor;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Self {
        Self {
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

    pub async fn write_rdb_data(&mut self, data: &[u8]) -> anyhow::Result<()> {
        self.stream.write_u8(b'$').await?;
        self.stream
            .write_all(data.len().to_string().as_bytes())
            .await?;
        self.stream.write_all(b"\r\n").await?;
        self.stream.write_all(data).await?;

        self.stream.flush().await?;

        Ok(())
    }

    pub async fn read_value<T: Frame>(&mut self) -> anyhow::Result<Option<T>> {
        self.read_value_count_bytes()
            .await
            .map(|res| res.map(|(frame, _)| frame))
    }

    pub async fn read_value_count_bytes<T: Frame>(&mut self) -> anyhow::Result<Option<(T, usize)>> {
        loop {
            if let Some(value) = self.parse_frame::<T>()? {
                return Ok(Some(value));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(anyhow!("connection reset by peer"));
                }
            }
        }
    }

    fn parse_frame<T: Frame>(&mut self) -> anyhow::Result<Option<(T, usize)>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match T::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let frame = T::parse(&mut buf)?;
                self.buffer.advance(len);

                Ok(Some((frame, len)))
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
