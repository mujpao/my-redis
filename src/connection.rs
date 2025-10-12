// use bytes::{Buf, BytesMut};
// use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
// use tokio::net::TcpStream;

// struct Connection {
//     stream: BufWriter<TcpStream>,
//     buffer: BytesMut,
// }

// impl Connection {
//     fn new() -> Self {
//         Self {
//             // TODO change size
//             stream: BufWriter::new(socket),

//             buffer: BytesMut::with_capacity(4 * 1024),
//         }
//     }

//     async fn read_value(&mut self) -> anyhow::Result<Option<Frame>> {
//         loop {
//             if let Some(frame) = self.parse_frame()? {
//                 return Ok(Some(frame));
//             }

//             if 0 == self.stream.read_buf(&mut self.buffer).await? {
//                 if self.buffer.is_empty() {
//                     return Ok(None);
//                 } else {
//                     return Err("connection reset by peer".into());
//                 }
//             }
//         }
//     }

//     fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
//         let mut buf = Cursor::new(&self.buffer[..]);

//         match Frame::check(&mut buf) {
//             Ok(_) => {
//                 // Get the byte length of the frame
//                 let len = buf.position() as usize;

//                 // Reset the internal cursor for the
//                 // call to `parse`.
//                 buf.set_position(0);

//                 // Parse the frame
//                 let frame = Frame::parse(&mut buf)?;

//                 // Discard the frame from the buffer
//                 self.buffer.advance(len);

//                 // Return the frame to the caller.
//                 Ok(Some(frame))
//             }
//             // Not enough data has been buffered
//             Err(Incomplete) => Ok(None),
//             // An error was encountered
//             Err(e) => Err(e.into()),
//         }
//     }
// }
