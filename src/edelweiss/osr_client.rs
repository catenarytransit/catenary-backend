//! Newline-delimited OSR RPCs are read-only and can be retried after a reset.
use anyhow::{Context, Result};
use serde::{Serialize, de::DeserializeOwned};
use std::io::{self, BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::Duration;

#[derive(Debug)]
pub(super) struct TransportError(io::Error);

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OSR worker connection failed: {}", self.0)
    }
}

impl std::error::Error for TransportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

fn exchange(addr: &str, request: &[u8]) -> io::Result<String> {
    let mut stream = TcpStream::connect(addr)?;
    stream.set_read_timeout(Some(Duration::from_secs(45)))?;
    stream.set_write_timeout(Some(Duration::from_secs(45)))?;
    stream.write_all(request)?;
    stream.flush()?;
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line)?;
    if !line.ends_with('\n') {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete OSR response",
        ));
    }
    Ok(line)
}

pub(super) fn query<T: Serialize, R: DeserializeOwned>(addr: &str, request: &T) -> Result<R> {
    let mut bytes = serde_json::to_vec(request)?;
    bytes.push(b'\n');
    for attempt in 0..2 {
        match exchange(addr, &bytes) {
            Ok(line) => return serde_json::from_str(&line).context("decoding OSR worker response"),
            Err(error)
                if attempt == 0
                    && matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset
                            | io::ErrorKind::ConnectionAborted
                            | io::ErrorKind::BrokenPipe
                            | io::ErrorKind::UnexpectedEof
                    ) =>
            {
                eprintln!("OSR worker connection interrupted; retrying once: {error}");
            }
            Err(error) => return Err(TransportError(error).into()),
        }
    }
    unreachable!("the final attempt always returns")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn retries_an_incomplete_response_on_a_new_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = String::new();
                BufReader::new(stream.try_clone().unwrap()).read_line(&mut request).unwrap();
                assert_eq!(request, "{\"mode\":\"BIKE\"}\n");
                if attempt == 1 {
                    stream.write_all(b"{\"reachable\":[],\"error\":null}\n").unwrap();
                }
            }
        });
        let reply: serde_json::Value = query(&addr, &serde_json::json!({"mode": "BIKE"})).unwrap();
        assert!(reply["error"].is_null());
        server.join().unwrap();
    }
}
