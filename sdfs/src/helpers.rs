use crate::message_types::{sdfs_command::Type, GetReq, SdfsCommand};
use prost::Message;
use std::ops::Deref;
use tokio::fs;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{error, info, instrument, warn};

#[derive(Debug, Clone)]
pub struct FileKey {
    name: String,
}

impl FileKey {
    pub fn new(file_prefix: &str, key: &str) -> FileKey {
        FileKey {
            name: format!("{file_prefix}_{key}"),
        }
    }
}

impl Deref for FileKey {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.name
    }
}

impl std::fmt::Display for FileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

#[instrument(name = "Buf write helper", level = "trace")]
pub async fn write_to_buf<T: AsyncWrite + std::marker::Unpin + std::fmt::Debug>(
    buffer: &mut T,
    stream: TcpStream,
    line_range: Option<(u32, u32)>,
) {
    let mut read_buf = Vec::new();
    let mut buf_reader = BufReader::new(stream);
    if let Some((start_line, end_line)) = line_range {
        let mut line_count = 0;
        while let Ok(size) = buf_reader.read_until(b'\n', &mut read_buf).await {
            info!("Read from stream with size: {}", size);
            if size == 0 {
                break;
            }

            if start_line <= line_count && line_count <= end_line {
                info!("Currently at line {}, within range", line_count);
                if let Err(e) = buffer.write_all(&read_buf).await {
                    error!("Unable to write to file with error {}", e);
                    break;
                }
            }

            line_count += 1;
            read_buf.clear();
        }
    } else {
        while let Ok(size) = buf_reader.read_until(b'\n', &mut read_buf).await {
            if size == 0 {
                break;
            }
            if let Err(e) = buffer.write_all(&read_buf).await {
                error!("Unable to write to file with error {}", e);
                break;
            }
            read_buf.clear();
        }
    }
}

#[instrument(name = "GET helper function", level = "trace")]
pub async fn client_get_helper(
    machines: Vec<String>,
    sdfs_file_name: &str,
    local_file_name: &str,
    line_range: Option<(u32, u32)>,
) -> Result<(), String> {
    let Ok(mut file) = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(local_file_name)
        .await
    else {
        error!("Unable to open file");
        return Err("Unable to open local file".to_string());
    };

    for machine in machines {
        if let Err(e) = file.rewind().await {
            error!("Unable to rewind file {}, aborting", e);
            return Err("Operation on file failed".to_string());
        };

        let server_address = machine + ":56552";
        let Ok(mut server_stream) = TcpStream::connect(&server_address).await else {
            warn!(
                "Unable to connect to server {}, moving to the next",
                server_address
            );
            continue;
        };
        let get_req = GetReq {
            file_name: sdfs_file_name.to_string(),
        };
        let server_req_buffer = SdfsCommand {
            r#type: Some(Type::GetReq(get_req)),
        }
        .encode_to_vec();
        if let Err(e) = server_stream.write_all(&server_req_buffer).await {
            warn!(
                "Unable to send request to server {} with error: {}",
                server_address, e
            );
            continue;
        };
        info!("Successfully sent to server");

        // Receive the file data from the replica
        write_to_buf(&mut file, server_stream, line_range).await;

        info!("Client GET finished");
        if let Err(e) = file.sync_all().await {
            error!("Unable to sync file {e}");
            return Err("Unable to sync file".to_string());
        } else {
            return Ok(());
        }
    }
    Err("Unable to successfully get file from any server".to_string())
}

pub fn split_id_to_components<T: Deref<Target = [u8]>>(raw_id: &T) -> Option<(&str, &str)> {
    let Ok(id) = std::str::from_utf8(raw_id) else {
        return None;
    };
    let id_elems = id.split('_').collect::<Vec<_>>();
    let ip = id_elems[0];
    let port = id_elems[1];
    Some((ip, port))
}
