use crate::message_types::{sdfs_command::Type, GetData, GetReq, SdfsCommand};
use prost::{length_delimiter_len, Message};
use std::ops::Deref;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt};
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

pub async fn write_to_buf<T: AsyncWrite + std::marker::Unpin>(
    buffer: &mut T,
    mut stream: TcpStream,
) {
    let mut res_buffer: Vec<u8> = Vec::new();
    let mut remaining_buffer = [0; 5120];
    while let Ok(n) = stream.read(&mut remaining_buffer).await {
        //println!("Read data from client with size {}", n);
        if n == 0 {
            break;
        }

        let (left, _) = remaining_buffer.split_at_mut(n);
        res_buffer.extend_from_slice(left);
        remaining_buffer.fill(0);

        while let Ok(res_data) = GetData::decode_length_delimited(res_buffer.as_slice()) {
            //println!("Decoded data from client, seeking file to: {}", res_data.offset);
            let raw_length = res_data.encoded_len();
            let delim_length = length_delimiter_len(raw_length);
            res_buffer = res_buffer.split_off(raw_length + delim_length);
            // if let Err(e) = file.seek(io::SeekFrom::Start(res_data.offset)).await {
            //     error!("Unable to seek file {}, aborting", e);
            //     return;
            // };
            // Write the fetched data to a local file
            if let Err(e) = buffer.write_all(&res_data.data).await {
                error!(
                    "Unable to write to file at offset {} with error {}",
                    res_data.offset, e
                );
                break;
            }
        }
    }
}

#[instrument(name = "GET helper function", level = "trace")]
pub async fn client_get_helper(
    machines: Vec<String>,
    sdfs_file_name: &str,
    local_file_name: &str,
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
        write_to_buf(&mut file, server_stream).await;

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
