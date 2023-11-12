use crate::message_types::sdfs_command::Type;
use crate::message_types::{
    Ack, Delete, Fail, GetData, GetReq, LsReq, LsRes, MultiRead, MultiWrite, PutData, PutReq,
    SdfsCommand,
};
use futures::stream::{self, StreamExt};
use prost::{length_delimiter_len, Message};
use std::{io, sync::Arc, time::Instant};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::{fs, net::TcpStream, sync::RwLock};
use tracing::{error, info, instrument, warn};

#[derive(Debug)]
pub struct Client {
    leader_ip: Arc<RwLock<String>>,
}

struct PutInProgress {
    server_address: String,
    server_stream: TcpStream,
}

impl Client {
    pub fn new(leader_ip: Arc<RwLock<String>>) -> Self {
        Client { leader_ip }
    }

    #[instrument(name = "Client Put", level = "trace")]
    pub async fn put_file(&self, local_file_name: &str, sdfs_file_name: &str) {
        info!("Starting PUT at client to file: {}", sdfs_file_name);
        let start_time = Instant::now();
        // Read the local file
        let Ok(mut file) = fs::File::open(local_file_name).await else {
            println!("Unable to open file");
            warn!("Unable to open file");
            return;
        };
        // let Ok(_file_size) = file.metadata().await else {
        //     println!("Unable to get file metadata");
        //     return;
        // };

        let leader_address = {
            let locked = self.leader_ip.read().await;
            locked.clone() + ":56553"
        };

        let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
            error!("Unable to contact leader, aborting");
            return;
        };
        let req_buffer = SdfsCommand {
            r#type: Some(Type::PutReq(PutReq {
                file_name: sdfs_file_name.to_string(),
            })),
        }
        .encode_to_vec();
        let _ = leader_stream.write_all(&req_buffer).await;

        let mut res_buffer = [0; 1024];
        let Ok(n) = leader_stream.read(&mut res_buffer).await else {
            error!("No leader response to request: ");
            return;
        };
        let Ok(machine_list) = LsRes::decode(&res_buffer[..n]) else {
            error!("Unable to decode leader response, aborting");
            return;
        };

        info!(
            "Client received list from server: {:?}",
            machine_list.machines
        );
        let mut servers_in_prog = Vec::new();

        for machine in machine_list.machines {
            let server_address = machine.clone() + ":56552";
            let Ok(mut server_stream) = TcpStream::connect(&server_address).await else {
                warn!(
                    "Unable to connect to server {}, ignoring server",
                    server_address
                );
                continue;
            };
            //println!("Request buffer: {:?}", req_buffer);
            if let Err(e) = server_stream.write_all(&req_buffer).await {
                warn!("Unable to send request to server: {}, ignoring server", e);
                continue;
            }
            info!("Sent request to server maybe");
            let mut ack_buffer = [0; 1024];
            let Ok(n) = server_stream.read(&mut ack_buffer).await else {
                warn!("No server response to request, ignoring server");
                continue;
            };
            info!("Received ACK from server");
            if let Err(e) = Ack::decode(&ack_buffer[..n]) {
                warn!("Unable to decode ACK server message {}, ignoring serve", e);
                continue;
            };
            info!("Decoded ACK from server");
            info!("Established communication with server");
            servers_in_prog.push(PutInProgress {
                server_address: machine,
                server_stream,
            });
        }

        let mut file_buf = [0; 4096];
        let mut file_offset = 0;
        let mut mesg = PutData {
            machine: "".to_string(),
            file_name: sdfs_file_name.to_string(),
            offset: 0,
            data: Vec::new(),
        };

        while let Ok(read_size) = file.read(&mut file_buf).await {
            if read_size == 0 {
                break;
            }

            let send_buffer = file_buf.iter().take_while(|&&b| b != 0).copied().collect();
            //println!("Send buffer: {:?}", send_buffer);
            //println!("File buf: {:?}", file_buf);

            mesg.offset = file_offset as u64;
            mesg.data = send_buffer;

            let mesg_buffer = mesg.encode_length_delimited_to_vec();

            let mesg_buffer_references = stream::repeat(&mesg_buffer);

            servers_in_prog = stream::iter(servers_in_prog)
                .zip(mesg_buffer_references)
                .filter_map(|(mut server, buffer)| async move {
                    match server.server_stream.write_all(buffer).await {
                        Ok(_) => Some(server),
                        Err(_) => {
                            warn!(
                                "Unable to write to server {}, ignoring server",
                                server.server_address
                            );
                            None
                        }
                    }
                })
                .collect()
                .await;

            file_offset += read_size;
            file_buf.fill(0);
        }

        for server in &mut servers_in_prog {
            let _ = server.server_stream.shutdown().await;
        }

        let ls_buffer = LsRes {
            machines: servers_in_prog
                .into_iter()
                .map(|server| server.server_address)
                .collect(),
        }
        .encode_to_vec();

        println!("File PUT successful");
        let duration = start_time.elapsed();
        println!("Total time taken to write the file: {:?}", duration);
        let _ = leader_stream.write_all(&ls_buffer).await;
        let _ = leader_stream.shutdown().await;
    }

    #[instrument(name = "Client Get", level = "trace")]
    pub async fn get_file(&self, sdfs_file_name: &str, local_file_name: &str) {
        info!("Starting GET at client from file: {}", sdfs_file_name);
        let start_time = Instant::now();
        // Create and send a GetReq message to the leader

        // Connect to the leader and send the message
        let leader_address = {
            let locked = self.leader_ip.read().await;
            locked.clone() + ":56553"
        };
        let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
            error!("Unable to contact leader, aborting");
            return;
        };
        let get_req = GetReq {
            file_name: sdfs_file_name.to_string(),
        };
        let req_buffer = SdfsCommand {
            r#type: Some(Type::GetReq(get_req)),
        }
        .encode_to_vec();
        let _ = leader_stream.write_all(&req_buffer).await;

        // Await the response from the leader
        let mut res_buffer = [0; 1024];
        let Ok(n) = leader_stream.read(&mut res_buffer).await else {
            error!("No leader response to request: ");
            return;
        };
        let Ok(machine_list) = LsRes::decode(&res_buffer[..n]) else {
            error!("Unable to decode leader response, aborting");
            return;
        };

        let ack_buffer = Ack {
            message: "File GET completed successfully".to_string(),
        }
        .encode_to_vec();

        if machine_list.machines.is_empty() {
            error!("No replicas available or file not found");
            let _ = leader_stream.write_all(&ack_buffer).await;
            return;
        }

        // Use the list of replicas from the leader's response to fetch the file from one of the replicas.
        // For simplicity, we'll just use the first replica. In real-world scenarios, you might want to add
        // fault tolerance here by trying the next replica if one fails.
        match client_get_helper(machine_list.machines, sdfs_file_name, local_file_name).await {
            Ok(_) => {
                let ack_buffer = Ack {
                    message: "File getting completed successfully".to_string(),
                }
                .encode_to_vec();
                let _ = leader_stream.write_all(&ack_buffer).await;
                let duration = start_time.elapsed();
                println!("File GET successful");
                println!("Total time taken to write the file: {:?}", duration);
            }
            Err(e) => {
                println!("File GET unsuccessful with error {}", e);
                let fail_buffer = Fail { message: e }.encode_to_vec();
                let _ = leader_stream.write_all(&fail_buffer).await;
            }
        };
        let _ = leader_stream.write_all(&ack_buffer).await;
        let _ = leader_stream.shutdown().await;
    }

    #[instrument(name = "Client Delete", level = "trace")]
    pub async fn delete_file(&self, sdfs_file_name: &str) {
        info!("Starting Delete on client side");
        let leader_address = {
            let locked = self.leader_ip.read().await;
            locked.clone() + ":56553"
        };
        let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
            error!("Unable to contact leader, aborting");
            return;
        };
        let del_req = Delete {
            file_name: sdfs_file_name.to_string(),
        };
        let req_buffer = SdfsCommand {
            r#type: Some(Type::Del(del_req)),
        }
        .encode_to_vec();
        let _ = leader_stream.write_all(&req_buffer).await;

        let mut res_buffer = [0; 1024];
        let Ok(n) = leader_stream.read(&mut res_buffer).await else {
            error!("No leader response to request: ");
            println!("File delete failed");
            return;
        };
        if let Err(e) = Ack::decode(&res_buffer[..n]) {
            error!("Unable to decode leader ACK: {}", e);
            println!("File delete failed");
            return;
        };
        println!("File Delete successful");
    }

    #[instrument(name = "Client Ls", level = "trace")]
    pub async fn list_file(&self, sdfs_file_name: &str) {
        info!("Starting Ls on client side");
        let leader_address = {
            let locked = self.leader_ip.read().await;
            locked.clone() + ":56553"
        };
        let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
            error!("Unable to contact leader, aborting");
            return;
        };
        let ls_req = LsReq {
            file_name: sdfs_file_name.to_string(),
        };
        let req_buffer = SdfsCommand {
            r#type: Some(Type::LsReq(ls_req)),
        }
        .encode_to_vec();
        let _ = leader_stream.write_all(&req_buffer).await;

        let mut res_buffer = [0; 1024];
        let Ok(n) = leader_stream.read(&mut res_buffer).await else {
            error!("No leader response to request: ");
            return;
        };
        let Ok(machine_list) = LsRes::decode(&res_buffer[..n]) else {
            error!("Unable to decode leader response, aborting");
            return;
        };
        for machine in machine_list.machines {
            println!("{}", machine);
        }
    }

    #[instrument(name = "Client Multi-Read", level = "trace")]
    pub async fn multi_read(&self, sdfs_file_name: &str, local_file_name: &str, vms: &[&str]) {
        info!("Starting Multi-Read on client side");
        let start_time = Instant::now();
        stream::iter(vms)
            .for_each_concurrent(None, |vm| async move {
                let addr = format!("{vm}:56552");
                let Ok(mut server_stream) = TcpStream::connect(addr).await else {
                    warn!("Failure to connect to address {vm}:56552");
                    return;
                };
                let leader_ip = {
                    let leader_locked = self.leader_ip.read().await;
                    leader_locked.clone()
                };
                let multi_read_req = SdfsCommand {
                    r#type: Some(Type::MultiRead(MultiRead {
                        sdfs_file_name: sdfs_file_name.to_string(),
                        local_file_name: local_file_name.to_string(),
                        leader_ip,
                    })),
                }
                .encode_to_vec();
                let _ = server_stream.write_all(&multi_read_req).await;
                let mut res_buffer = [0; 1024];
                let Ok(n) = server_stream.read(&mut res_buffer).await else {
                    warn!("No server response to request");
                    return;
                };
                if let Err(e) = Ack::decode(&res_buffer[..n]) {
                    warn!("Unable to decode ACK message from server: {}", e);
                }
            })
            .await;
        let duration = start_time.elapsed();
        println!(
            "Time taken until last reader reads the file: {:?}",
            duration
        );
    }

    #[instrument(name = "Client Multi-Write", level = "trace")]
    pub async fn multi_write(&self, local_file_name: &str, sdfs_file_name: &str, vms: &[&str]) {
        let start_time = Instant::now();
        stream::iter(vms)
            .for_each_concurrent(None, |vm| async move {
                let addr = format!("{vm}:56552");
                let Ok(mut server_stream) = TcpStream::connect(addr).await else {
                    return;
                };
                let leader_ip = {
                    let leader_locked = self.leader_ip.read().await;
                    leader_locked.clone()
                };
                let multi_read_req = SdfsCommand {
                    r#type: Some(Type::MultiWrite(MultiWrite {
                        local_file_name: local_file_name.to_string(),
                        sdfs_file_name: sdfs_file_name.to_string(),
                        leader_ip,
                    })),
                }
                .encode_to_vec();
                let _ = server_stream.write_all(&multi_read_req).await;
                let mut res_buffer = [0; 1024];
                let Ok(n) = server_stream.read(&mut res_buffer).await else {
                    warn!("No server response to request");
                    return;
                };
                if let Err(e) = Ack::decode(&res_buffer[..n]) {
                    warn!("Unable to decode ACK message from server: {}", e);
                }
            })
            .await;
        let duration = start_time.elapsed();
        println!(
            "Time taken until last writer writes the file: {:?}",
            duration
        );
    }

    pub async fn map(
        &self,
        executable_name: &str,
        num_workers: u32,
        file_name_prefix: &str,
        input_dir: &str,
    ) {
    }

    pub async fn reduce(
        &self,
        executable_name: &str,
        num_workers: u32,
        file_name_prefix: &str,
        input_dir: &str,
        is_delete: bool,
    ) {
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
        let mut res_buffer: Vec<u8> = Vec::new();
        let mut remaining_buffer = [0; 5120];
        while let Ok(n) = server_stream.read(&mut remaining_buffer).await {
            //println!("Client read from server with size: {}", n);
            if n == 0 {
                break;
            }

            let (left, _) = remaining_buffer.split_at_mut(n);
            res_buffer.extend_from_slice(left);
            remaining_buffer.fill(0);

            while let Ok(res_data) = GetData::decode_length_delimited(res_buffer.as_slice()) {
                let raw_length = res_data.encoded_len();
                let delim_length = length_delimiter_len(raw_length);
                res_buffer = res_buffer.split_off(raw_length + delim_length);
                if let Err(e) = file.seek(io::SeekFrom::Start(res_data.offset)).await {
                    error!("Unable to seek file {}, aborting", e);
                    return Err("Operation on file failed".to_string());
                };
                // Write the fetched data to a local file
                if let Err(e) = file.write_all(&res_data.data).await {
                    error!(
                        "Unable to write to file at offset {} with error {}",
                        res_data.offset, e
                    );
                    break;
                }
            }
        }
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
