use crate::helpers::{client_get_helper, write_to_buf, FileKey};
use crate::message_types::{sdfs_command::Type, SdfsCommand};
use crate::message_types::{
    Ack, Delete, Fail, FileSizeReq, FileSizeRes, GetReq, LeaderMapReq, LeaderPutReq,
    LeaderReduceReq, LeaderStoreRes, LsRes, MultiRead, MultiWrite, PutReq, ServerMapReq,
    ServerMapRes, ServerReduceReq,
};
use futures::{stream, StreamExt};
use prost::Message;
use std::{fmt, process::Command, sync::Arc};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::{fs, sync::Mutex};
use tracing::{error, info, instrument, warn};

#[derive(Debug, Clone)]
pub struct LocalFileList {
    list: Vec<String>,
}

impl LocalFileList {
    pub fn new() -> LocalFileList {
        LocalFileList { list: Vec::new() }
    }
    pub fn list_mut(&mut self) -> &mut Vec<String> {
        &mut self.list
    }
    pub fn list(&self) -> &[String] {
        &self.list
    }
}

impl fmt::Display for LocalFileList {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        for file_name in self.list() {
            writeln!(f, "{}", file_name)?;
        }
        write!(f, "")
    }
}

enum ServerPutFlavor {
    Put,
    Map,
    Reduce,
}

async fn put_from_server(file_name: String, ip: String, flavor: ServerPutFlavor) {
    let get_req = GetReq {
        file_name: match flavor {
            ServerPutFlavor::Put => file_name.clone(),
            ServerPutFlavor::Map | ServerPutFlavor::Reduce => format!("mrout/{file_name}"),
        },
    };
    info!("Connecting to the other server {}", ip);
    //Add server port 56552 to the end of the machine string
    let machine = ip + ":56552";
    info!("Connecting to the other server from server {}", machine);
    let Ok(mut inter_server_stream) = TcpStream::connect(machine).await else {
        warn!("Unable to connect to the other server");
        return;
    };
    let req_buf = SdfsCommand {
        r#type: match flavor {
            ServerPutFlavor::Put => Some(Type::PutReq(PutReq { file_name })),
            ServerPutFlavor::Map => Some(Type::ServerMapReq(ServerMapReq {
                output_file: file_name,
            })),
            ServerPutFlavor::Reduce => Some(Type::ServerRedReq(ServerReduceReq {
                output_file: file_name,
            })),
        },
    }
    .encode_to_vec();
    let _ = inter_server_stream.write_all(&req_buf).await;
    let mut res_buf = [0; 1024];
    let Ok(n) = inter_server_stream.read(&mut res_buf).await else {
        warn!("Failed to read ACK from the other server");
        return;
    };
    if let Err(e) = Ack::decode(&res_buf[..n]) {
        warn!("Unable to decode ACK message: {}", e);
        return;
    }

    handle_get(get_req, inter_server_stream).await;
}

async fn handle_put(
    put_req: PutReq,
    mut stream: TcpStream,
    local_file_list: Arc<Mutex<LocalFileList>>,
) {
    info!("Handling client PUT request");
    let ack_buffer = Ack {
        message: "File PUT acknowledged".to_string(),
    }
    .encode_to_vec();
    let _ = stream.write_all(&ack_buffer).await;

    let path = format!("/home/sdfs/{}", put_req.file_name);
    let Ok(mut file) = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
    else {
        error!("Unable to open file");
        return;
    };

    write_to_buf(&mut file, stream, None).await;

    if let Err(e) = file.sync_all().await {
        error!("Unable to sync file {e}");
    } else {
        let mut file_list = local_file_list.lock().await;
        file_list.list_mut().push(put_req.file_name);
        info!("Server handled client PUT successfully");
    }
}

async fn handle_leader_put(leader_put_req: LeaderPutReq, mut stream: TcpStream) {
    info!("Handling leader PUT request at server");
    put_from_server(
        leader_put_req.file_name,
        leader_put_req.machine,
        ServerPutFlavor::Put,
    )
    .await;

    info!("Server handled leader PUT successfully");
    let leader_ack = Ack {
        message: "Server PUT successful".to_string(),
    }
    .encode_to_vec();
    let _ = stream.write_all(&leader_ack).await;
    let _ = stream.shutdown().await;
}

#[instrument(name = "Server Get", level = "trace")]
async fn handle_get(get_req: GetReq, mut stream: TcpStream) {
    info!("Handling GET request");
    let path = format!("/home/sdfs/{}", get_req.file_name);
    let Ok(file) = fs::File::open(path).await else {
        warn!("Unable to open file {}", get_req.file_name);
        return;
    };

    let mut file_buf = Vec::new();
    let mut buf_reader = BufReader::new(file);

    info!("Server beginning send");
    while let Ok(size) = buf_reader.read_until(b'\n', &mut file_buf).await {
        if size == 0 {
            break;
        }
        if let Err(e) = stream.write_all(&file_buf).await {
            warn!("Unable to write to client {}", e);
        }
        file_buf.clear();
    }

    info!("Server handled GET request successfully");
    let _ = stream.shutdown().await;
}

#[instrument(name = "Server Delete", level = "trace")]
async fn handle_del(
    del_req: Delete,
    mut stream: TcpStream,
    local_file_list: Arc<Mutex<LocalFileList>>,
) {
    let path = format!("/home/sdfs/{}", del_req.file_name);
    let _ = fs::remove_file(path).await;
    let mut file_list = local_file_list.lock().await;
    file_list
        .list_mut()
        .retain(|elem| *elem != del_req.file_name);
    let ack_buffer = Ack {
        message: "File DELETE successful".to_string(),
    }
    .encode_to_vec();
    info!("Server deleted file {}", del_req.file_name);
    let _ = stream.write_all(&ack_buffer).await;
    let _ = stream.shutdown().await;
}

#[instrument(name = "Server Store", level = "trace")]
async fn handle_leader_store(mut stream: TcpStream, local_file_list: Arc<Mutex<LocalFileList>>) {
    info!("Handling leader store request at server");
    let resp = LeaderStoreRes {
        files: local_file_list.lock().await.list().to_vec(),
    }
    .encode_to_vec();
    let _ = stream.write_all(&resp).await;
    let _ = stream.shutdown().await;
}

#[instrument(name = "Server Multi-Read", level = "trace")]
async fn handle_multi_read(mut client_stream: TcpStream, multi_read_req: MultiRead) {
    let leader_address = multi_read_req.leader_ip + ":56553";
    let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
        error!("Unable to contact leader");
        return;
    };

    let req_buffer = SdfsCommand {
        r#type: Some(Type::GetReq(GetReq {
            file_name: multi_read_req.sdfs_file_name.clone(),
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

    if !machine_list.machines.is_empty() {
        let _ = client_get_helper(
            machine_list.machines,
            &multi_read_req.sdfs_file_name,
            &multi_read_req.local_file_name,
            None,
        )
        .await;
    }

    let leader_ack_buffer = Ack {
        message: "Received list from server".to_string(),
    }
    .encode_to_vec();
    let client_ack_buffer = Ack {
        message: "Successfully read from server".to_string(),
    }
    .encode_to_vec();
    let _ = leader_stream.write_all(&leader_ack_buffer).await;
    let _ = leader_stream.shutdown().await;
    let _ = client_stream.write_all(&client_ack_buffer).await;
    let _ = client_stream.shutdown().await;
}

#[instrument(name = "Server MultiWrite", level = "trace")]
async fn handle_multi_write(mut client_stream: TcpStream, multi_write_req: MultiWrite) {
    let leader_address = multi_write_req.leader_ip + ":56553";
    let Ok(mut leader_stream) = TcpStream::connect(leader_address).await else {
        error!("Unable to contact leader");
        return;
    };

    let req_buffer = SdfsCommand {
        r#type: Some(Type::PutReq(PutReq {
            file_name: multi_write_req.sdfs_file_name.clone(),
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

    stream::iter(machine_list.machines)
        .for_each_concurrent(None, |machine| async {
            let machine_address = machine + ":56552";
            let Ok(mut inter_server_stream) = TcpStream::connect(machine_address).await else {
                warn!("Unable to connect to the other server");
                return;
            };

            let get_req = GetReq {
                file_name: multi_write_req.local_file_name.clone(),
            };

            let req_buf = SdfsCommand {
                r#type: Some(Type::PutReq(PutReq {
                    file_name: multi_write_req.sdfs_file_name.clone(),
                })),
            }
            .encode_to_vec();

            let _ = inter_server_stream.write_all(&req_buf).await;
            let mut res_buf = [0; 1024];
            let Ok(n) = inter_server_stream.read(&mut res_buf).await else {
                warn!("Failed to read ACK from the other server");
                return;
            };
            if let Err(e) = Ack::decode(&res_buf[..n]) {
                warn!("Unable to decode ACK message: {}", e);
                return;
            }

            handle_get(get_req, inter_server_stream).await;
        })
        .await;

    let leader_ack_buffer: Vec<u8> = Ack {
        message: "Server PUT successful".to_string(),
    }
    .encode_to_vec();
    let _ = leader_stream.write_all(&leader_ack_buffer).await;
    let _ = leader_stream.shutdown().await;

    let client_ack_buffer = Ack {
        message: "Successfully read from server".to_string(),
    }
    .encode_to_vec();
    let _ = client_stream.write_all(&client_ack_buffer).await;
    let _ = client_stream.shutdown().await;
}

#[instrument(name = "Server Map", level = "trace")]
async fn handle_map(mut leader_stream: TcpStream, map_req: LeaderMapReq) {
    info!("Server map: Processing map on server");
    // run executable and on the file from map_req.file_name

    // First, fetch the file from the SDFS server
    let mut files = Vec::new();
    let mut local_files = Vec::new();
    for (file, servers) in map_req.file_server_map.into_iter() {
        let local_file = format!("/home/sdfs/mrin/{file}");
        if let Err(e) = client_get_helper(
            servers.servers,
            &file,
            &local_file,
            Some((map_req.start_line, map_req.end_line)),
        )
        .await
        {
            warn!(
                "Server map: Unable to fetch file from server: {}, aborting",
                e
            );
            continue;
        }
        files.push(file);
        local_files.push(local_file);
    }

    info!("Server map: Fetched files from servers");
    // run the executable and collect keys
    // assume executable output keys to terminal
    let Some(keys) = tokio::task::block_in_place(|| {
        let Ok(raw_output) = Command::new("python3")
            .args(
                [
                    &format!("/home/sdfs/{}", &map_req.executable),
                    &map_req.output_prefix,
                ]
                .into_iter()
                .chain(files.iter())
                .collect::<Vec<_>>(),
            )
            .output()
        else {
            warn!("Server map: unable to run executable");
            return None;
        };
        let Ok(output) = std::str::from_utf8(&raw_output.stdout) else {
            warn!("Server map: unable to parse keys");
            return None;
        };
        if let Ok(stderr) = std::str::from_utf8(&raw_output.stderr) {
            info!("Server map: stderr {}", stderr);
        }
        Some(
            output
                .lines()
                .map(|line| line.to_string())
                .collect::<Vec<_>>(),
        )
    }) else {
        return;
    };

    for local_file in local_files {
        let _ = fs::remove_file(local_file).await;
    }

    info!("Server map: successfully ran executables");
    // PUT the output files to the target SDFS server
    for key in &keys {
        let file_name = FileKey::new(&map_req.output_prefix, key);
        stream::iter(&map_req.target_servers)
            .for_each_concurrent(None, |server| async {
                put_from_server(
                    file_name.to_string(),
                    server.to_string(),
                    ServerPutFlavor::Map,
                )
                .await;
            })
            .await;
        let path = format!("/home/sdfs/mrout/{file_name}");
        let _ = fs::remove_file(path).await;
    }

    info!("Server map: successfully put files on target servers");
    // ack the leader
    let leader_ack_buffer = ServerMapRes { keys }.encode_to_vec();
    let _ = leader_stream.write_all(&leader_ack_buffer).await;
    let _ = leader_stream.shutdown().await;
}

#[instrument(name = "Server Reduce", level = "trace")]
async fn handle_reduce(mut leader_stream: TcpStream, red_req: LeaderReduceReq) {
    info!("Server reduce: Processing reduce on server");
    // fetch files
    let mut files = Vec::new();
    let mut local_keys = Vec::new();
    for (key, servers) in red_req.key_server_map.into_iter() {
        let local_key = format!("/home/sdfs/mrin/{key}");
        if let Err(e) = client_get_helper(servers.servers.clone(), &key, &local_key, None).await {
            error!("Unable to fetch key file: {}", e);
            return;
        }
        files.push(key);
        local_keys.push(local_key);
    }
    info!("Finished fetching files");

    // run executable and send to target server
    if let Err(e) = tokio::task::block_in_place(|| {
        Command::new("python3")
            .args(
                [
                    &format!("/home/sdfs/{}", &red_req.executable),
                    &red_req.output_file,
                ]
                .into_iter()
                .chain(files.iter())
                .collect::<Vec<_>>(),
            )
            .output()
    }) {
        error!("Unable to run executable: {}", e);
        return;
    }
    info!("Finishing running executable");

    for local_key in local_keys {
        let _ = fs::remove_file(local_key).await;
    }

    put_from_server(
        red_req.output_file.clone(),
        red_req.target_server,
        ServerPutFlavor::Reduce,
    )
    .await;
    info!("Finished PUT'ing");

    let path = format!("/home/sdfs/mrout/{}", red_req.output_file);
    let _ = fs::remove_file(path).await;

    // end request
    let leader_ack_buffer = Ack {
        message: "Worker successfully executed reduce".to_string(),
    }
    .encode_to_vec();
    let _ = leader_stream.write_all(&leader_ack_buffer).await;
    let _ = leader_stream.shutdown().await;
}

async fn handle_server_map_reduce(
    mut server_stream: TcpStream,
    output_file: String,
    is_reduce: bool,
    local_file_list: Arc<Mutex<LocalFileList>>,
) {
    info!("Server M-R: Reading operation results");
    let ack_buffer = Ack {
        message: "Reduce acknowledged".to_string(),
    }
    .encode_to_vec();
    let _ = server_stream.write_all(&ack_buffer).await;

    let mut data_buffer = Vec::new();
    write_to_buf(&mut data_buffer, server_stream, None).await;

    let path = format!("/home/sdfs/{}", output_file);
    let Ok(file) = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(path)
        .await
    else {
        warn!("Server M-R receiver: Unable to open file");
        return;
    };

    let mut file_lock = fd_lock::RwLock::new(file);

    let Ok(mut locked_file) = tokio::task::block_in_place(|| file_lock.write()) else {
        error!("Server M-R receiver: Unable to acquire a file lock");
        return;
    };
    if let Err(e) = locked_file.write_all(&data_buffer).await {
        error!(
            "Server M-R receiver: Unable to append to file with error {}",
            e
        );
        return;
    };
    info!("Server wrote map-reduce data successfully");
    if is_reduce {
        let mut file_list = local_file_list.lock().await;
        file_list.list_mut().push(output_file);
    }
}

async fn handle_file_size(mut leader_stream: TcpStream, req: FileSizeReq) {
    let path = format!("/home/sdfs/{}", req.file_name);
    let Ok(file) = tokio::fs::File::open(path).await else {
        warn!("File size: Unable to open file");
        return;
    };
    let mut file_buf = Vec::new();
    let mut buf_reader = BufReader::new(file);

    let mut line_count = 0;
    while let Ok(size) = buf_reader.read_until(b'\n', &mut file_buf).await {
        if size == 0 {
            break;
        }
        line_count += 1;
        file_buf.clear();
    }

    let response = FileSizeRes { size: line_count }.encode_to_vec();
    let _ = leader_stream.write_all(&response).await;
    let _ = leader_stream.shutdown().await;
}

#[instrument(name = "Server startup and listener", level = "trace")]
pub async fn run_server(local_file_list: Arc<Mutex<LocalFileList>>) {
    let raw_machine_name = hostname::get().unwrap().into_string().unwrap();
    let addr = raw_machine_name + ":56552"; // Ensure this is the correct IP and port.
    let Ok(listener) = TcpListener::bind(&addr).await else {
        println!("Failed to bind server, aborting");
        return;
    };
    info!("Server listening on port 56552");

    let mut buffer = [0; 1024];
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            error!("Unable to accept TCP socket connection");
            continue;
        };
        info!("Accepted connection from client");

        match stream.read(&mut buffer).await {
            Ok(size) => {
                info!("Received data from client with size {}", size);
                let actual_buffer = &buffer[..size];
                //println!("Data: {:?}", actual_buffer);

                let command: SdfsCommand = match SdfsCommand::decode(actual_buffer) {
                    Ok(cmd) => cmd,
                    Err(e) => {
                        warn!("Failed to decode command: {}", e);
                        continue;
                    }
                };

                info!("Received command at server: {:?}", command);

                match command.r#type {
                    Some(Type::PutReq(put_req)) => {
                        // ... [snipped: unchanged PutData handling code]
                        info!("Received PutData command from client");
                        let file_list = local_file_list.clone();
                        tokio::spawn(async move {
                            handle_put(put_req, stream, file_list).await;
                        });
                    }
                    Some(Type::GetReq(get_req)) => {
                        info!("Received GetData command from client");
                        // Better error handling instead of unwrap()
                        // In your server's GetData and PutData handling, construct the file path like this:
                        tokio::spawn(async move {
                            handle_get(get_req, stream).await;
                        });
                    }
                    Some(Type::Del(del_req)) => {
                        info!("Received Delete command at server");
                        let file_list = local_file_list.clone();
                        tokio::spawn(async move {
                            handle_del(del_req, stream, file_list).await;
                        });
                    }
                    Some(Type::LeaderPutReq(leader_put_req)) => {
                        info!("Received Put command from the leader");
                        tokio::spawn(async move {
                            handle_leader_put(leader_put_req, stream).await;
                        });
                    }
                    Some(Type::LeaderStoreReq(_)) => {
                        info!("Received Store request from the leader");
                        let file_list = local_file_list.clone();
                        tokio::spawn(async move {
                            handle_leader_store(stream, file_list).await;
                        });
                    }
                    Some(Type::MultiRead(multi_read_req)) => {
                        info!("Received MultiRead command from client");
                        tokio::spawn(async move {
                            handle_multi_read(stream, multi_read_req).await;
                        });
                    }
                    Some(Type::MultiWrite(multi_write_req)) => {
                        info!("Received MultiWrite command from client");
                        tokio::spawn(async move {
                            handle_multi_write(stream, multi_write_req).await;
                        });
                    }
                    Some(Type::LeaderMapReq(map_req)) => {
                        info!("Received Map request from the leader");
                        tokio::spawn(async move {
                            handle_map(stream, map_req).await;
                        });
                    }
                    Some(Type::LeaderRedReq(red_req)) => {
                        info!("Received Reduce request from the leader");
                        tokio::spawn(async move {
                            handle_reduce(stream, red_req).await;
                        });
                    }
                    Some(Type::ServerRedReq(req)) => {
                        let file_list = local_file_list.clone();
                        tokio::spawn(async move {
                            handle_server_map_reduce(stream, req.output_file, true, file_list)
                                .await;
                        });
                    }
                    Some(Type::ServerMapReq(req)) => {
                        let file_list = local_file_list.clone();
                        tokio::spawn(async move {
                            handle_server_map_reduce(stream, req.output_file, false, file_list)
                                .await;
                        });
                    }
                    Some(Type::FileSizeReq(req)) => {
                        tokio::spawn(async move {
                            handle_file_size(stream, req).await;
                        });
                    }
                    _ => {
                        // Other types of commands are not handled here
                        tokio::spawn(async move {
                            let fail_buffer = Fail {
                                message: "Invalid command".to_string(),
                            }
                            .encode_to_vec();
                            let _ = stream.write_all(&fail_buffer).await;
                        });
                    }
                }
            }
            Err(e) => warn!("Failed to read from socket: {}", e),
        }
        buffer.fill(0);
    }
}
