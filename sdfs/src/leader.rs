use crate::helpers::FileKey;
use crate::message_types::sdfs_command::Type;
use crate::message_types::{
    Ack, Delete, FileSizeReq, FileSizeRes, GetReq, KeyServers, LeaderMapReq, LeaderPutReq,
    LeaderReduceReq, LsRes, MapReq, PutReq, ReduceReq, SdfsCommand, ServerMapRes,
};
use crate::node::Node;
use dashmap::DashMap;
use prost::Message;
use rand::seq::{IteratorRandom, SliceRandom};
use std::collections::{HashMap, VecDeque};
use std::iter::{once, repeat, zip};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock, Semaphore};
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{error, info, instrument, warn};

// Define the file table and queues
#[derive(Debug)]
struct FileTable {
    // Map of the file name to the VMs that have the file
    table: DashMap<String, Vec<Ipv4Addr>>,
    actors: DashMap<String, mpsc::Sender<RequestInfo>>, // channel to actor processes
    keys: DashMap<String, Vec<FileKey>>,
    map_reduce_actor: Mutex<mpsc::Sender<MapReduceReqInfo>>, // channel to mapreduce process
}

struct MapReduceReqInfo {
    request: MapReduceAccType,
    stream: TcpStream,
}

#[derive(Debug)]
enum MapReduceAccType {
    Map(MapReq),
    Reduce(ReduceReq),
}

struct RequestInfo {
    request: AccessType,
    stream: TcpStream,
}

#[derive(Debug)]
enum AccessType {
    Read(GetReq),
    Write(PutReq),
}

struct ReduceResult {
    succ_worker: Option<Ipv4Addr>,
    fail_blocks: Vec<(String, KeyServers)>,
}

struct MapResult {
    succ_worker: Option<Ipv4Addr>,
    fail_blocks: Vec<(String, KeyServers)>,
    keys: Vec<String>,
}

async fn send_leader_reduce_req(vm: Ipv4Addr, command: LeaderReduceReq) -> ReduceResult {
    info!("Leader reduce: Starting reduce task");
    let succ = ReduceResult {
        succ_worker: Some(vm),
        fail_blocks: Vec::new(),
    };
    let fail = ReduceResult {
        succ_worker: None,
        fail_blocks: command.key_server_map.clone().into_iter().collect(),
    };
    let message = SdfsCommand {
        r#type: Some(Type::LeaderRedReq(command)),
    }
    .encode_to_vec();
    //Append server port to sender
    let vm = vm.to_string() + ":56552";
    info!("Leader reduce: Sending new Reduce to server: {}", vm);
    let Ok(mut stream) = TcpStream::connect(&vm).await else {
        error!("Failed to contact reduce worker {}", vm);
        return fail;
    };
    info!(
        "Leader reduce: sending reduce messge with size {}",
        message.len()
    );
    if let Err(e) = stream.write_all(&message).await {
        error!(
            "Leader reduce: failed to send message to server with error {}",
            e
        );
    }
    let mut res = Vec::new();
    if let Err(e) = stream.read_to_end(&mut res).await {
        error!(
            "Leader reduce: Failed to get ack from reduce worker {}: {}",
            vm, e
        );
        return fail;
    }
    if let Err(e) = Ack::decode(res.as_slice()) {
        error!(
            "Leader reduce: Failed to decode ack from reduce worker {}: {}",
            vm, e
        );
        return fail;
    }
    info!(
        "Leader reduce: Successfully executed reduce at worker {}",
        vm
    );
    succ
}

async fn send_leader_map_req(vm: Ipv4Addr, command: LeaderMapReq) -> MapResult {
    info!("Leader map: Starting map task");
    let mut succ = MapResult {
        succ_worker: Some(vm),
        fail_blocks: Vec::new(),
        keys: Vec::new(),
    };
    let fail = MapResult {
        succ_worker: None,
        fail_blocks: command.file_server_map.clone().into_iter().collect(),
        keys: Vec::new(),
    };
    let message = SdfsCommand {
        r#type: Some(Type::LeaderMapReq(command)),
    }
    .encode_to_vec();
    //Append server port to sender
    let vm = vm.to_string() + ":56552";
    info!("Leader map: Sending new Map to server: {}", vm);
    let Ok(mut stream) = TcpStream::connect(&vm).await else {
        warn!("Leader map: Failed to contact map worker {}", vm);
        return fail;
    };

    let _ = stream.write_all(&message).await;
    let mut res = Vec::new();
    if let Err(e) = stream.read_to_end(&mut res).await {
        warn!(
            "Leader map: Failed to get ack from map worker {}: {}",
            vm, e
        );
        return fail;
    }
    let Ok(res) = ServerMapRes::decode(res.as_slice()) else {
        warn!("Leader map: Failed to decode ack from map worker {}", vm);
        return fail;
    };
    info!("Leader map: Successfully executed map at worker {}", vm);
    succ.keys = res.keys;
    succ
}

async fn send_leader_put_req<'recv>(
    sender: &Ipv4Addr,
    command: LeaderPutReq,
    fail_receivers: &mut Vec<&'recv Ipv4Addr>,
    receiver: &'recv Ipv4Addr,
    succ_receivers: &mut Vec<&'recv Ipv4Addr>,
) {
    let message = SdfsCommand {
        r#type: Some(Type::LeaderPutReq(command)),
    }
    .encode_to_vec();
    //Append server port to sender
    let sender = sender.to_string() + ":56552";
    info!("Sending new PUT to server: {}", sender);
    let Ok(mut stream) = TcpStream::connect(&sender).await else {
        error!("Failed to contact sender machine {}", sender);
        fail_receivers.push(receiver);
        return;
    };
    let _ = stream.write_all(&message).await;
    let mut res = Vec::new();
    if let Err(e) = stream.read_to_end(&mut res).await {
        error!("Failed to get ack from sender machine {}: {}", sender, e);
        fail_receivers.push(receiver);
        return;
    }
    if let Err(e) = Ack::decode(res.as_slice()) {
        error!("Failed to decode ack from sender machine {}: {}", sender, e);
        fail_receivers.push(receiver);
        return;
    }
    info!("Successfully replicated file at receiver: {}", receiver);
    succ_receivers.push(receiver);
}

async fn get_active_vms(members: Arc<RwLock<Vec<Node>>>) -> Vec<Ipv4Addr> {
    members
        .read()
        .await
        .iter()
        .filter(|node| !node.fail()) // only consider nodes that haven't failed
        .map(|node| node.id())
        .filter_map(|bytes| String::from_utf8(bytes.to_vec()).ok())
        .filter_map(|s| s.split_once('_').map(|(pre, _)| pre)?.parse().ok())
        .collect::<Vec<_>>()
}

impl FileTable {
    fn new(map_req_tx: mpsc::Sender<MapReduceReqInfo>) -> Self {
        FileTable {
            table: DashMap::new(),
            actors: DashMap::new(),
            keys: DashMap::new(),
            map_reduce_actor: Mutex::new(map_req_tx),
        }
    }

    #[instrument(name = "Leader map processor", level = "trace")]
    async fn start_map(
        &self,
        map_req: MapReq,
        mut socket: TcpStream,
        members: Arc<RwLock<Vec<Node>>>,
    ) {
        info!("Leader map: Processing map on leader");
        // Step 1: Find files with the prefix map_req.input_dir
        info!("Leader map: Looking for prefix: {}", map_req.input_dir);
        let mut file_server_map: Vec<_> = self
            .table
            .iter()
            .filter(|elem| elem.key().starts_with(&map_req.input_dir))
            .map(|elem| {
                (
                    elem.key().clone(),
                    KeyServers {
                        servers: elem
                            .value()
                            .clone()
                            .into_iter()
                            .map(|ip| ip.to_string())
                            .collect(),
                    },
                )
            })
            .take(1)
            .collect();

        if file_server_map.is_empty() {
            warn!("Found no server containing the file");
            return;
        }

        info!(
            "Leader map: Found files matching prefix: {:?}",
            file_server_map
        );

        // Step 2: Find active workers containing the executable
        let active_vms = get_active_vms(members.clone()).await;
        if active_vms.is_empty() {
            warn!("Leader map: Unable to pick a target VM");
            return;
        }
        let target_vms: Vec<_> = active_vms
            .choose_multiple(&mut rand::thread_rng(), 4)
            .map(|ip| ip.to_string())
            .collect();

        let mut worker_vms = active_vms.clone();
        {
            // let Some(executable_servers) = self.table.get(&map_req.executable) else {
            //     warn!("Leader map: No executable found in the file system, aborting");
            //     return;
            // };
            // worker_vms.retain(|vm| executable_servers.contains(vm));
            worker_vms.truncate(map_req.num_workers as usize);
        }

        info!("Leader map: Found active workers: {:?}", worker_vms);

        // have the peer upload executable to workers
        let Ok(peer_addr) = socket.peer_addr() else {
            warn!("Unable to get peer address info, aborting");
            return;
        };
        let ip = match peer_addr.ip() {
            IpAddr::V4(ip) => ip,
            IpAddr::V6(ip) => ip.to_ipv4().unwrap(),
        };
        let mut succ_receivers = Vec::new();
        let mut fail_receivers = Vec::new();
        for vm in &worker_vms {
            let command = LeaderPutReq {
                machine: vm.to_string(),
                file_name: map_req.executable.clone(),
            };
            send_leader_put_req(&ip, command, &mut fail_receivers, vm, &mut succ_receivers).await;
        }
        worker_vms = succ_receivers.into_iter().copied().collect();
        info!("Uploaded executable to workers");

        // get file count
        let message = SdfsCommand {
            r#type: Some(Type::FileSizeReq(FileSizeReq {
                file_name: file_server_map[0].0.clone(),
            })),
        }
        .encode_to_vec();

        let mut file_size = None;
        for server in &file_server_map[0].1.servers {
            let res = async {
                let server_addr = format!("{server}:56552");
                let Ok(mut stream) = TcpStream::connect(&server_addr).await else {
                    warn!("Leader map: Failed to contact map worker {}", server_addr);
                    return None;
                };

                let _ = stream.write_all(&message).await;
                let mut res = Vec::new();
                if let Err(e) = stream.read_to_end(&mut res).await {
                    warn!(
                        "Leader map: Failed to get ack from map worker {}: {}",
                        server_addr, e
                    );
                    return None;
                }
                let Ok(res) = FileSizeRes::decode(res.as_slice()) else {
                    warn!(
                        "Leader map: Failed to decode ack from map worker {}",
                        server_addr
                    );
                    return None;
                };
                Some(res.size)
            }
            .await;
            if res.is_some() {
                file_size = res;
            }
        }

        let Some(size) = file_size else {
            return;
        };

        // Step 3: Distribute files among workers
        let mut keys = Vec::new();
        loop {
            info!("Leader map: Initiating map at workers");
            let mut num_workers = worker_vms.len() as u32;
            if num_workers > size {
                num_workers = size;
            }

            let mut task_handlers = JoinSet::new();
            let mut map_results = Vec::new();

            info!("Leader map: sending tasks");
            let chunk_size = (size).div_ceil(num_workers);
            let task_permit: Arc<Semaphore> = Arc::new(Semaphore::new(num_workers as usize));

            for (vm, chunk) in zip(worker_vms.into_iter().cycle(), 0..num_workers) {
                let file = file_server_map[0].0.clone();
                let servers = file_server_map[0].1.clone();
                info!(
                    "Chunk {}: start line {}, end line {}",
                    chunk,
                    chunk * chunk_size,
                    (chunk + 1) * chunk_size - 1
                );
                let command = LeaderMapReq {
                    executable: map_req.executable.clone(),
                    output_prefix: map_req.file_name_prefix.clone(),
                    file_server_map: HashMap::from([(file, servers)]),
                    target_servers: target_vms.clone(),
                    start_line: chunk * chunk_size,
                    end_line: (chunk + 1) * chunk_size - 1,
                    arguments: map_req.arguments.clone(),
                };
                let task_permit_cloned = task_permit.clone();
                if let Ok(permit) = task_permit_cloned.acquire_owned().await {
                    task_handlers.spawn(async move {
                        let res = send_leader_map_req(vm, command).await;
                        drop(permit);
                        res
                    });
                }
            }

            while let Some(join) = task_handlers.join_next().await {
                if let Ok(res) = join {
                    map_results.push(res);
                }
            }
            info!("Leader map: Joined map tasks");
            let (succ_workers_iter, fail_keys_blocks_iter): (Vec<_>, Vec<_>) = map_results
                .into_iter()
                .map(
                    |MapResult {
                         succ_worker,
                         fail_blocks,
                         keys,
                     }| (succ_worker, (fail_blocks, keys)),
                )
                .unzip();
            let (fail_blocks_iter, keys_iter): (Vec<_>, Vec<_>) =
                fail_keys_blocks_iter.into_iter().unzip();
            (worker_vms, file_server_map) = (
                succ_workers_iter.into_iter().flatten().collect(),
                fail_blocks_iter.into_iter().flatten().collect(),
            );
            keys.extend(keys_iter.into_iter().flatten());
            if file_server_map.is_empty() {
                break;
            }
        }
        info!("Leader map: Workers successfully ran map in workers");

        // Step 4: Once successful, populate FileTable.keys with the key and the file name
        for key in keys {
            self.keys
                .entry(key.clone())
                .and_modify(|file_keys| {
                    file_keys.push(FileKey::new(&map_req.file_name_prefix, &key))
                })
                .or_insert(Vec::from([FileKey::new(&map_req.file_name_prefix, &key)]));
            let file_key = FileKey::new(&map_req.file_name_prefix, &key);
            self.table.entry(file_key.to_string()).or_insert(
                target_vms
                    .iter()
                    .map(|ip| ip.parse::<Ipv4Addr>().unwrap())
                    .collect(),
            );
        }

        info!("Leader map: Put files in filetable");

        // Step 5: Send a message to the client that the map is successful
        let ack_buffer = Ack {
            message: "Map successful".to_string(),
        }
        .encode_to_vec();

        info!("Leader map: Sent ack to client");

        if let Err(e) = socket.write_all(&ack_buffer).await {
            warn!("Leader map: Failed to send map ack to client: {:?}", e);
        }
    }

    // Implement the `get_file_line_count` and `send_map_task_to_worker` functions as needed

    #[instrument(name = "Leader reduce processor", level = "trace")]
    async fn start_reduce(
        &self,
        red_req: ReduceReq,
        mut socket: TcpStream,
        members: Arc<RwLock<Vec<Node>>>,
    ) {
        info!("Leader reduce: starting reduce on leader");
        // fetch active workers containing executable
        let key_file_map = self.keys.clone();
        key_file_map.retain(|_, v| {
            v.retain(|e| e.starts_with(&red_req.file_name_prefix));
            !v.is_empty()
        });
        let key_files = key_file_map
            .into_iter()
            .map(|(_, v)| (*v[0]).clone())
            .collect::<Vec<_>>();

        let mut file_server_map = Vec::new();

        for file in key_files {
            let Some(storing_servers) = self.table.get(&file) else {
                error!("Unable to find a key file, aborting reduce");
                return;
            };
            file_server_map.push((
                file,
                KeyServers {
                    servers: storing_servers
                        .clone()
                        .into_iter()
                        .map(|ip| ip.to_string())
                        .collect(),
                },
            ));
        }

        let active_vms = get_active_vms(members.clone()).await;
        let Some(target_vm) = active_vms.choose(&mut rand::thread_rng()).copied() else {
            warn!("Unable to pick a target VM");
            return;
        };

        let mut worker_vms = active_vms.clone();
        {
            // let Some(executable_servers) = self.table.get(&red_req.executable) else {
            //     info!("No executable found in the file system, abortin");
            //     return;
            // };
            // worker_vms.retain(|vm| executable_servers.contains(vm));
            worker_vms.truncate(red_req.num_workers as usize);
        }

        info!("Leader reduce: found active servers: {:?}", worker_vms);

        // have the peer upload executable to workers
        let Ok(peer_addr) = socket.peer_addr() else {
            warn!("Unable to get peer address info, aborting");
            return;
        };
        let ip = match peer_addr.ip() {
            IpAddr::V4(ip) => ip,
            IpAddr::V6(ip) => ip.to_ipv4().unwrap(),
        };
        let mut succ_receivers = Vec::new();
        let mut fail_receivers = Vec::new();
        for vm in &worker_vms {
            let command = LeaderPutReq {
                machine: vm.to_string(),
                file_name: red_req.executable.clone(),
            };
            send_leader_put_req(&ip, command, &mut fail_receivers, vm, &mut succ_receivers).await;
        }
        worker_vms = succ_receivers.into_iter().copied().collect();
        info!("Uploaded executable to workers");

        // send reduce requests to workers
        loop {
            info!("Leader reduce: sending reduce requests to workers");
            let worker_vms_num = worker_vms.len();

            let mut task_handlers = JoinSet::new();
            let mut reduce_results = Vec::new();

            let chunk_size = (file_server_map.len()).div_ceil(worker_vms_num);
            let task_permit: Arc<Semaphore> = Arc::new(Semaphore::new(worker_vms_num));

            for (vm, key_file_chunk) in zip(
                worker_vms.into_iter().cycle(),
                file_server_map.chunks(chunk_size),
            ) {
                let command = LeaderReduceReq {
                    key_server_map: HashMap::from_iter(key_file_chunk.to_vec()),
                    target_server: target_vm.to_string(),
                    output_file: red_req.output_file.clone(),
                    executable: red_req.executable.clone(),
                };
                let task_permit_cloned = task_permit.clone();
                if let Ok(permit) = task_permit_cloned.acquire_owned().await {
                    task_handlers.spawn(async move {
                        let res = send_leader_reduce_req(vm, command).await;
                        drop(permit);
                        res
                    });
                }
            }

            while let Some(join) = task_handlers.join_next().await {
                if let Ok(res) = join {
                    reduce_results.push(res);
                }
            }
            let (succ_workers_iter, fail_blocks_iter): (Vec<_>, Vec<_>) = reduce_results
                .into_iter()
                .map(
                    |ReduceResult {
                         succ_worker,
                         fail_blocks,
                     }| (succ_worker, fail_blocks),
                )
                .unzip();
            (worker_vms, file_server_map) = (
                succ_workers_iter.into_iter().flatten().collect(),
                fail_blocks_iter.into_iter().flatten().collect(),
            );
            if file_server_map.is_empty() {
                break;
            }
        }
        info!("Leader reduce: sent reduce requests");

        // request end server replicate at 3 more server
        let mut succ_receivers = Vec::new();
        let mut fail_receivers = Vec::new();
        let mut missing = 3;
        loop {
            let rep_servers: Vec<_> = active_vms
                .iter()
                .filter(|s| !succ_receivers.contains(s))
                .choose_multiple(&mut rand::thread_rng(), missing);
            for vm in rep_servers {
                let command = LeaderPutReq {
                    machine: vm.to_string(),
                    file_name: red_req.output_file.clone(),
                };
                send_leader_put_req(
                    &target_vm,
                    command,
                    &mut fail_receivers,
                    vm,
                    &mut succ_receivers,
                )
                .await;
            }
            if fail_receivers.is_empty() {
                break;
            }
            missing = fail_receivers.len();
            fail_receivers.clear();
        }

        info!("Leader reduce: replicated output file");

        self.table.insert(
            red_req.output_file,
            once(target_vm)
                .chain(succ_receivers.into_iter().copied())
                .collect::<Vec<_>>(),
        );

        // end request
        let ack_buffer = Ack {
            message: "Reduce successful".to_string(),
        }
        .encode_to_vec();

        if let Err(e) = socket.write_all(&ack_buffer).await {
            warn!("Failed to send reduce ack to client: {:?}", e);
        }
    }

    #[instrument(name = "Leader read processor", level = "trace")]
    async fn start_read(&self, get_req: GetReq, mut socket: TcpStream) {
        info!("Starting Read at leader");
        // Update the state to indicate that a read operation is ongoing.
        let file_name = &get_req.file_name;

        // Perform the file reading.
        // Check if the file is available on any VMs.
        if let Some(vms) = self.table.get(file_name) {
            let response = LsRes {
                machines: vms.iter().map(|ip| ip.to_string()).collect(),
            };
            info!("Ls Response for get: {:?}", response);
            let buffer = response.encode_to_vec();
            if let Err(e) = socket.write_all(&buffer).await {
                warn!("Failed to send information to client: {:?}", e);
            }
        } else {
            info!("File not found: {}", get_req.file_name);
            if let Err(e) = socket.write_all(b"FILE_NOT_FOUND").await {
                warn!("Failed to send 'file not found' notification: {:?}", e);
            }
        }
        let mut client_ack_buffer = [0; 1024];

        let Ok(n) = socket.read(&mut client_ack_buffer).await else {
            warn!("received no ack from client");
            return;
        };
        info!("Received ACK from client");
        if let Err(e) = Ack::decode(&client_ack_buffer[..n]) {
            warn!("Unable to decode ACK server message, {e}");
        };
    }

    #[instrument(name = "Leader ls processor", level = "trace")]
    async fn start_ls(&self, file_name: &str, mut socket: TcpStream) {
        // Perform the file reading.
        // Check if the file is available on any VMs.
        info!("Starting Ls at leader");
        if let Some(vms) = self.table.get(file_name) {
            let response = LsRes {
                machines: vms.iter().map(|ip| ip.to_string()).collect(),
            };
            info!("Ls Response for LsReq: {:?}", response);
            let mut buffer = Vec::new();
            response.encode(&mut buffer).unwrap();
            if let Err(e) = socket.write_all(&buffer).await {
                warn!("Failed to send information to client: {:?}", e);
            }
        } else {
            let response = LsRes {
                machines: Vec::new(),
            }
            .encode_to_vec();
            info!("File not found: {}", file_name);
            if let Err(e) = socket.write_all(&response).await {
                warn!("Failed to send 'file not found' notification: {:?}", e);
            }
        }
    }

    #[instrument(name = "Leader delete processor", level = "trace")]
    async fn delete_file(&self, del_req: Delete, mut socket: TcpStream) {
        info!("Starting Delete at leader");
        let file_name = &del_req.file_name;
        if let Some(vms) = self.table.get(file_name) {
            for machine in vms.iter() {
                let server_address = machine.to_string() + ":56552";
                let Ok(mut server_stream) = TcpStream::connect(&server_address).await else {
                    warn!(
                        "Unable to connect to server {}, ignoring server",
                        server_address
                    );
                    continue;
                };

                let del_buffer = SdfsCommand {
                    r#type: Some(Type::Del(del_req.clone())),
                }
                .encode_to_vec();

                // println!("Request buffer: {:?}", del_buffer);
                if let Err(e) = server_stream.write_all(&del_buffer).await {
                    warn!("Unable to send request to server: {}", e);
                    continue;
                }
                info!("Sent request to server maybe");
                let mut ack_buffer = [0; 1024];
                let Ok(n) = server_stream.read(&mut ack_buffer).await else {
                    info!("Nothing to delete");
                    continue;
                };
                info!("Received ACK from server");
                if let Err(e) = Ack::decode(&ack_buffer[..n]) {
                    warn!("Unable to decode ACK server message {}", e);
                    continue;
                };
                info!("File delete from machine: {}", machine);
            }
            // Send ack to client
            let ack_buffer = Ack {
                message: "File DELETE successful".to_string(),
            }
            .encode_to_vec();

            if let Err(e) = socket.write_all(&ack_buffer).await {
                warn!("Failed to send information to client: {:?}", e);
            }
        } else {
            info!("File not found: {}", del_req.file_name);
            if let Err(e) = socket.write_all(b"FILE_NOT_FOUND").await {
                warn!("Failed to send 'file not found' notification: {:?}", e);
            }
        }
        if let Some(tx) = self.actors.get(file_name) {
            drop(tx);
        }
        // Remove the file from the file table
        self.table.remove(file_name);
        self.actors.remove(file_name);
    }

    #[instrument(name = "Leader write processor", level = "trace")]
    async fn start_write(
        &self,
        put_req: PutReq,
        mut socket: TcpStream,
        members: Arc<RwLock<Vec<Node>>>,
    ) {
        info!("Starting Write at leader");
        let file_name = &put_req.file_name;

        let active_vms = get_active_vms(members).await;

        let start_time = Instant::now(); // Capture the start time
        info!("Active VMs (write handler task): {:?}", active_vms);
        // Check if we have at least 4 active VMs.
        if active_vms.len() < 4 {
            error!("Not enough active VMs to proceed.");
            return;
        }

        // Now, select 4 VMs at random from the list of active VMs.
        let selected_vm_names: Vec<_>;
        {
            let mut rng = rand::thread_rng(); // Create a random number generator.
                                              // Convert Bytes back to String if necessary, depending on your setup.
            selected_vm_names = active_vms.choose_multiple(&mut rng, 4).cloned().collect();
        }

        info!("Selected VMs for write: {:?}", selected_vm_names);
        // Update the state to indicate that a write operation is ongoing.

        // Send back the response to the client.
        let response = LsRes {
            machines: selected_vm_names.iter().map(|ip| ip.to_string()).collect(),
        };
        let buffer = response.encode_to_vec();
        if let Err(e) = socket.write_all(&buffer).await {
            warn!("Failed to send information to client: {:?}", e);
            warn!("File reps not sent: {}", put_req.file_name);
        }
        info!("Ls Response for put: {:?}", response);
        // Update the state to indicate that the write operation is complete.
        let mut client_ls_buffer = [0; 1024];

        let Ok(n) = socket.read(&mut client_ls_buffer).await else {
            warn!("received no ack from client");
            return;
        };
        info!("Received ACK from client");
        let Ok(succ_vms) = LsRes::decode(&client_ls_buffer[..n]) else {
            warn!("Unable to decode ACK server message");
            return;
        };

        if !succ_vms.machines.is_empty() {
            // TODO: Write this after insert is complete
            self.table.insert(
                file_name.to_string(),
                succ_vms
                    .machines
                    .into_iter()
                    .map(|v| v.parse().unwrap())
                    .collect(),
            );
        }
        let duration = start_time.elapsed();
        info!("Total time taken to write the file: {:?}", duration);
    }

    #[instrument(name = "Leader failure listener", level = "trace")]
    async fn failure_listener(
        &self,
        mut rx_leader: mpsc::Receiver<Vec<String>>,
        members: Arc<RwLock<Vec<Node>>>,
    ) {
        while let Some(machine) = rx_leader.recv().await {
            warn!(
                "Detected failures from machines: {:?}, leader responding",
                machine
            );
            //let mut files_stored = Vec::new();
            for mut elem in self.table.iter_mut() {
                let (key, val) = elem.pair_mut();
                let prev_size = val.len();
                val.retain(|elem| !machine.contains(&elem.to_string()));

                let mut missing = prev_size - val.len();
                warn!("Missing {} replicas", missing);
                if missing == 0 || missing == prev_size {
                    info!("No need to replicate");
                    continue;
                }
                let start_time = Instant::now();
                info!("Replicating file: {}", key);

                let active_vms = get_active_vms(members.clone()).await;

                info!("Active VMs (failure task) {:?}", active_vms);

                let mut machines_to_req: Vec<_>;
                let mut machines_to_recv: Vec<_>;

                let mut succ_receivers = Vec::new();
                let mut fail_receivers = Vec::new();
                loop {
                    {
                        let mut rng = rand::thread_rng();
                        machines_to_req = val.choose_multiple(&mut rng, missing).collect();
                        machines_to_recv = active_vms
                            .iter()
                            .filter(|s| !val.contains(s) && !succ_receivers.contains(s))
                            .choose_multiple(&mut rng, missing);
                    }
                    info!(
                        "Machines already with file: {:?}, {:?}",
                        val, succ_receivers
                    );
                    info!("Machines selected to send file: {:?}", machines_to_req);
                    info!("Machines selected to receive file: {:?}", machines_to_recv);
                    let rem_elem = machines_to_req[0];
                    for (sender, receiver) in zip(
                        machines_to_req.iter().chain(repeat(&rem_elem)),
                        machines_to_recv.iter(),
                    ) {
                        let command = LeaderPutReq {
                            machine: receiver.to_string(),
                            file_name: key.to_string(),
                        };
                        send_leader_put_req(
                            sender,
                            command,
                            &mut fail_receivers,
                            receiver,
                            &mut succ_receivers,
                        )
                        .await;
                    }
                    if fail_receivers.is_empty() {
                        break;
                    }
                    missing = fail_receivers.len();
                    machines_to_recv.retain(|elem| fail_receivers.contains(elem));
                    fail_receivers.clear();
                }
                val.extend(succ_receivers.into_iter().cloned());
                let duration = start_time.elapsed();
                info!("Total time taken to replicate the file: {:?}", duration);
            }
        }
    }
}

#[instrument(name = "Leader request handler", level = "trace")]
async fn handle_request(
    file_table: Arc<FileTable>,
    command: SdfsCommand,
    mut stream: TcpStream,
    members: Arc<RwLock<Vec<Node>>>,
) {
    match command.r#type {
        Some(Type::GetReq(get_req)) => {
            // Send the request to this file's helper task, or spawn the helper if not running
            if let Some(tx) = file_table.actors.get_mut(&get_req.file_name) {
                info!("Actor already running, sending READ");
                let _ = tx
                    .send(RequestInfo {
                        request: AccessType::Read(get_req),
                        stream,
                    })
                    .await;
            } else {
                info!("Actor not running, starting up actor and send READ");
                let (tx, rx) = mpsc::channel(10);
                let file_name = get_req.file_name.clone();
                let _ = tx
                    .send(RequestInfo {
                        request: AccessType::Read(get_req),
                        stream,
                    })
                    .await;
                file_table.actors.insert(file_name, tx);
                let file_table_cloned = file_table.clone();
                tokio::spawn(async move {
                    process_operations(file_table_cloned, members, rx).await;
                });
            }
        }
        Some(Type::PutReq(put_req)) => {
            // Similar to GET
            if let Some(tx) = file_table.actors.get_mut(&put_req.file_name) {
                info!("Actor already running, sending WRITE");
                let _ = tx
                    .send(RequestInfo {
                        request: AccessType::Write(put_req),
                        stream,
                    })
                    .await;
            } else {
                info!("Actor not running, starting up actor and send WRITE");
                let (tx, rx) = mpsc::channel(10);
                let file_name = put_req.file_name.clone();
                let _ = tx
                    .send(RequestInfo {
                        request: AccessType::Write(put_req),
                        stream,
                    })
                    .await;
                file_table.actors.insert(file_name, tx);
                let file_table_cloned = file_table.clone();
                tokio::spawn(async move {
                    process_operations(file_table_cloned, members, rx).await;
                });
            }
        }
        Some(Type::LsReq(ls_req)) => {
            // Similarly, enqueue the write request and then determine if it can start.
            file_table.start_ls(&ls_req.file_name, stream).await;
        }
        Some(Type::Del(del_req)) => {
            file_table.delete_file(del_req, stream).await;
        }
        Some(Type::MapReq(map_req)) => {
            let mr_tx = file_table.map_reduce_actor.lock().await;
            let _ = mr_tx
                .send(MapReduceReqInfo {
                    request: MapReduceAccType::Map(map_req),
                    stream,
                })
                .await;
        }
        Some(Type::RedReq(red_req)) => {
            let mr_tx = file_table.map_reduce_actor.lock().await;
            let _ = mr_tx
                .send(MapReduceReqInfo {
                    request: MapReduceAccType::Reduce(red_req),
                    stream,
                })
                .await;
        }
        _ => {
            stream.write_all(b"INVALID_COMMAND").await.unwrap();
        }
    }
}

#[instrument(name = "Leader request scheduler", level = "trace")]
async fn actor_listener(
    mut rx: mpsc::Receiver<RequestInfo>,
    queued_requests: Arc<Mutex<VecDeque<(AccessType, TcpStream)>>>,
    stop_tx: oneshot::Sender<()>,
    notifier: Arc<Notify>,
) {
    while let Some(info) = rx.recv().await {
        info!("Putting request onto queue");
        let mut queued_requests = queued_requests.lock().await;
        let length = queued_requests.len();
        let mut insert_idx = length;
        let slice_with_idx = queued_requests
            .make_contiguous()
            .iter()
            .enumerate()
            .collect::<Vec<_>>();
        match &info.request {
            AccessType::Read(_) => {
                for window in slice_with_idx.as_slice().windows(4) {
                    if let [(_, (AccessType::Write(_), _)), (_, (AccessType::Write(_), _)), (_, (AccessType::Write(_), _)), (idx, (AccessType::Write(_), _))] =
                        window
                    {
                        info!("Potential starvation detected for READ request, rescheduling");
                        insert_idx = *idx;
                        break;
                    }
                }
            }
            AccessType::Write(_) => {
                for window in slice_with_idx.as_slice().windows(4) {
                    if let [(_, (AccessType::Read(_), _)), (_, (AccessType::Read(_), _)), (_, (AccessType::Read(_), _)), (idx, (AccessType::Read(_), _))] =
                        window
                    {
                        info!("Potential starvation detected for WRITE request, rescheduling");
                        insert_idx = *idx;
                        break;
                    }
                }
            }
        }
        queued_requests.insert(insert_idx, (info.request, info.stream));
        notifier.notify_one();
    }
    notifier.notify_one();
    let _ = stop_tx.send(());
}

#[instrument(name = "Leader request processor", level = "trace")]
async fn process_operations(
    file_table: Arc<FileTable>,
    members: Arc<RwLock<Vec<Node>>>,
    rx: mpsc::Receiver<RequestInfo>,
) {
    info!("Starting operation processor");
    let queued_requests: Arc<Mutex<VecDeque<(AccessType, TcpStream)>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    let queue_clone = queued_requests.clone();

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

    let notifier = Arc::new(Notify::new());

    let notifiee = notifier.clone();

    let task_permit: Arc<Semaphore> = Arc::new(Semaphore::new(2));

    let listener_handler = tokio::spawn(async move {
        actor_listener(rx, queue_clone, stop_tx, notifier).await;
    });

    loop {
        let mut queued = queued_requests.lock().await;
        let front = queued.pop_front();
        drop(queued);
        match front {
            Some((AccessType::Read(get_req), stream)) => {
                let file_table_cloned = file_table.clone();
                let task_permit_cloned = task_permit.clone();
                if let Ok(permit) = task_permit_cloned.acquire_owned().await {
                    info!("Machine acquired read permit");
                    tokio::spawn(async move {
                        file_table_cloned.start_read(get_req, stream).await;
                        //let _permit = permit;
                        drop(permit);
                    });
                } else {
                    error!("Machine failed to acquire read permit");
                }
            }
            Some((AccessType::Write(put_req), stream)) => {
                let file_table_cloned = file_table.clone();
                let members_cloned = members.clone();
                let task_permit_cloned = task_permit.clone();
                if let Ok(permit) = task_permit_cloned.acquire_many_owned(2).await {
                    info!("Machine acquired write permit");
                    tokio::spawn(async move {
                        file_table_cloned
                            .start_write(put_req, stream, members_cloned)
                            .await;
                        //let _permit = permit;
                        drop(permit);
                    });
                } else {
                    error!("Failed to acquire write permit");
                }
            }
            None => notifiee.notified().await,
        };
        match stop_rx.try_recv() {
            Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                break;
            }
            _ => {}
        }
    }

    let _ = listener_handler.await;
}

#[instrument(name = "Leader map reduce scheduler", level = "trace")]
async fn map_reduce_actor_listener(
    mut rx: mpsc::Receiver<MapReduceReqInfo>,
    queued_requests: Arc<Mutex<VecDeque<(MapReduceAccType, TcpStream)>>>,
    stop_tx: oneshot::Sender<()>,
    notifier: Arc<Notify>,
) {
    while let Some(info) = rx.recv().await {
        let mut queued_requests = queued_requests.lock().await;
        queued_requests.push_back((info.request, info.stream));
        notifier.notify_one();
    }
    notifier.notify_one();
    let _ = stop_tx.send(());
}

#[instrument(name = "Leader map reduce request processor", level = "trace")]
async fn process_map_reduce(
    file_table: Arc<FileTable>,
    members: Arc<RwLock<Vec<Node>>>,
    rx: mpsc::Receiver<MapReduceReqInfo>,
) {
    let queued_requests: Arc<Mutex<VecDeque<(MapReduceAccType, TcpStream)>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    let queue_clone = queued_requests.clone();

    let (stop_tx, mut stop_rx) = oneshot::channel::<()>();

    let notifier = Arc::new(Notify::new());

    let notifiee = notifier.clone();

    let listener_handler = tokio::spawn(async move {
        map_reduce_actor_listener(rx, queue_clone, stop_tx, notifier).await;
    });
    loop {
        let mut queued = queued_requests.lock().await;
        let front = queued.pop_front();
        drop(queued);
        match front {
            Some((MapReduceAccType::Map(map_req), stream)) => {
                file_table.start_map(map_req, stream, members.clone()).await
            }
            Some((MapReduceAccType::Reduce(red_req), stream)) => {
                file_table
                    .start_reduce(red_req, stream, members.clone())
                    .await
            }
            None => notifiee.notified().await,
        };
        match stop_rx.try_recv() {
            Ok(_) | Err(oneshot::error::TryRecvError::Closed) => {
                break;
            }
            _ => {}
        }
    }

    let _ = listener_handler.await;
}

#[instrument(name = "Leader startup and listener", level = "trace")]
pub async fn run_leader(
    mut rx_leader: mpsc::Receiver<Vec<String>>,
    members: Arc<RwLock<Vec<Node>>>,
    timeout: Duration,
) {
    let (map_req_tx, map_req_rx) = mpsc::channel::<MapReduceReqInfo>(10);
    let file_table = Arc::new(FileTable::new(map_req_tx));

    // Getting the hostname and binding should be done without blocking.
    let raw_machine_name = hostname::get().unwrap().into_string().unwrap();
    info!("Leader machine name: {}", raw_machine_name);
    let listener = TcpListener::bind([raw_machine_name, ":56553".to_string()].join(""))
        .await
        .unwrap();
    info!("Leader listening on port 56553");

    // // Sleep before doing anything
    // sleep(timeout).await;
    // println!("Leader starting after wait...Will collect files from other machines");
    // // TODO: Fetch list stores from servers
    // // For every member in the membership list, send a request to get the list of files stored on that machine
    // //loop over every machine in the membership list
    // for node in members.read().await.iter() {
    //     // Skip if the node is marked as failed
    //     if node.fail() {
    //         warn!("Skipping failed node {:?}", node.id());
    //         continue;
    //     }
    //     info!("Sending LeaderStoreReq to node {:?}", node.id());
    //     // Create a LeaderStoreReq message
    //     let message = SdfsCommand {
    //         r#type: Some(Type::LeaderStoreReq(LeaderStoreReq {
    //             message: "Give me your files".to_string(),
    //         })),
    //     }
    //     .encode_to_vec();

    //     // Connect to the node and send the request
    //     let machine_name = String::from_utf8(node.id().to_vec()).unwrap();
    //     let ip_address = machine_name.split('_').next().unwrap();
    //     info!("Connecting to node {}", ip_address);
    //     let addr = format!("{}:56552", ip_address);
    //     match TcpStream::connect(addr).await {
    //         Ok(mut stream) => {
    //             if stream.write_all(&message).await.is_ok() {
    //                 // Read the response
    //                 let mut res_buf = Vec::new();
    //                 match stream.read_to_end(&mut res_buf).await {
    //                     Ok(_) => {
    //                         // Decode the LeaderStoreRes message
    //                         if let Ok(res) = LeaderStoreRes::decode(res_buf.as_slice()) {
    //                             // Update the file table
    //                             info!("LeaderStoreRes from {}: {:?}", machine_name, res);
    //                             let file_table = file_table.clone();
    //                             for file_name in res.files {
    //                                 file_table
    //                                     .table
    //                                     .entry(file_name)
    //                                     .and_modify(|e| e.push(ip_address.parse().unwrap()))
    //                                     .or_insert_with(|| vec![ip_address.parse().unwrap()]);
    //                             }
    //                         } else {
    //                             warn!("Failed to decode LeaderStoreRes from {}", machine_name);
    //                         }
    //                     }
    //                     Err(e) => warn!("Failed to read from node {}: {}", machine_name, e),
    //                 }
    //             } else {
    //                 warn!("Failed to send LeaderStoreReq to {}", machine_name);
    //             }
    //         }
    //         Err(e) => warn!("Failed to connect to node {}: {}", machine_name, e),
    //     }
    // }

    while rx_leader.try_recv().is_ok() {}

    let file_table_cloned = file_table.clone();
    let mem_cloned = members.clone();
    tokio::spawn(async move {
        file_table_cloned
            .failure_listener(rx_leader, mem_cloned)
            .await
    });

    let mr_file_table = file_table.clone();
    let mr_mem = members.clone();
    tokio::spawn(async move {
        process_map_reduce(mr_file_table, mr_mem, map_req_rx).await;
    });

    loop {
        let (mut socket, _) = listener.accept().await.unwrap(); // Declare socket as mutable
        let file_table = file_table.clone();

        let processor_ft = file_table.clone();
        let mem = members.clone();
        tokio::spawn(async move {
            let mut buffer = [0u8; 1024];

            let n = socket.read(&mut buffer).await.unwrap();
            if n == 0 {
                return;
            }

            let command: SdfsCommand = SdfsCommand::decode(&buffer[..n]).unwrap();
            handle_request(processor_ft, command, socket, mem).await;
        });
    }
}
