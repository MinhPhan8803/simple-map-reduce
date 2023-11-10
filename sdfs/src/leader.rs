use crate::message_types::sdfs_command::Type;
use crate::message_types::{
    Ack, Delete, GetReq, LeaderPutReq, LeaderStoreReq, LeaderStoreRes, LsRes, PutReq, SdfsCommand,
};
use crate::node::Node;
use dashmap::DashMap;
use prost::Message;
use rand::seq::{IteratorRandom, SliceRandom};
use std::collections::VecDeque;
use std::iter::{repeat, zip};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex, Notify, RwLock, Semaphore};
use tokio::time::{sleep, Duration};
use tracing::{error, info, instrument, warn};

// Define the file table and queues
#[derive(Debug)]
struct FileTable {
    // Map of the file name to the VMs that have the file
    table: DashMap<String, Vec<String>>,
    actors: DashMap<String, mpsc::Sender<RequestInfo>>, // channel to actor processes
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

impl FileTable {
    fn new() -> Self {
        FileTable {
            table: DashMap::new(),
            actors: DashMap::new(),
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
                machines: vms.clone(),
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
                machines: vms.clone(),
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
                let server_address = machine.to_owned() + ":56552";
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

        let read_guard = members.read().await;

        let active_vms = read_guard
            .iter()
            .filter(|node| !node.fail()) // only consider nodes that haven't failed
            .map(|node| node.id())
            .filter_map(|bytes| String::from_utf8(bytes.to_vec()).ok())
            .filter_map(|s| s.split_once('_').map(|(pre, _)| pre.to_string()))
            .collect::<Vec<_>>();
        drop(read_guard);

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
            machines: selected_vm_names,
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
            self.table.insert(file_name.to_string(), succ_vms.machines);
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
                val.retain(|elem| !machine.contains(elem));

                let mut missing = prev_size - val.len();
                warn!("Missing {} replicas", missing);
                if missing == 0 || missing == prev_size {
                    info!("No need to replicate");
                    continue;
                }
                let start_time = Instant::now();
                info!("Replicating file: {}", key);

                let mem = members.read().await;
                let mem_lock = mem.clone();
                drop(mem);
                let active_vms: Vec<_> = mem_lock
                    .iter()
                    .filter(|node| !node.fail()) // only consider nodes that haven't failed
                    .map(|node| node.id())
                    .filter_map(|bytes| String::from_utf8(bytes.to_vec()).ok())
                    .filter_map(|s| s.split_once('_').map(|(pre, _)| pre.to_string()))
                    .collect::<Vec<_>>();

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
                        let message = SdfsCommand {
                            r#type: Some(Type::LeaderPutReq(LeaderPutReq {
                                machine: receiver.to_string(),
                                file_name: key.to_string(),
                            })),
                        }
                        .encode_to_vec();
                        //Append server port to sender
                        let sender = sender.to_string() + ":56552";
                        info!("Sending new PUT to server: {}", sender);
                        let Ok(mut stream) = TcpStream::connect(&sender).await else {
                            error!("Failed to contact sender machine {}", sender);
                            fail_receivers.push(*receiver);
                            continue;
                        };
                        let _ = stream.write_all(&message).await;
                        let mut res = Vec::new();
                        if let Err(e) = stream.read_to_end(&mut res).await {
                            error!("Failed to get ack from sender machine {}: {}", sender, e);
                            fail_receivers.push(*receiver);
                            continue;
                        }
                        if let Err(e) = Ack::decode(res.as_slice()) {
                            error!("Failed to decode ack from sender machine {}: {}", sender, e);
                            fail_receivers.push(*receiver);
                            continue;
                        }
                        info!("Successfully replicated file at receiver: {}", receiver);
                        succ_receivers.push(receiver);
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
        Some(crate::message_types::sdfs_command::Type::GetReq(get_req)) => {
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
        Some(crate::message_types::sdfs_command::Type::PutReq(put_req)) => {
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
        Some(crate::message_types::sdfs_command::Type::LsReq(ls_req)) => {
            // Similarly, enqueue the write request and then determine if it can start.
            file_table.start_ls(&ls_req.file_name, stream).await;
        }
        Some(crate::message_types::sdfs_command::Type::Del(del_req)) => {
            file_table.delete_file(del_req, stream).await;
        }
        // ... handle other command types similarly ...
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

#[instrument(name = "Leader startup and listener", level = "trace")]
pub async fn run_leader(
    mut rx_leader: mpsc::Receiver<Vec<String>>,
    members: Arc<RwLock<Vec<Node>>>,
    timeout: Duration,
) {
    let file_table = Arc::new(FileTable::new());

    // Getting the hostname and binding should be done without blocking.
    let raw_machine_name = hostname::get().unwrap().into_string().unwrap();
    info!("Leader machine name: {}", raw_machine_name);
    let listener = TcpListener::bind([raw_machine_name, ":56553".to_string()].join(""))
        .await
        .unwrap();
    info!("Leader listening on port 56553");

    // Sleep before doing anything
    sleep(timeout).await;
    println!("Leader starting after wait...Will collect files from other machines");
    // TODO: Fetch list stores from servers
    // For every member in the membership list, send a request to get the list of files stored on that machine
    //loop over every machine in the membership list
    for node in members.read().await.iter() {
        // Skip if the node is marked as failed
        if node.fail() {
            warn!("Skipping failed node {:?}", node.id());
            continue;
        }
        info!("Sending LeaderStoreReq to node {:?}", node.id());
        // Create a LeaderStoreReq message
        let message = SdfsCommand {
            r#type: Some(Type::LeaderStoreReq(LeaderStoreReq {
                message: "Give me your files".to_string(),
            })),
        }
        .encode_to_vec();

        // Connect to the node and send the request
        let machine_name = String::from_utf8(node.id().to_vec()).unwrap();
        let ip_address = machine_name.split('_').next().unwrap();
        info!("Connecting to node {}", ip_address);
        let addr = format!("{}:56552", ip_address);
        match TcpStream::connect(addr).await {
            Ok(mut stream) => {
                if stream.write_all(&message).await.is_ok() {
                    // Read the response
                    let mut res_buf = Vec::new();
                    match stream.read_to_end(&mut res_buf).await {
                        Ok(_) => {
                            // Decode the LeaderStoreRes message
                            if let Ok(res) = LeaderStoreRes::decode(res_buf.as_slice()) {
                                // Update the file table
                                info!("LeaderStoreRes from {}: {:?}", machine_name, res);
                                let file_table = file_table.clone();
                                for file_name in res.files {
                                    file_table
                                        .table
                                        .entry(file_name)
                                        .and_modify(|e| e.push(machine_name.clone()))
                                        .or_insert_with(|| vec![machine_name.clone()]);
                                }
                            } else {
                                warn!("Failed to decode LeaderStoreRes from {}", machine_name);
                            }
                        }
                        Err(e) => warn!("Failed to read from node {}: {}", machine_name, e),
                    }
                } else {
                    warn!("Failed to send LeaderStoreReq to {}", machine_name);
                }
            }
            Err(e) => warn!("Failed to connect to node {}: {}", machine_name, e),
        }
    }

    while rx_leader.try_recv().is_ok() {}

    let file_table_cloned = file_table.clone();
    let mem_cloned = members.clone();
    // tokio::spawn(async move {
    //     file_table_cloned
    //         .failure_listener(rx_leader, mem_cloned)
    //         .await
    // });

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
