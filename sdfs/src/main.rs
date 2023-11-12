mod client;
mod leader;
mod node;
mod receiver;
mod sender;
mod server;
use node::Node;
use server::LocalFileList;
pub mod message_types {
    include!(concat!(env!("OUT_DIR"), "/message_types.rs"));
}
pub mod member_list {
    include!(concat!(env!("OUT_DIR"), "/member_list.rs"));
}
use crate::member_list::MemberList;
use anyhow::{anyhow, Result};
use bytes::Bytes;
use chrono::offset::Local;
use inquire::Text;
use prost::Message;
use std::{fs::File, net::IpAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tracing::{error, info, instrument, trace_span, Instrument};

#[tokio::main]
async fn main() {
    // Check if we should run as a leader or as a node (client/server)
    let Ok(mut vm_num) = get_vm_num() else {
        // Handle the error here if needed
        println!("Failed to get machine number");
        return;
    };
    if vm_num == 0 {
        vm_num = 10;
    }

    let file: File = match File::create(format!("/home/logs/vm{vm_num}.log")) {
        Ok(f) => f,
        Err(e) => {
            println!("Log file not found {}", e);
            return;
        }
    };
    tracing_subscriber::fmt().with_writer(Arc::new(file)).init();

    let Ok(local_ip) = local_ip_address::local_ip() else {
        println!("Local ip not found");
        return;
    };

    let Ok(sock) = UdpSocket::bind(format!("{local_ip}:12307")).await else {
        println!("Unable to bind UDP Socket to {}:12307", local_ip);
        return;
    };

    let local_time_raw = Local::now();
    let local_time = local_time_raw.to_rfc3339();

    let sender_id: Arc<str> = Arc::from(format!("{local_ip}_12307_{local_time}"));
    let initial_node = Node::new(
        Bytes::copy_from_slice(sender_id.as_ref().as_bytes()),
        0,
        local_time_raw.into(),
        false,
    );
    let members: Arc<RwLock<Vec<Node>>> = Arc::new(RwLock::new(Vec::from([initial_node])));

    let receiver_members = members.clone();
    let cmd_members = members.clone();
    let leader_members = members.clone();
    let receiver_sender_id = sender_id.clone();
    let cmd_sender_id = sender_id.clone();
    let local_file_list: Arc<Mutex<LocalFileList>> = Arc::new(Mutex::new(LocalFileList::new()));
    let server_local_file_list = local_file_list.clone();
    let leader_ip = Arc::from(RwLock::from("172.22.158.225".to_string()));
    let cmd_leader_ip = leader_ip.clone();
    let receiver_leader_ip = leader_ip.clone();

    let (tx_leader, rx_leader) = mpsc::channel::<Vec<String>>(10);

    async {
        if vm_num != 5 {
            let member_list = MemberList {
                sender: sender_id.to_string(),
                machines: Vec::new(),
            }
            .encode_to_vec();
            if let Err(e) = sock
                .send_to(&member_list, "fa23-cs425-6805.cs.illinois.edu:12307")
                .await
            {
                error!("Unable to contact introducer: {}", e);
                return;
            }
            error!("Contacted introducer");
        } else {
            // Update: Made the introduce as machine 2
            //TODO: Would need to check this because 1 is both introducer and leader and
            //we would need to demo leader creashing and new leader being selected
            error!("Introducer booted up");
        }
        let (stop_tx, mut stop_rx) = tokio::sync::mpsc::channel::<()>(1);

        let notifier = Arc::new(Notify::new());
        let notified = notifier.clone();

        let recv = tokio::spawn(async move {
            receiver::receiver(
                receiver_members,
                receiver_sender_id,
                sock,
                receiver_leader_ip,
                tx_leader,
                notifier,
            )
            .await;
        });

        let send = tokio::spawn(async move {
            sender::sender(members, sender_id).await;
        });
        let cmd_listener = tokio::spawn(async move {
            command_listener(
                cmd_members,
                cmd_sender_id,
                stop_tx,
                local_file_list,
                cmd_leader_ip,
            )
            .await;
        });
        let server = tokio::spawn(async move {
            server::run_server(server_local_file_list).await;
        });
        let leader = tokio::spawn(async move {
            leader_runner(leader_ip, rx_leader, local_ip, leader_members, notified).await;
        });
        tokio::select! {
            _ = stop_rx.recv() => {
                info!("Stopping tasks");
            }
            _ = recv => {
                error!("Failure detector stopped. This should never happen.");
            }
            _ = send => {
                error!("Failure detector stopped. This should never happen.");
            }
            _ = cmd_listener => {
                error!("Command listener stopped. This should never happen.");
            }
            _ = server => {
                error!("SDFS server stopped. This should never happen.");
            }
            _ = leader => {
                error!("SDFS leader stopped. This should never happen");
            }
        }
    }
    .instrument(trace_span!("Node running"))
    .await;
    // Spawn the client and server for the machine.
}

async fn leader_runner(
    leader_ip: Arc<RwLock<String>>,
    rx_leader: mpsc::Receiver<Vec<String>>,
    local_ip: IpAddr,
    leader_mem: Arc<RwLock<Vec<Node>>>,
    leader_wakeup: Arc<Notify>,
) {
    let leader_ip = {
        let locked = leader_ip.read().await;
        locked.clone()
    };
    if leader_ip == format!("{local_ip}") {
        leader::run_leader(rx_leader, leader_mem, Duration::from_secs(15)).await;
    } else {
        leader_wakeup.notified().await;
        leader::run_leader(rx_leader, leader_mem, Duration::from_secs(2)).await;
    }
}

#[instrument(name = "Command listener loop", level = "trace")]
async fn command_listener(
    members: Arc<RwLock<Vec<Node>>>,
    sender_id: Arc<str>,
    stop_tx: tokio::sync::mpsc::Sender<()>,
    local_file_list: Arc<Mutex<LocalFileList>>,
    leader_ip: Arc<RwLock<String>>,
) {
    let client = client::Client::new(leader_ip.clone());
    while let Ok(Ok(input)) =
        tokio::task::spawn_blocking(|| Text::new("Enter command:").prompt()).await
    {
        let command: Vec<_> = input.split_whitespace().collect();
        match command.as_slice() {
            ["leave"] => {
                let _ = stop_tx.send(()).await;
                return;
            }
            ["list_mem"] => {
                let guard = members.read().await;
                println!("Membership List:");
                for node in guard.iter() {
                    println!("Node: {}", node); // Assuming the Node struct has a Display implementation
                }
            }
            ["list_self"] => {
                println!("Self's ID: {}", sender_id);
            }
            ["put", local_file_name, sdfs_file_name] => {
                client.put_file(local_file_name, sdfs_file_name).await;
            }
            ["get", sdfs_file_name, local_file_name] => {
                client.get_file(sdfs_file_name, local_file_name).await;
            }
            ["delete", sdfs_file_name] => {
                client.delete_file(sdfs_file_name).await;
            }
            ["ls", sdfs_file_name] => {
                client.list_file(sdfs_file_name).await;
            }
            ["store"] => {
                println!("{}", *local_file_list.lock().await);
            }
            ["multiread", sdfs_file_name, local_file_name, vms @ ..] => {
                client
                    .multi_read(sdfs_file_name, local_file_name, vms)
                    .await;
            }
            ["multiwrite", local_file_name, sdfs_file_name, vms @ ..] => {
                client
                    .multi_write(local_file_name, sdfs_file_name, vms)
                    .await;
            }
            ["rejoin", introducer] => {
                println!("Rejoining via introducer: {}", introducer);
            }
            _ => {
                println!("Invalid command!");
            }
        }
    }
}

fn get_vm_num() -> Result<i8> {
    let raw_machine_name = hostname::get()?
        .into_string()
        .map_err(|_| anyhow!("Machine host name is not valid UTF-8"))?;
    let machine_name = raw_machine_name
        .rsplit('-')
        .next()
        .ok_or(anyhow!("Invalid host name format"))?
        .split('.')
        .next()
        .ok_or(anyhow!("Invalid host name format"))?;
    //println!("{machine_name}");
    Ok(machine_name.split_at(3).1.parse::<i8>()?)
}
