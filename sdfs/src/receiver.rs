use crate::member_list::{
    failure_detection::Type, Coordinator, Election, FailureDetection, MemberList, Ok,
};
use crate::node::Node;
use bytes::Bytes;
use chrono::{offset::Local, DateTime, FixedOffset};
use futures::stream::{self, StreamExt};
use prost::Message;
use std::ops::Deref;
use std::{io, net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tokio::time::sleep;
use tracing::{info, instrument, trace, warn};

const TF_FAIL: Duration = Duration::from_millis(2200);
const TF_CLEAN: Duration = Duration::from_millis(5000);

fn split_id_to_components<T: Deref<Target = [u8]>>(raw_id: &T) -> Option<(&str, &str)> {
    let Ok(id) = std::str::from_utf8(raw_id) else {
        return None;
    };
    let id_elems = id.split('_').collect::<Vec<_>>();
    let ip = id_elems[0];
    let port = id_elems[1];
    Some((ip, port))
}

struct LeaderElection {
    leader_ip: Arc<RwLock<String>>,
    members: Arc<RwLock<Vec<Node>>>,
    election_ongoing: Arc<Mutex<bool>>,
    socket_addr: SocketAddr,
    coord_rx: Mutex<mpsc::Receiver<String>>,
    sender_id: Arc<str>,
    leader_wakeup: Arc<Notify>,
}

impl LeaderElection {
    fn new(
        leader_ip: Arc<RwLock<String>>,
        members: Arc<RwLock<Vec<Node>>>,
        mut socket_addr: SocketAddr,
        election_ongoing: Arc<Mutex<bool>>,
        coord_rx: mpsc::Receiver<String>,
        sender_id: Arc<str>,
        leader_wakeup: Arc<Notify>,
    ) -> LeaderElection {
        socket_addr.set_port(12308);
        let coord_rx = Mutex::new(coord_rx);
        LeaderElection {
            leader_ip,
            members,
            election_ongoing,
            socket_addr,
            coord_rx,
            sender_id,
            leader_wakeup,
        }
    }
    async fn run_election(self: Arc<Self>) {
        let mut elect_lock = self.election_ongoing.lock().await;
        if *elect_lock {
            println!("Election already ongoing");
            return;
        }
        *elect_lock = true;
        drop(elect_lock);

        loop {
            let members_locked = self.members.read().await;
            let mut lower_nodes: Vec<_> = members_locked
                .iter()
                .filter(|node| node.id().as_ref() < self.sender_id.as_bytes())
                .collect();
            let higher_nodes: Vec<_> = members_locked
                .iter()
                .filter(|node| node.id().as_ref() > self.sender_id.as_bytes())
                .collect();

            println!("Lower nodes: {:?}", lower_nodes);
            println!("Higher nodes: {:?}", higher_nodes);

            let mut base = 12309;
            let socket_addresses = stream::repeat(self.socket_addr);
            let ports = stream::repeat_with(|| {
                let tmp = base;
                base += 1;
                tmp
            });

            println!("Contacting {} lower nodes", lower_nodes.len());
            let contact_res: Vec<()> = stream::iter(lower_nodes.iter())
                .zip(socket_addresses.zip(ports))
                .filter_map(|(node, (mut sock_addr, port))| async move {
                    sock_addr.set_port(port);
                    let Ok(local_sock) = UdpSocket::bind(sock_addr).await else {
                        return None;
                    };
                    let raw_id = node.id();
                    let Some((ip, port)) = split_id_to_components(&raw_id) else {
                        return None;
                    };
                    let msg = FailureDetection {
                        r#type: Some(Type::Elect(Election {})),
                    }
                    .encode_to_vec();

                    if let Err(e) = local_sock.send_to(&msg, format!("{ip}:{port}")).await {
                        println!("Unable to contact higher node: {}", e);
                        return None;
                    }
                    let mut ok_buffer = [0; 1024];
                    let Ok(size) = tokio::select! {
                        res = local_sock.recv(&mut ok_buffer) => res,
                        _ = sleep(Duration::from_secs(10)) => {
                            println!("Timed out, no OK from lower node: {}", ip);
                            Err(io::Error::new(io::ErrorKind::TimedOut, "Connection timed out"))
                        }
                    } else {
                        return None;
                    };
                    if let Err(e) = Ok::decode(&ok_buffer[..size]) {
                        println!("Unable to decode OK message: {}", e);
                        return None;
                    }
                    Some(())
                })
                .collect()
                .await;

            if contact_res.is_empty() {
                println!("No lower node, electing myself");
                let raw_id = self.sender_id.as_bytes();
                let Some((ip, _)) = split_id_to_components(&raw_id) else {
                    continue;
                };
                *self.leader_ip.write().await = ip.to_string();
                self.leader_wakeup.notify_one();
            } else {
                println!("Receiving COORDs from {} lower nodes", contact_res.len());
                let mut received_ips = Vec::new();
                let mut bruh = self.coord_rx.lock().await;
                while let Some(ip) = tokio::select! {
                    ip = bruh.recv() => {
                        ip
                    }
                    _ = sleep(Duration::from_secs(25)) => {
                        println!("Timed out, no COORD from a lower node");
                        None
                    }
                } {
                    received_ips.push(ip);
                }

                if received_ips.is_empty() {
                    continue;
                }

                lower_nodes.retain(|node| {
                    for ip in received_ips.iter() {
                        if node.id().starts_with(ip.as_bytes()) {
                            return true;
                        }
                    }
                    false
                });

                lower_nodes.sort_unstable_by_key(|node| node.id());

                let _ = stream::iter(lower_nodes)
                    .take_while(|node| async {
                        let raw_id = node.id();
                        let Some((ip, _)) = split_id_to_components(&raw_id) else {
                            return true;
                        };
                        println!("Elected {} as new leader", ip);
                        *self.leader_ip.write().await = ip.to_string();
                        false
                    })
                    .collect::<Vec<_>>()
                    .await;
            }

            let Ok(coord_sock) = UdpSocket::bind(self.socket_addr).await else {
                println!("Unable to bind 12308");
                return;
            };

            println!(
                "Sending Coord messages to {} higher nodes",
                higher_nodes.len()
            );
            for node in higher_nodes {
                let raw_id = node.id();
                let Some((ip, port)) = split_id_to_components(&raw_id) else {
                    continue;
                };

                let msg = FailureDetection {
                    r#type: Some(Type::Coord(Coordinator {
                        leader_ip: self.leader_ip.read().await.clone(),
                    })),
                }
                .encode_to_vec();

                if let Err(e) = coord_sock.send_to(&msg, format!("{ip}:{port}")).await {
                    println!("Unable to contact higher node: {}", e);
                    return;
                }
            }

            println!("Sent COORD to higher nodes, election should be complete");

            break;
        }
        *self.election_ongoing.lock().await = false;
    }
}

#[instrument(name = "Receiver loop", level = "trace")]
pub async fn receiver(
    members: Arc<RwLock<Vec<Node>>>,
    sender_id: Arc<str>,
    udp_socket: UdpSocket,
    leader_ip: Arc<RwLock<String>>,
    tx_leader: mpsc::Sender<Vec<String>>,
    leader_wakeup: Arc<Notify>,
) {
    let mut buffer = [0; 2048];
    let proc_list_ip = leader_ip.clone();
    let proc_list_tx_leader = tx_leader.clone();
    let proc_list_members = members.clone();
    let Ok(local_addr) = udp_socket.local_addr() else {
        println!("Malformed local addr, unable to start failure listener");
        return;
    };
    let election_ongoing = Arc::new(Mutex::new(false));
    let (coord_tx, coord_rx) = mpsc::channel::<String>(10);
    let leader_election = Arc::new(LeaderElection::new(
        leader_ip.clone(),
        members.clone(),
        local_addr,
        election_ongoing.clone(),
        coord_rx,
        sender_id.clone(),
        leader_wakeup,
    ));
    let leader_election_proc = leader_election.clone();
    tokio::spawn(async move {
        process_list(
            proc_list_members,
            proc_list_ip,
            proc_list_tx_leader,
            leader_election_proc,
        )
        .await;
    });
    loop {
        let Ok((size, remote_addr)) = udp_socket.recv_from(&mut buffer).await else {
            warn!("Failed to read data from UDP socket");
            continue;
        };
        let received_data = &buffer[..size];

        info!("Received message from {}", remote_addr);
        // Checking if received membership list
        let received_message = match FailureDetection::decode(received_data) {
            Ok(res) => res,
            Err(e) => {
                warn!("Failed to decode membership list {}", e);
                continue;
            }
        };
        match received_message.r#type {
            Some(Type::Members(received_member_list)) => {
                info!("Received member list");
                process_received_data(&members, &received_member_list, &sender_id).await;
            }
            Some(Type::Coord(new_leader)) => {
                if !*election_ongoing.lock().await {
                    println!("Received Coordinator message, updating leader");
                    *leader_ip.write().await = new_leader.leader_ip;
                } else {
                    println!(
                        "Received Coordinator message, election running, informing the elector"
                    );
                    let _ = coord_tx.send(new_leader.leader_ip).await;
                }
            }
            Some(Type::Elect(_)) => {
                let ok_message = Ok {}.encode_to_vec();
                let _ = udp_socket.send_to(&ok_message, remote_addr).await;
                let election_clone = leader_election.clone();
                println!("Sent back OK message, running election");
                tokio::spawn(async move {
                    election_clone.clone().run_election().await;
                });
            }
            None => {
                println!("No message detected");
            }
        }
        buffer.fill(0);
    }
}

#[instrument(name = "Process membership list", level = "trace")]
async fn process_received_data(
    members: &Arc<RwLock<Vec<Node>>>,
    received_member_list: &MemberList,
    local_sender_id: &Arc<str>,
) {
    let local_time: DateTime<FixedOffset> = Local::now().into();
    let mut guard = members.write().await;

    let remote_sender = received_member_list.sender.as_str();
    let remote_node_id = Bytes::copy_from_slice(remote_sender.as_bytes());

    match guard.iter_mut().find(|node| node.id() == remote_node_id) {
        None => {
            info!("Adding new node: {}", remote_sender);
            guard.push(Node::new(remote_node_id, 1, local_time, false));
        }
        Some(i) => {
            info!("Received heartbeat from: {}, updating", remote_sender);
            i.set_heartbeat(i.heartbeat() + 1);
            i.set_time(Local::now().into());
            i.set_fail(false);
        }
    };

    for received_member in &received_member_list.machines {
        let received_id_bytes = Bytes::copy_from_slice(received_member.id.as_bytes());

        // Check if member exists in our local state
        if let Some(local_node) = guard.iter_mut().find(|node| node.id() == received_id_bytes) {
            let local_time: DateTime<FixedOffset> = Local::now().into();
            // let remote_time = DateTime::parse_from_rfc3339(&received_member.time).unwrap_or_default();
            if received_member.heartbeat > local_node.heartbeat() {
                info!("Updating node: {}", received_member.id);
                local_node.set_heartbeat(received_member.heartbeat);
                local_node.set_fail(false);
                local_node.set_time(local_time);
            }
        } else if **local_sender_id != received_member.id {
            let remote_time =
                DateTime::parse_from_rfc3339(&received_member.time).unwrap_or_default();
            if remote_time + TF_CLEAN > local_time {
                info!("Adding new node: {}", received_member.id);
                guard.push(Node::new(
                    received_id_bytes,
                    received_member.heartbeat,
                    remote_time,
                    false,
                ));
            }
        }
    }
}

async fn process_list(
    members: Arc<RwLock<Vec<Node>>>,
    leader_ip: Arc<RwLock<String>>,
    tx_leader: mpsc::Sender<Vec<String>>,
    leader_election: Arc<LeaderElection>,
) {
    loop {
        let local_time: DateTime<FixedOffset> = Local::now().into();

        let mut fail_ids = Vec::new();
        let mut clean_ids = Vec::new();

        {
            let read_guard = members.read().await;
            for node in read_guard.iter() {
                if local_time >= node.time() + TF_FAIL {
                    fail_ids.push(node.id().clone());
                }
                if local_time >= node.time() + TF_CLEAN {
                    clean_ids.push(node.id().clone());
                }
            }
        }

        if !fail_ids.is_empty() {
            trace!("Processing own membership list for suspicion");
            let mut guard = members.write().await;
            // Process own membership list for suspicion
            guard.iter_mut().for_each(|node: &mut Node| {
                //let duration_since_last_heartbeat = current_time.duration_since(node.time());
                if fail_ids.contains(&node.id()) && !node.fail() {
                    warn!(
                        "Failing node due to timeout: {}",
                        String::from_utf8_lossy(&node.id())
                    );
                    //println!("Node suspicious: {}", String::from_utf8_lossy(&node.id()));
                    node.set_fail(true);
                }
            });
        }

        let mut should_run_election = false;

        if !clean_ids.is_empty() {
            // For normal gossip, just cleanup
            let mut guard = members.write().await;
            let mut failed_machines = Vec::new();
            let mut new_guard = Vec::new();

            for node in guard.iter() {
                if clean_ids.contains(&node.id()) {
                    let leader_guard = leader_ip.read().await;
                    let raw_id = node.id();
                    let Ok(node_id) = std::str::from_utf8(&raw_id) else {
                        continue;
                    };
                    if let Some((left, _)) = node_id.split_once('_') {
                        failed_machines.push(left.to_string());
                    }
                    warn!("Old time {}, new time {}", node.time(), local_time);
                    warn!("Removing node due to cleanup: {}", node_id);
                    //println!("Removing node due to cleanup: {}", node_id);
                    if node.id().starts_with(leader_guard.as_bytes()) {
                        should_run_election = true;
                    }
                } else {
                    new_guard.push(node.clone());
                }
            }
            *guard = new_guard;
            let _ = tx_leader.send(failed_machines).await;
        }
        // if should_run_election {
        //     println!("Detected failure on leader, calling election");
        //     let election_clone = leader_election.clone();
        //     tokio::spawn(async move {
        //         election_clone.clone().run_election().await;
        //     });
        // }
        sleep(Duration::from_millis(100)).await;
    }
}
