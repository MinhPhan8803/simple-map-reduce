use crate::member_list::{failure_detection::Type, FailureDetection, Member, MemberList};
use crate::node::Node;
use prost::Message;
use rand::prelude::SliceRandom;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use tokio::{net::UdpSocket, sync::RwLock};
use tracing::{error, info, trace, trace_span, warn, Instrument};

pub async fn sender(members: Arc<RwLock<Vec<Node>>>, sender_id: Arc<str>) {
    let can_sock: Option<UdpSocket> = async {
        info!("Starting sender");
        let Ok(raw_host) = hostname::get() else {
            error!("Failed to read hostname");
            return None;
        };
        let Ok(host) = raw_host.into_string() else {
            error!("Failed to convert hostname");
            return None;
        };
        let Ok(new_sock) = UdpSocket::bind(format!("{host}:12306")).await else {
            error!("Failed to bind socket");
            return None;
        };
        Some(new_sock)
    }
    .instrument(trace_span!("Sender start up"))
    .await;
    let mut start_node = 0;
    let Some(sock) = can_sock else {
        return;
    };
    let span = trace_span!("Sender loop");
    async {
        loop {
            trace!("Entering loop");

            let guard = members.read().await;
            start_node = if !(*guard).is_empty() {
                start_node % (*guard).len()
            } else {
                0
            };

            // Regular membership list message
            let member_list = FailureDetection {
                r#type: Some(Type::Members(MemberList {
                    sender: sender_id.to_string(),
                    machines: (*guard)
                        .iter()
                        .filter_map(|node| {
                            (!node.fail()).then_some({
                                Member {
                                    id: String::from_utf8_lossy(&node.id()).into_owned(),
                                    heartbeat: node.heartbeat(),
                                    time: node.time().to_rfc3339().to_string(),
                                }
                            })
                        })
                        .collect::<Vec<_>>(),
                })),
            }
            .encode_to_vec();

            let length = (*guard).len();
            info!("Starting to send");
            if length > 0 {
                for member_idx in start_node..start_node + 3 {
                    let Some(elem) = (*guard).get(member_idx % length) else {
                        continue;
                    };
                    let raw_id = elem.id();
                    let Ok(id) = std::str::from_utf8(&raw_id) else {
                        continue;
                    };
                    let id_elems = id.split('_').collect::<Vec<_>>();
                    let ip = id_elems[0];
                    let port = id_elems[1];
                    info!("Starting to send to {}:{}", ip, port);

                    // Sending regular membership list message
                    if let Err(e) = sock.send_to(&member_list, format!("{ip}:{port}")).await {
                        warn!(
                            "Unable to send member list to {}:{} with error {}",
                            ip, port, e
                        );
                    } else {
                        info!("Sent member list to {}:{}", ip, port);
                    }
                }
            }
            if length <= 1 {
                let member_list = FailureDetection {
                    r#type: Some(Type::Members(MemberList {
                        sender: sender_id.to_string(),
                        machines: Vec::new(),
                    })),
                }
                .encode_to_vec();
                if let Err(e) = sock
                    .send_to(&member_list, "fa23-cs425-6805.cs.illinois.edu:12307")
                    .await
                {
                    error!("Unable to contact introducer: {}", e);
                    continue;
                }
                info!("Contacted introducer again");
            }
            start_node = if !(*guard).is_empty() {
                (start_node + 3) % (*guard).len()
            } else {
                0
            };
            drop(guard);
            {
                info!("Shuffling membership list");
                let mut guard = members.write().await;
                guard.shuffle(&mut rand::thread_rng());
            }
            sleep(Duration::from_millis(200)).await;
        }
    }
    .instrument(span)
    .await;
}
