use bytes::Bytes;
use chrono::{DateTime, FixedOffset};
use std::fmt;

#[derive(Debug, Clone)]
pub struct Node {
    id: Bytes,
    heartbeat_count: u32,
    time: DateTime<FixedOffset>,
    _fail: bool,
}

impl Node {
    pub fn new(id: Bytes, heartbeat_count: u32, time: DateTime<FixedOffset>, _fail: bool) -> Node {
        Node {
            id,
            heartbeat_count,
            time,
            _fail: false,
        }
    }
    pub fn id(&self) -> Bytes {
        self.id.clone()
    }
    pub fn heartbeat(&self) -> u32 {
        self.heartbeat_count
    }
    pub fn time(&self) -> DateTime<FixedOffset> {
        self.time
    }
    pub fn fail(&self) -> bool {
        self._fail
    }
    pub fn set_heartbeat(&mut self, heartbeat: u32) {
        self.heartbeat_count = heartbeat;
    }
    pub fn set_time(&mut self, time: DateTime<FixedOffset>) {
        self.time = time;
    }
    pub fn set_fail(&mut self, fail: bool) {
        self._fail = fail;
    }
}

impl fmt::Display for Node {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "Displaying info for node {}",
            String::from_utf8(self.id.to_vec()).unwrap()
        )?;
        writeln!(f, "Heartbeat: {}", self.heartbeat_count)?;
        writeln!(f, "Latest time: {}", self.time)?;
        writeln!(f, "Failure: {}", self._fail)
    }
}
