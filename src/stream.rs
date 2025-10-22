use anyhow::anyhow;
use std::str::FromStr;

use radix_trie::{Trie, TrieKey};

pub struct Stream {
    data: Trie<StreamId, Vec<StreamData>>,
    last_entry_id: Option<StreamId>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            data: Trie::new(),
            last_entry_id: None,
        }
    }

    pub fn append(&mut self, id: StreamId, data: Vec<StreamData>) -> Result<StreamId, StreamError> {
        if id
            == (StreamId {
                milliseconds_time: 0,
                sequence_number: 0,
            })
        {
            return Err(StreamError::IdEqualsZero);
        }

        if let Some(last_entry_id) = &self.last_entry_id {
            if *last_entry_id >= id {
                return Err(StreamError::NewIdLtePrevious);
            }
        }
        self.data.insert(id.clone(), data);
        self.last_entry_id = Some(id.clone());
        Ok(id)
    }
}

pub struct StreamData {
    pub field: String,
    pub value: String,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct StreamId {
    pub milliseconds_time: usize,
    pub sequence_number: usize,
}

impl std::fmt::Display for StreamId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.milliseconds_time, self.sequence_number)
    }
}

impl FromStr for StreamId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let split_s: Vec<_> = s.split("-").collect();
        if split_s.len() != 2 {
            return Err(anyhow!("Unable to parse stream id"));
        }

        let milliseconds_time: usize = split_s[0]
            .parse()
            .map_err(|e| anyhow!("Unable to parse stream id, {:?}", e))?;
        let sequence_number: usize = split_s[1]
            .parse()
            .map_err(|e| anyhow!("Unable to parse stream id, {:?}", e))?;
        Ok(Self {
            milliseconds_time,
            sequence_number,
        })
    }
}

impl TrieKey for StreamId {
    fn encode_bytes(&self) -> Vec<u8> {
        let v = vec![self.milliseconds_time, self.sequence_number];
        v.encode_bytes()
    }
}

#[derive(Debug)]
pub enum StreamError {
    NewIdLtePrevious,
    IdEqualsZero,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewIdLtePrevious => write!(
                f,
                "The ID specified in XADD is equal or smaller than the target stream top item"
            ),
            Self::IdEqualsZero => write!(f, "The ID specified in XADD must be greater than 0-0"),
        }
    }
}
