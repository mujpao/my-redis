use anyhow::anyhow;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

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

    pub fn append(
        &mut self,
        id: StreamIdInput,
        data: Vec<StreamData>,
    ) -> Result<StreamId, StreamError> {
        let milliseconds_time = match id.milliseconds_time {
            Some(milliseconds_time) => milliseconds_time,
            None => match SystemTime::now().duration_since(UNIX_EPOCH) {
                Ok(n) => n.as_millis() as usize,
                Err(_) => {
                    return Err(StreamError::InvalidTimestamp);
                }
            },
        };

        let sequence_number = {
            match id.sequence_number {
                Some(sequence_number) => sequence_number,
                None => {
                    if let Some(last_entry_id) = &self.last_entry_id {
                        if milliseconds_time == last_entry_id.milliseconds_time {
                            last_entry_id.sequence_number + 1
                        } else {
                            match milliseconds_time {
                                0 => 1,
                                _ => 0,
                            }
                        }
                    } else {
                        match milliseconds_time {
                            0 => 1,
                            _ => 0,
                        }
                    }
                }
            }
        };

        let id = StreamId {
            milliseconds_time: milliseconds_time,
            sequence_number,
        };

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

pub struct StreamIdInput {
    pub milliseconds_time: Option<usize>,
    pub sequence_number: Option<usize>,
}

impl FromStr for StreamIdInput {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "*" {
            return Ok(Self {
                sequence_number: None,
                milliseconds_time: None,
            });
        }
        let split_s: Vec<_> = s.split("-").collect();
        if split_s.len() != 2 {
            return Err(anyhow!("Unable to split stream id"));
        }

        let milliseconds_time = Some(
            split_s[0]
                .parse::<usize>()
                .map_err(|e| anyhow!("Unable to parse stream id ms time, {:?}", e))?,
        );

        let sequence_number = match split_s[1] {
            "*" => None,
            num => Some(
                num.parse::<usize>()
                    .map_err(|e| anyhow!("Unable to parse stream id seq no, {:?}", e))?,
            ),
        };

        Ok(Self {
            milliseconds_time,
            sequence_number,
        })
    }
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
    InvalidTimestamp,
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NewIdLtePrevious => write!(
                f,
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            ),
            Self::IdEqualsZero => {
                write!(f, "ERR The ID specified in XADD must be greater than 0-0")
            }
            Self::InvalidTimestamp => {
                write!(f, "ERR The milliseconds timestamp is invalid")
            }
        }
    }
}
