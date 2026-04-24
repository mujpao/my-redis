use crate::frame::resp::RespValue;
use anyhow::anyhow;
use radix_trie::{Trie, TrieCommon, TrieKey};
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, instrument};

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
                    if let Some(last_entry_id) = &self.last_entry_id
                        && milliseconds_time == last_entry_id.milliseconds_time
                    {
                        last_entry_id.sequence_number + 1
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
            milliseconds_time,
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

        if let Some(last_entry_id) = &self.last_entry_id
            && *last_entry_id >= id
        {
            return Err(StreamError::NewIdLtePrevious);
        }
        self.data.insert(id.clone(), data);
        self.last_entry_id = Some(id.clone());
        Ok(id)
    }

    #[instrument(skip(self))]
    pub fn get_after_id(&self, last_id: Option<&str>) -> Result<Vec<RespValue>, StreamError> {
        let last_id = match last_id {
            Some(id) => Some(StreamId::from_range(id, false).map_err(|e| {
                info!(reason= ?e, "invalid range for stream id");
                StreamError::InvalidRange
            })?),
            None => None,
        };

        let mut values_in_range = vec![];

        for (id, data) in self.data.iter() {
            if let Some(ref last_id) = last_id
                && *id <= *last_id
            {
                continue;
            }
            let mut value_as_resp = vec![RespValue::BulkString(id.to_string())];

            let mut fields = vec![];
            for StreamData { field, value } in data {
                fields.push(RespValue::BulkString(field.into()));
                fields.push(RespValue::BulkString(value.into()));
            }

            value_as_resp.push(RespValue::Array(fields));
            values_in_range.push(RespValue::Array(value_as_resp));
        }

        Ok(values_in_range)
    }

    pub fn range(&self, start: &str, end: &str) -> Result<RespValue, StreamError> {
        let mut common_idx = 0;
        for i in 0..std::cmp::min(start.len(), end.len()) {
            if start[i..i + 1] != end[i..i + 1] {
                common_idx = i;
                break;
            } else {
                common_idx = i + 1;
            }
        }

        let common_prefix = if common_idx > 0 {
            Some(
                StreamId::from_range(&start[0..common_idx], false).map_err(|e| {
                    info!(reason= ?e, "invalid range for stream id");
                    StreamError::InvalidRange
                })?,
            )
        } else {
            None
        };

        let start = match start {
            "-" => StreamId {
                milliseconds_time: 0,
                sequence_number: 0,
            },
            start => StreamId::from_range(start, false).map_err(|e| {
                info!(reason= ?e, "invalid range for stream id");
                StreamError::InvalidRange
            })?,
        };

        let end = match end {
            "+" => StreamId {
                milliseconds_time: usize::MAX,
                sequence_number: usize::MAX,
            },
            end => StreamId::from_range(end, true).map_err(|e| {
                info!(reason= ?e, "invalid range for stream id");
                StreamError::InvalidRange
            })?,
        };

        let ancestor_iter = match common_prefix {
            Some(prefix) => self.data.get_raw_ancestor(&prefix).iter(),
            None => {
                // No common prefix, so we have to search the entire trie
                self.data.iter()
            }
        };

        let mut values_in_range = vec![];

        for (id, data) in ancestor_iter {
            if *id >= start && *id <= end {
                let mut value_as_resp = vec![RespValue::BulkString(id.to_string())];

                let mut fields = vec![];
                for StreamData { field, value } in data {
                    fields.push(RespValue::BulkString(field.into()));
                    fields.push(RespValue::BulkString(value.into()));
                }

                value_as_resp.push(RespValue::Array(fields));
                values_in_range.push(RespValue::Array(value_as_resp));
            }
        }

        Ok(RespValue::Array(values_in_range))
    }

    pub fn get_last_id(&self) -> Option<String> {
        self.last_entry_id.as_ref().map(|id| id.to_string())
    }
}

#[derive(Debug)]
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

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
pub struct StreamId {
    pub milliseconds_time: usize,
    pub sequence_number: usize,
}

impl StreamId {
    pub fn from_range(range: &str, end: bool) -> anyhow::Result<StreamId> {
        let split_s: Vec<_> = range.split("-").collect();
        if split_s.len() == 2 && !split_s[1].is_empty() {
            let milliseconds_time = split_s[0]
                .parse::<usize>()
                .map_err(|e| anyhow!("Unable to parse stream id ms time, {:?}", e))?;

            let sequence_number = split_s[1]
                .parse::<usize>()
                .map_err(|e| anyhow!("Unable to parse stream id seq no, {:?}", e))?;

            Ok(StreamId {
                milliseconds_time,
                sequence_number,
            })
        } else if split_s.len() == 2 && split_s[1].is_empty() || split_s.len() == 1 {
            let milliseconds_time = split_s[0]
                .parse::<usize>()
                .map_err(|e| anyhow!("Unable to parse stream range start id ms time, {:?}", e))?;
            let sequence_number = if end { usize::MAX } else { 0 };
            Ok(StreamId {
                milliseconds_time,
                sequence_number,
            })
        } else {
            Err(anyhow!("Unable to parse stream range start"))
        }
    }
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
    InvalidRange,
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
            Self::InvalidRange => {
                write!(f, "ERR The range is invalid")
            }
        }
    }
}
