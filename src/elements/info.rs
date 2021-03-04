use crate::elements::common;

use simple_protocolbuffers::{pack_value, IterTags, PbfTag};

use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Eq, PartialEq, Clone, serde::Serialize)]
pub struct Info {
    pub version: i64,
    pub changeset: i64,
    pub timestamp: i64,
    pub user_id: i64,
    pub user: String,
}

impl Info {
    pub fn new() -> Info {
        Info {
            version: 0,
            changeset: 0,
            timestamp: 0,
            user_id: 0,
            user: String::from(""),
        }
    }

    pub fn read(strings: &Vec<String>, data: &[u8]) -> Result<Info> {
        let mut res = Info::new();
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(1, v) => res.version = v as i64,
                PbfTag::Value(2, v) => res.timestamp = v as i64,
                PbfTag::Value(3, v) => res.changeset = v as i64,
                PbfTag::Value(4, v) => res.user_id = v as i64,
                PbfTag::Value(5, v) => {
                    if v as usize >= strings.len() {
                        return Err(Error::new(ErrorKind::Other, "info user idx out of range"));
                    }
                    res.user = strings[v as usize].clone();
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("unexpected {:?} for info", x),
                    ))
                }
            }
        }

        Ok(res)
    }

    pub fn pack_length(&self, _pack_strings: &mut Box<common::PackStringTable>) -> usize {
        50
        /*
        let mut l=0;
        l += value_length(1, self.version as u64);
        l += value_length(2, self.timestamp as u64);
        l += value_length(3, self.changeset as u64);
        l += value_length(4, self.user_id as u64);
        l += value_length(5, 250);//pack_strings.call(&self.user));

        l*/
    }
    pub fn pack(&self, pack_strings: &mut Box<common::PackStringTable>) -> Result<Vec<u8>> {
        let mut res = Vec::with_capacity(self.pack_length(pack_strings));
        pack_value(&mut res, 1, self.version as u64);
        pack_value(&mut res, 2, self.timestamp as u64);
        pack_value(&mut res, 3, self.changeset as u64);
        pack_value(&mut res, 4, self.user_id as u64);
        pack_value(&mut res, 5, pack_strings.call(&self.user));
        Ok(res)
    }
}
