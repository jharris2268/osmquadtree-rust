use super::read_pbf;
use super::common;  
use super::write_pbf;
use std::io::{Error,Result,ErrorKind};

#[derive(Debug,Eq,PartialEq)]
pub struct Info {
    pub version: i64,
    pub changeset: i64,
    pub timestamp: i64,
    pub user_id: i64,
    pub user: String,
}

impl Info {
    pub fn new() -> Info {
        Info{version: 0, changeset: 0, timestamp: 0, user_id: 0, user: String::from("")}
    }
    
    pub fn read(strings: &Vec<String>, data: &[u8]) -> Result<Info> {
        
        let mut res = Info::new();
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Value(1, v) => res.version = v as i64,
                read_pbf::PbfTag::Value(2, v) => res.timestamp = v as i64,
                read_pbf::PbfTag::Value(3, v) => res.changeset = v as i64,
                read_pbf::PbfTag::Value(4, v) => res.user_id = v as i64,
                read_pbf::PbfTag::Value(5, v) => {
                    if v as usize >=strings.len() { return Err(Error::new(ErrorKind::Other,"info user idx out of range")); }
                    res.user = strings[v as usize].clone();
                },
                _ => return Err(Error::new(ErrorKind::Other,format!("unexpected {:?} for info", x))),
            }
        }
        
        Ok(res)
    }
    
    pub fn pack_length(&self, _pack_strings: &mut Box<common::PackStringTable>) -> usize {
        50
        /*
        let mut l=0;
        l += write_pbf::value_length(1, self.version as u64);
        l += write_pbf::value_length(2, self.timestamp as u64);
        l += write_pbf::value_length(3, self.changeset as u64);
        l += write_pbf::value_length(4, self.user_id as u64);
        l += write_pbf::value_length(5, 250);//pack_strings.call(&self.user));
        
        l*/
    }
    pub fn pack(&self, pack_strings: &mut Box<common::PackStringTable>) -> Result<Vec<u8>> {
        let mut res = Vec::with_capacity(self.pack_length(pack_strings));
        write_pbf::pack_value(&mut res, 1, self.version as u64);
        write_pbf::pack_value(&mut res, 2, self.timestamp as u64);
        write_pbf::pack_value(&mut res, 3, self.changeset as u64);
        write_pbf::pack_value(&mut res, 4, self.user_id as u64);
        write_pbf::pack_value(&mut res, 5, pack_strings.call(&self.user));
        Ok(res)
    }
        
}
