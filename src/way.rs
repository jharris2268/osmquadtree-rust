use super::read_pbf;
use super::common::{Common,Changetype,PackStringTable};

use std::io::{Result,};
use core::cmp::Ordering;
use super::write_pbf;

#[derive(Debug,Eq)]
pub struct Way {
    pub common: Common,
    pub refs: Vec<i64>
}

impl Way {
    pub fn new(id: i64, changetype: Changetype) -> Way {
        Way{common: Common::new(id,changetype), refs: Vec::new()}
    }
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Way> {
        
        let tgs = read_pbf::read_all_tags(&data,0);
        let cc = Common::read(changetype, &strings, &tgs, minimal)?;
        
        let mut w = Way{common: cc, refs: Vec::new()};
        for t in read_pbf::IterTags::new(&data,0) {
            match t {
                read_pbf::PbfTag::Data(8, d) => w.refs = read_pbf::read_delta_packed_int(&d),
                _ => {},
            }
        }
        Ok(w)
    }
    pub fn pack(&self, pack_strings: &mut Box<PackStringTable>, include_qts: bool) -> Result<Vec<u8>> {
        
        let refs = write_pbf::pack_delta_int_ref(self.refs.iter());
        
        let l = self.common.pack_length(pack_strings, include_qts) + write_pbf::data_length(8, refs.len());
        
        let mut res = Vec::with_capacity(l);
        self.common.pack_head(&mut res, pack_strings)?;
        write_pbf::pack_data(&mut res, 8, &refs);
        self.common.pack_tail(&mut res, include_qts)?;
        Ok(res)
        
        //Err(Error::new(ErrorKind::Other, "not impl"))
    }
}
impl Ord for Way {
    fn cmp(&self, other: &Self) -> Ordering {
        self.common.cmp(&other.common)
    }
}
impl PartialOrd for Way {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Way {
    fn eq(&self, other: &Self) -> bool {
        self.common.eq(&other.common)
    }
}  
