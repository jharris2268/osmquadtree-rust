use super::read_pbf;
use std::io::{Error,ErrorKind,Result};
use super::common::{Common,Changetype,PackStringTable};
use core::cmp::Ordering;
#[derive(Debug,Eq)]
pub struct Node {
    pub common: Common,
    pub lon: i64,
    pub lat: i64,
}

impl Node {
    pub fn new(id: i64, changetype: Changetype) -> Node {
        Node{common: Common::new(id, changetype), lon: 0, lat: 0}
    }
    
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Node> {
        
        let tt = read_pbf::read_all_tags(&data,0);
        let cc = Common::read(changetype, &strings, &tt, minimal)?;
        let mut res = Node{common: cc, lon: 0, lat: 0};
        for t in read_pbf::IterTags::new(&data,0) {
            match t {
                read_pbf::PbfTag::Value(8,lat) => res.lat = read_pbf::unzigzag(lat),
                read_pbf::PbfTag::Value(9,lon) => res.lon = read_pbf::unzigzag(lon),
                _ => {},
            }
        }
        Ok(res)
    }
    pub fn pack(&self, _prep_strings: &mut Box<PackStringTable>, _include_qts: bool) -> Result<Vec<u8>> {
        Err(Error::new(ErrorKind::Other, "not impl"))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        self.common.cmp(&other.common)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.common.eq(&other.common)
    }
}  
