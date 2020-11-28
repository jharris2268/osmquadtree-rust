mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::read_pbf;

use super::common::{read_common,common_cmp,common_eq,Changetype,PackStringTable,SetCommon};
use super::info::Info;
use super::tags::Tag;
use super::quadtree::Quadtree;


use std::io::{Error,ErrorKind,Result};
use core::cmp::Ordering;
#[derive(Debug,Eq,Clone)]
pub struct Node {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>, 
    pub tags: Vec<Tag>,
    
    pub lon: i32,
    pub lat: i32,
    
    pub quadtree: Quadtree,
}

impl Node {
    pub fn new(id: i64, changetype: Changetype) -> Node {
        Node{id:id, changetype:changetype, info: None, tags: Vec::new(), lon: 0, lat: 0, quadtree: Quadtree::empty()}
    }
    
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Node> {
        let mut nd = Node::new(0,changetype);
                
        let tgs = read_pbf::read_all_tags(&data,0);
        //let mut rem=Vec::new();
        //(nd.id, nd.info, nd.tags, nd.quadtree, rem) = read_common(&strings, &tt, minimal)?;
        
        //for t in rem {
        let rem = read_common(&mut nd, &strings, &tgs, minimal)?;
        
        
        for t in rem {
            match t {
                read_pbf::PbfTag::Value(8,lat) => nd.lat = read_pbf::un_zig_zag(*lat) as i32,
                read_pbf::PbfTag::Value(9,lon) => nd.lon = read_pbf::un_zig_zag(*lon) as i32,
                _ => {},
            }
        }
        Ok(nd)
    }
    pub fn pack(&self, _prep_strings: &mut Box<PackStringTable>, _include_qts: bool) -> Result<Vec<u8>> {
        Err(Error::new(ErrorKind::Other, "not impl"))
    }
}
impl SetCommon for Node {
    fn set_id(&mut self, id: i64) { self.id=id; }
    fn set_info(&mut self, info: Info) { self.info=Some(info); }
    fn set_tags(&mut self, tags: Vec<Tag>) { self.tags=tags; }
    fn set_quadtree(&mut self, quadtree: Quadtree) { self.quadtree=quadtree; }
}

impl Ord for Node {
    fn cmp(&self, other: &Self) -> Ordering {
        common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype))
    }
}

impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        common_eq(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}  
