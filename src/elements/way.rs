mod osmquadtree {
    pub use super::super::super::*;
}


use osmquadtree::read_pbf;
use osmquadtree::write_pbf;

use super::common::{read_common,pack_head,pack_tail,pack_length,common_cmp,common_eq,Changetype,PackStringTable};
use super::info::Info;
use super::tags::Tag;
use super::quadtree::Quadtree;

use std::io::{Result,};
use core::cmp::Ordering;


#[derive(Debug,Eq)]
pub struct Way {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>, 
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,
    
    pub quadtree: Quadtree,
}

impl Way {
    pub fn new(id: i64, changetype: Changetype) -> Way {
        Way{id: id, changetype: changetype, info: None, tags: Vec::new(), refs: Vec::new(), quadtree: Quadtree::empty()}
    }
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Way> {
        let mut w = Way::new(0,changetype);
        
        let tgs = read_pbf::read_all_tags(&data,0);
        //let mut rem=Vec::new();
        //(w.id, w.info, w.tags, w.quadtree, rem) = read_common(&strings, &tgs, minimal)?;
        let mut zz = read_common(&strings, &tgs, minimal)?;
        w.id = zz.0; w.info = zz.1.take(); w.tags = std::mem::take(&mut zz.2); w.quadtree = zz.3;
        
        for t in zz.4 {
            match t {
                read_pbf::PbfTag::Data(8, d) => w.refs = read_pbf::read_delta_packed_int(&d),
                _ => {},
            }
        }
        Ok(w)
    }
    pub fn pack(&self, pack_strings: &mut Box<PackStringTable>, include_qts: bool) -> Result<Vec<u8>> {
        
        let refs = write_pbf::pack_delta_int_ref(self.refs.iter());
        
        let l = pack_length(&self.tags, pack_strings, include_qts) + write_pbf::data_length(8, refs.len());
        
        let mut res = Vec::with_capacity(l);
        pack_head(&self.id, &self.info, &self.tags, &mut res, pack_strings)?;
        write_pbf::pack_data(&mut res, 8, &refs);
        pack_tail(&self.quadtree, &mut res, include_qts)?;
        Ok(res)
        
        //Err(Error::new(ErrorKind::Other, "not impl"))
    }
}
impl Ord for Way {
    fn cmp(&self, other: &Self) -> Ordering {
        common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}
impl PartialOrd for Way {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype))
    }
}

impl PartialEq for Way {
    fn eq(&self, other: &Self) -> bool {
        common_eq(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}  
