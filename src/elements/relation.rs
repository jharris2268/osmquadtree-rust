mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::read_pbf;
use osmquadtree::write_pbf;

use super::common::{read_common,pack_head,pack_tail,pack_length,common_cmp,common_eq,Changetype,PackStringTable};
use super::info::Info;
use super::tags::Tag;
use super::quadtree::Quadtree;

use std::io::{Result,Error,ErrorKind};
use core::cmp::Ordering;

#[derive(Debug,Eq)]
pub struct Relation {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>, 
    pub tags: Vec<Tag>,
    pub members: Vec<Member>,
    pub quadtree: Quadtree,
}

#[derive(Debug,Eq,PartialEq,Clone,Ord,PartialOrd)]
pub enum ElementType {
    Node,
    Way,
    Relation
}

fn make_elementtype(t: u64) -> ElementType {
    if t==0 { return ElementType::Node; }
    if t==1 { return ElementType::Way; }
    if t==2 { return ElementType::Relation; }
    panic!("wrong type");
}

fn elementtype_int(t: &ElementType) -> u64 {
    match t {
        ElementType::Node => 0,
        ElementType::Way => 1,
        ElementType::Relation => 2,
    }
}

#[derive(Debug,Eq,PartialEq)]
pub struct Member {
    role: String, 
    mem_type: ElementType,
    mem_ref: i64,
}
impl Member {
    pub fn new(role: String, mem_type: ElementType, mem_ref: i64) -> Member {
        Member{role,mem_type,mem_ref}
    }
}
impl Relation {
    pub fn new(id: i64, changetype: Changetype) -> Relation {
        Relation{id: id, changetype: changetype, info: None, tags: Vec::new(), members: Vec::new(), quadtree: Quadtree::empty()}
    }
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Relation> {
        
        let mut rel = Relation::new(0,changetype);
        
        let tgs = read_pbf::read_all_tags(&data,0);
        //let mut rem=Vec::new();
        //(rel.id, rel.info, rel.tags, rel.quadtree, rem) = read_common(&strings,&tgs, minimal)?;
        let mut zz = read_common(&strings, &tgs, minimal)?;
        rel.id = zz.0; rel.info = zz.1.take(); rel.tags = std::mem::take(&mut zz.2); rel.quadtree = zz.3;
        
        let mut roles = Vec::new();
        let mut refs = Vec::new();
        let mut types = Vec::new();
        
        for t in zz.4 {
            match t {
                read_pbf::PbfTag::Data(8, d) => {
                    if !minimal {
                        roles = read_pbf::read_packed_int(&d)
                    }
                },
                    
                read_pbf::PbfTag::Data(9, d) => refs = read_pbf::read_delta_packed_int(&d),
                read_pbf::PbfTag::Data(10, d) => types = read_pbf::read_packed_int(&d),
                _ => {},
            }
        }
        if types.len()!= refs.len() || (!minimal && types.len() != roles.len()) {
            return Err(Error::new(ErrorKind::Other,"member lens don't match"));
        }
        if types.len()!=0 {
            for i in 0..types.len() {
                if minimal {
                    rel.members.push(Member{role: String::from(""), mem_type: make_elementtype(types[i]), mem_ref: refs[i]});
                } else {
                    let m = Member{
                        role: strings[roles[i] as usize].clone(),
                        mem_type: make_elementtype(types[i]),
                        mem_ref: refs[i],
                    };
                    rel.members.push(m);
                }
            }
        }
        
        Ok(rel)
    }
    pub fn pack(&self, pack_strings: &mut Box<PackStringTable>, include_qts: bool) -> Result<Vec<u8>> {
        
        
        let l = pack_length(&self.tags, pack_strings, include_qts)
            + self.members.len()*10 + 6;
        
        let mut res = Vec::with_capacity(l);
        pack_head(&self.id, &self.info, &self.tags, &mut res,pack_strings)?;
        
        if !self.members.is_empty() {
            let roles = write_pbf::pack_int(self.members.iter().map( |m| { pack_strings.call(&m.role) }));
            let refs = write_pbf::pack_delta_int(self.members.iter().map( |m| { m.mem_ref }));
            let types = write_pbf::pack_int(self.members.iter().map( |m| { elementtype_int(&m.mem_type) }));
            
            write_pbf::pack_data(&mut res, 8, &roles);
            write_pbf::pack_data(&mut res, 9, &refs);
            write_pbf::pack_data(&mut res, 10, &types);
        }
        pack_tail(&self.quadtree, &mut res, include_qts)?;
        Ok(res)
        
        //Err(Error::new(ErrorKind::Other, "not impl"))
    }
}
impl Ord for Relation {
    fn cmp(&self, other: &Self) -> Ordering {
        common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}
impl PartialOrd for Relation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(common_cmp(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype))
    }
}

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        common_eq(&self.id,&self.info,&self.changetype, &other.id,&other.info,&other.changetype)
    }
}  
