use super::read_pbf;
use super::common;
use super::write_pbf;
use std::io::{Result,Error,ErrorKind};
use core::cmp::Ordering;
#[derive(Debug,Eq)]
pub struct Relation {
    pub common: common::Common,
    pub members: Vec<Member>
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

impl Relation {
    pub fn new(id: i64) -> Relation {
        Relation{common: common::Common::new(id,0), members: Vec::new()}
    }
    pub fn read(changetype: u64, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Relation> {
        
        let tgs = read_pbf::read_all_tags(&data,0);
        let cc = common::Common::read(changetype, &strings,&tgs, minimal)?;
        
        let mut res = Relation{common: cc, members: Vec::new()};
        
        let mut roles = Vec::new();
        let mut refs = Vec::new();
        let mut types = Vec::new();
        
        for t in read_pbf::IterTags::new(&data,0) {
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
                    res.members.push(Member{role: String::from(""), mem_type: make_elementtype(types[i]), mem_ref: refs[i]});
                } else {
                    let m = Member{
                        role: strings[roles[i] as usize].clone(),
                        mem_type: make_elementtype(types[i]),
                        mem_ref: refs[i],
                    };
                    res.members.push(m);
                }
            }
        }
        
        Ok(res)
    }
    pub fn pack(&self, pack_strings: &mut Box<common::PackStringTable>, include_qts: bool) -> Result<Vec<u8>> {
        
        
        let l = self.common.pack_length(pack_strings, include_qts)
            + self.members.len()*10 + 6;
        
        let mut res = Vec::with_capacity(l);
        self.common.pack_head(&mut res,pack_strings)?;
        
        if !self.members.is_empty() {
            let roles = write_pbf::pack_int(self.members.iter().map( |m| { pack_strings.call(&m.role) }));
            let refs = write_pbf::pack_delta_int(self.members.iter().map( |m| { m.mem_ref }));
            let types = write_pbf::pack_int(self.members.iter().map( |m| { elementtype_int(&m.mem_type) }));
            
            write_pbf::pack_data(&mut res, 8, &roles);
            write_pbf::pack_data(&mut res, 9, &refs);
            write_pbf::pack_data(&mut res, 10, &types);
        }
        self.common.pack_tail(&mut res, include_qts)?;
        Ok(res)
        
        //Err(Error::new(ErrorKind::Other, "not impl"))
    }
}
impl Ord for Relation {
    fn cmp(&self, other: &Self) -> Ordering {
        self.common.cmp(&other.common)
    }
}
impl PartialOrd for Relation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Relation {
    fn eq(&self, other: &Self) -> bool {
        self.common.eq(&other.common)
    }
}  
