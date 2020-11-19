use super::read_pbf;
use super::info; use super::tags; use super::quadtree;

use super::write_pbf;

use core::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{Error,ErrorKind,Result};

#[derive(Debug,Eq)]
pub struct Common {
    pub id: i64,
    pub changetype: u64,
    pub info: info::Info, 
    pub tags: Vec<tags::Tag>,
    pub quadtree: quadtree::Quadtree,
}

impl Common {
    pub fn new(id: i64, ct: u64) -> Common {
        Common{id:id, changetype: ct, info: info::Info::new(), tags: Vec::new(), quadtree: quadtree::Quadtree::new(-1)}
    }

    pub fn read(changetype: u64, strings: &Vec<String>, pbftags: &Vec<read_pbf::PbfTag>, minimal: bool) -> Result<Common> {

        let mut res = Common::new(0,changetype);
        
        let mut kk = Vec::new();
        let mut vv = Vec::new();
        
        for t in pbftags {
            
            match t {
                read_pbf::PbfTag::Value(1, i) => res.id = *i as i64,
                read_pbf::PbfTag::Data(4, d) => {
                    if !minimal {
                        res.info = info::Info::read(strings, d)?
                    }
                },
                    
                read_pbf::PbfTag::Data(2, d) => {
                    if !minimal {
                        if !kk.is_empty() { return Err(Error::new(ErrorKind::Other,"more than one keys??")); } 
                        kk = read_pbf::read_packed_int(d);
                        
                        /*if res.tags.len()!=0 { return Err(Error::new(ErrorKind::Other,"more than one keys??")); }
                        let kk = read_pbf::read_packed_int(&d);
                        for k in &kk {
                            if *k as usize >=strings.len() {
                                return Err(Error::new(ErrorKind::Other,format!("tag key out of range {:?}", &kk)));
                            }
                            res.tags.push(tags::Tag::new(strings[*k as usize].clone(), String::from("")));
                        }*/
                    }
                },
                read_pbf::PbfTag::Data(3, d) => {
                    if !minimal {
                        if !vv.is_empty() { return Err(Error::new(ErrorKind::Other,"more than one keys??")); }
                        vv = read_pbf::read_packed_int(d);
                        /*
                        if res.tags.len()==0 {
                            if !(d.len()==1 && d[0]==0u8) {
                                return Err(Error::new(ErrorKind::Other,"vals without keys??"));
                            }
                        }
                        
                        let vv = read_pbf::read_packed_int(&d);
                        if vv.len() != res.tags.len() {
                            return Err(Error::new(ErrorKind::Other,"tags keys and vals don't match"))
                        }
                        for i in 0..(vv.len() as usize) {
                            if vv[i] as usize >=strings.len() {
                                return Err(Error::new(ErrorKind::Other,format!("tag val out of range {:?}", vv)));
                            }
                            res.tags[i].val = strings[vv[i] as usize].clone();
                        }*/
                    }
                },
                read_pbf::PbfTag::Value(20, q) => res.quadtree=quadtree::Quadtree::new(read_pbf::unzigzag(*q)),
                _ => {},
            }
        }
        if kk.len() != vv.len() {
            return Err(Error::new(ErrorKind::Other, format!("tags don't match: [id={}] {} // {}", res.id, kk.len(), vv.len())));
        }
        if kk.len()>0 {
            res.tags.reserve(kk.len());
            for i in 0..kk.len() {
                res.tags.push(tags::Tag::new(strings[kk[i] as usize].clone(), strings[vv[i] as usize].clone()));
            }
            
            //res.tags.extend( kk.iter().zip(vv).map( |(k,v)| { tags::Tag::new(strings[*k as usize].clone(), strings[v as usize].clone()) }) );
        
        }
        Ok(res)
    }
    
    pub fn pack_length(&self, _pack_strings: &mut Box<PackStringTable>, _include_qts: bool) -> usize {
        
        70 + 10*self.tags.len()
        /*
        let mut l = 0;
        l += write_pbf::value_length(1, self.id as u64);
        
        //l += write_pbf::data_length(2, write_pbf::packed_int_length(self.tags.iter().map( |t| { pack_strings.call(&t.key) } )));
        //l += write_pbf::data_length(3, write_pbf::packed_int_length(self.tags.iter().map( |t| { pack_strings.call(&t.val) } )));
        l += write_pbf::data_length(2, 5*self.tags.len());
        l += write_pbf::data_length(3, 5*self.tags.len());
        
        l += write_pbf::data_length(4, self.info.pack_length(pack_strings));
        if include_qts {
            l += write_pbf::value_length(20, write_pbf::zig_zag(self.quadtree.as_int()));
        }
        l*/
    }
    
    pub fn pack_head(&self, res: &mut Vec<u8>,  pack_strings: &mut Box<PackStringTable>) -> Result<()> {
        write_pbf::pack_value(res, 1, self.id as u64);
        if !self.tags.is_empty() {
            write_pbf::pack_data(res, 2, &write_pbf::pack_int(self.tags.iter().map( |t| { pack_strings.call(&t.key) } )));
            write_pbf::pack_data(res, 3, &write_pbf::pack_int(self.tags.iter().map( |t| { pack_strings.call(&t.val) } )));
        }
        write_pbf::pack_data(res, 4, &self.info.pack(pack_strings)?);
        Ok(())
    }
    pub fn pack_tail(&self, res:&mut Vec<u8>, include_qts: bool) -> Result<()> {
        if include_qts {
            write_pbf::pack_value(res, 20, write_pbf::zig_zag(self.quadtree.as_int()));
        }
        Ok(())
    }
}

impl Ord for Common {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.id.cmp(&other.id);
        if a!= Ordering::Equal { return a; }
        
        let b = self.info.version.cmp(&other.info.version);
        if b!=Ordering::Equal { return b; }
        
        self.changetype.cmp(&other.changetype)
    }
}

impl PartialOrd for Common {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Common {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.info.version==other.info.version && self.changetype==other.changetype
    }
}           
    
pub struct PackStringTable {
    strings: BTreeMap<String,u64>
}

impl PackStringTable {
    pub fn new() -> PackStringTable {
        let mut strings = BTreeMap::new();
        strings.insert(String::from("(*%£("),0);
        
        PackStringTable{strings: strings }
    }
    
    pub fn call(&mut self, s: &String) -> u64 {
        if !self.strings.contains_key(s) {
            let x = self.strings.len() as u64;
            self.strings.insert(s.clone(), x);
            x
        } else {
            *self.strings.get(s).unwrap()
        }
    }
    
    pub fn len(&self) -> usize {
        let mut l = write_pbf::data_length(1,0);
        for (s,t) in &self.strings {
            if *t!=0 {
                l += write_pbf::data_length(1, s.as_bytes().len());
            }
        }
        l
    }
    pub fn pack(&self) -> Vec<u8> {
        let mut m = vec![String::new();self.strings.len()];
        let mut tl = 0;
        
        for (s,t) in &self.strings {
            if *t==0 {
                m[0] = String::new()
            } else {
                m[*t as usize] = s.clone();
                tl += write_pbf::data_length(1,s.as_bytes().len());
            }
        }
        
        let mut r = Vec::with_capacity(tl);
        for t in m {
            write_pbf::pack_data(&mut r, 1, t.as_bytes());
        }
        r
    }
}


