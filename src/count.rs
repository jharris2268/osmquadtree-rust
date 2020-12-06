

use super::elements::{Changetype,PrimitiveBlock,MinimalBlock,Node,Way,Relation,MinimalNode,MinimalWay,MinimalRelation,get_changetype};
use super::update::ChangeBlock;
use super::utils::timestamp_string;
use super::read_pbf::read_delta_packed_int;

use std::fmt;
use std::collections::BTreeMap;
    
    


#[derive(Debug)]
pub struct NodeCount {
    num: i64,
    min_id: i64,
    max_id: i64,
    min_ts: i64,
    max_ts: i64,
    min_lon: i32,
    min_lat: i32,
    max_lon: i32,
    max_lat: i32,
}
impl NodeCount {
    pub fn new() -> NodeCount {
        NodeCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, min_lon:1800000000,min_lat:900000000,max_lon:-1800000000,max_lat:-900000000}
    }
    pub fn add(&mut self, nd: &Node) {
        self.num += 1;
        if self.min_id==-1 || nd.id < self.min_id { self.min_id = nd.id; }
        if self.max_id==-1 || nd.id > self.max_id { self.max_id = nd.id; }
        
        match &nd.info {
            Some(info) => {
                if self.min_ts==-1 || info.timestamp < self.min_ts { self.min_ts = info.timestamp; }
                if self.max_ts==-1 || info.timestamp > self.max_ts { self.max_ts = info.timestamp; }
            },
            None => {}
        }
        
        if nd.lon < self.min_lon { self.min_lon = nd.lon; }
        if nd.lon > self.max_lon { self.max_lon = nd.lon; }
        if nd.lat < self.min_lat { self.min_lat = nd.lat; }
        if nd.lat > self.max_lat { self.max_lat = nd.lat; }
    }
    
    pub fn add_minimal(&mut self, nd: &MinimalNode) {
        self.num += 1;
        if self.min_id==-1 || nd.id < self.min_id { self.min_id = nd.id; }
        if self.max_id==-1 || nd.id > self.max_id { self.max_id = nd.id; }
        
        if self.min_ts==-1 || nd.timestamp < self.min_ts { self.min_ts = nd.timestamp; }
        if self.max_ts==-1 || nd.timestamp > self.max_ts { self.max_ts = nd.timestamp; }
        
        if nd.lon < self.min_lon { self.min_lon = nd.lon; }
        if nd.lon > self.max_lon { self.max_lon = nd.lon; }
        if nd.lat < self.min_lat { self.min_lat = nd.lat; }
        if nd.lat > self.max_lat { self.max_lat = nd.lat; }
    }
    
    pub fn add_other(&mut self, nc: &NodeCount) {
        if nc.min_id==-1 { return; }
            
        self.num += nc.num;
        if self.min_id==-1 || nc.min_id < self.min_id { self.min_id = nc.min_id; }
        if self.max_id==-1 || nc.max_id > self.max_id { self.max_id = nc.max_id; }
        
        if self.min_ts==-1 || nc.min_ts < self.min_ts { self.min_ts = nc.min_ts; }
        if self.max_ts==-1 || nc.max_ts > self.max_ts { self.max_ts = nc.max_ts; }
        
        if nc.min_lon < self.min_lon { self.min_lon = nc.min_lon; }
        if nc.max_lon > self.max_lon { self.max_lon = nc.max_lon; }
        if nc.min_lat < self.min_lat { self.min_lat = nc.min_lat; }
        if nc.max_lat > self.max_lat { self.max_lat = nc.max_lat; }
    }
        
}

impl fmt::Display for NodeCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{{:-10}, {:-10}, {:-10}, {:-10}}}",
            self.num, self.min_id, self.max_id, timestamp_string(self.min_ts), timestamp_string(self.max_ts),
            self.min_lon, self.min_lat, self.max_lon, self.max_lat)
        
    }
}
    
    

#[derive(Debug)]        
pub struct WayCount {
    num: i64,
    min_id: i64,
    max_id: i64,
    min_ts: i64,
    max_ts: i64,
    num_refs: i64,
    max_refs_len: i64,
    min_ref:i64,
    max_ref: i64,
}

impl WayCount {
    pub fn new() -> WayCount {
        WayCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, num_refs:0, max_refs_len:-1,min_ref:-1,max_ref:-1}
    }
    pub fn add(&mut self, wy: &Way) {
        self.num += 1;
        if self.min_id==-1 || wy.id < self.min_id { self.min_id = wy.id; }
        if self.max_id==-1 || wy.id > self.max_id { self.max_id = wy.id; }
        
        match &wy.info {
            Some(info) => {
                if self.min_ts==-1 || info.timestamp < self.min_ts { self.min_ts = info.timestamp; }
                if self.max_ts==-1 || info.timestamp > self.max_ts { self.max_ts = info.timestamp; }
            },
            None => {}
        }
        
        self.num_refs += wy.refs.len() as i64;
        if self.max_refs_len==-1 || wy.refs.len() as i64>self.max_refs_len { self.max_refs_len = wy.refs.len() as i64; }
        
        for r in &wy.refs {
            if self.min_ref==-1 || *r < self.min_id { self.min_ref = *r; }
            if self.max_ref==-1 || *r > self.max_id { self.max_ref = *r; }
        }
    }
    
    pub fn add_minimal(&mut self, wy: &MinimalWay) {
        self.num += 1;
        if self.min_id==-1 || wy.id < self.min_id { self.min_id = wy.id; }
        if self.max_id==-1 || wy.id > self.max_id { self.max_id = wy.id; }
        
        if self.min_ts==-1 || wy.timestamp < self.min_ts { self.min_ts = wy.timestamp; }
        if self.max_ts==-1 || wy.timestamp > self.max_ts { self.max_ts = wy.timestamp; }
        
        
        let refs = read_delta_packed_int(&wy.refs_data);
        
        self.num_refs += refs.len() as i64;
        if self.max_refs_len==-1 || refs.len() as i64>self.max_refs_len { self.max_refs_len = refs.len() as i64; }
        
        for r in &refs {
            if self.min_ref==-1 || *r < self.min_id { self.min_ref = *r; }
            if self.max_ref==-1 || *r > self.max_id { self.max_ref = *r; }
        }
        
    }
    
    pub fn add_other(&mut self, wc: &WayCount) {
        if wc.min_id==-1 { return; }
        
        self.num += wc.num;
        
        if self.min_id==-1 || wc.min_id < self.min_id { self.min_id = wc.min_id; }
        if self.max_id==-1 || wc.max_id > self.max_id { self.max_id = wc.max_id; }
        
        if self.min_ts==-1 || wc.min_ts < self.min_ts { self.min_ts = wc.min_ts; }
        if self.max_ts==-1 || wc.max_ts > self.max_ts { self.max_ts = wc.max_ts; }
        
        self.num_refs += wc.num_refs;
        if self.max_refs_len==-1 || wc.max_refs_len>self.max_refs_len { self.max_refs_len = wc.max_refs_len; }
        
        if self.min_ref==-1 || wc.min_ref < self.min_ref { self.min_ref = wc.min_ref; }
        if self.max_ref==-1 || wc.max_ref > self.max_ref { self.max_ref = wc.max_ref; }
        
    }
    
}

impl fmt::Display for WayCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{{} refs, {} to {}. Longest: {}}}",
            self.num, self.min_id, self.max_id, timestamp_string(self.min_ts), timestamp_string(self.max_ts),
            self.num_refs, self.min_ref, self.max_ref, self.max_refs_len)
        
    }
}

#[derive(Debug)]
pub struct RelationCount {
    num: i64,
    min_id: i64,
    max_id: i64,
    min_ts: i64,
    max_ts: i64,
    num_empties: i64,
    num_mems: i64,
    max_mems_len: i64,
    
}
impl RelationCount {
    pub fn new() -> RelationCount {
        RelationCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, num_empties: 0, num_mems: 0, max_mems_len: 0}
    }
    pub fn add(&mut self, rl: &Relation) {
        self.num += 1;
        if self.min_id==-1 || rl.id < self.min_id { self.min_id = rl.id; }
        if self.max_id==-1 || rl.id > self.max_id { self.max_id = rl.id; }
        
        match &rl.info {
            Some(info) => {
                if self.min_ts==-1 || info.timestamp < self.min_ts { self.min_ts = info.timestamp; }
                if self.max_ts==-1 || info.timestamp > self.max_ts { self.max_ts = info.timestamp; }
            },
            None => {}
        }
        
        if rl.members.len() == 0 { self.num_empties += 1; }
        self.num_mems += rl.members.len() as i64;
        if self.max_mems_len==-1 || rl.members.len() as i64>self.max_mems_len { self.max_mems_len = rl.members.len() as i64; }
    }
    
    pub fn add_minimal(&mut self, rl: &MinimalRelation) {
        self.num += 1;
        if self.min_id==-1 || rl.id < self.min_id { self.min_id = rl.id; }
        if self.max_id==-1 || rl.id > self.max_id { self.max_id = rl.id; }
        
        if self.min_ts==-1 || rl.timestamp < self.min_ts { self.min_ts = rl.timestamp; }
        if self.max_ts==-1 || rl.timestamp > self.max_ts { self.max_ts = rl.timestamp; }
        
        //if rl.refs_data.len() == 0 { self.num_empties += 1; }
        let nr = read_delta_packed_int(&rl.refs_data).len() as i64;
        self.num_mems += nr;
        if nr == 0 { self.num_empties += 1; }
        if self.max_mems_len==-1 || nr>self.max_mems_len { self.max_mems_len = nr as i64; }
    }
    
    
    pub fn add_other(&mut self, rc: &RelationCount) {
        if rc.min_id==-1 { return; }
        self.num += rc.num;
        
        if self.min_id==-1 || rc.min_id < self.min_id { self.min_id = rc.min_id; }
        if self.max_id==-1 || rc.max_id > self.max_id { self.max_id = rc.max_id; }
        
        if self.min_ts==-1 || rc.min_ts < self.min_ts { self.min_ts = rc.min_ts; }
        if self.max_ts==-1 || rc.max_ts > self.max_ts { self.max_ts = rc.max_ts; }
        
        self.num_empties += rc.num_empties;
        self.num_mems += rc.num_mems;
        if self.max_mems_len==-1 || rc.max_mems_len > self.max_mems_len { self.max_mems_len = rc.max_mems_len; }
    }
}

impl fmt::Display for RelationCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{Longest: {}, {} empties.}}",
            self.num, self.min_id, self.max_id, timestamp_string(self.min_ts), timestamp_string(self.max_ts),
            self.max_mems_len, self.num_empties)
        
    }
}

pub trait CountBlocks {
    fn add_primitive(&mut self, bl: &PrimitiveBlock);
    fn add_minimal(&mut self, mb: &MinimalBlock);
}

#[derive(Debug)]
pub struct Count {
    pub node: NodeCount,
    pub way: WayCount,
    pub relation: RelationCount,
}
impl Count {
    pub fn new() -> Count {
        Count{node: NodeCount::new(), way: WayCount::new(), relation: RelationCount::new()}
    }
    pub fn add_other(&mut self, other: &Count) {
        self.node.add_other(&other.node);
        self.way.add_other(&other.way);
        self.relation.add_other(&other.relation);
    }
}
impl CountBlocks for Count {
    fn add_primitive(&mut self, bl: &PrimitiveBlock) {
        for nd in &bl.nodes {
            self.node.add(&nd);
        }
        for wy in &bl.ways {
            self.way.add(&wy);
        }
        for rl in &bl.relations {
            self.relation.add(&rl);
        }
    }
    
    fn add_minimal(&mut self, mb: &MinimalBlock) {
        for nd in &mb.nodes {
            self.node.add_minimal(&nd);
        }
        for wy in &mb.ways {
            self.way.add_minimal(&wy);
        }
        for rl in &mb.relations {
            self.relation.add_minimal(&rl);
        }
    }
}

    
impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "node:      {}\nway:       {}\nrelations: {}", self.node, self.way, self.relation)
    }
}
    
    
#[derive(Debug)]
pub struct CountChange {
    pub node: BTreeMap<Changetype,NodeCount>,
    pub way: BTreeMap<Changetype,WayCount>,
    pub relation: BTreeMap<Changetype,RelationCount>,
}
impl CountChange {
    pub fn new() -> CountChange {
        CountChange{node: BTreeMap::new(), way: BTreeMap::new(), relation: BTreeMap::new()}
    }
    pub fn add_changeblock(&mut self, bl: &ChangeBlock) {
        for (_,nd) in &bl.nodes {
            if !self.node.contains_key(&nd.changetype) {
                self.node.insert(nd.changetype, NodeCount::new());
            }
            self.node.get_mut(&nd.changetype).unwrap().add(&nd);
        }
        for (_,wy) in &bl.ways {
            if !self.way.contains_key(&wy.changetype) {
                self.way.insert(wy.changetype, WayCount::new());
            }
            self.way.get_mut(&wy.changetype).unwrap().add(&wy);
            
        }
        for (_,rl) in &bl.relations {
            if !self.relation.contains_key(&rl.changetype) {
                self.relation.insert(rl.changetype, RelationCount::new());
            }
            self.relation.get_mut(&rl.changetype).unwrap().add(&rl);
            
        }
    }
}
impl CountBlocks for CountChange {
    fn add_primitive(&mut self, bl: &PrimitiveBlock) {
        for nd in &bl.nodes {
            if !self.node.contains_key(&nd.changetype) {
                self.node.insert(nd.changetype, NodeCount::new());
            }
            self.node.get_mut(&nd.changetype).unwrap().add(&nd);
        }
        for wy in &bl.ways {
            if !self.way.contains_key(&wy.changetype) {
                self.way.insert(wy.changetype, WayCount::new());
            }
            self.way.get_mut(&wy.changetype).unwrap().add(&wy);
            
        }
        for rl in &bl.relations {
            if !self.relation.contains_key(&rl.changetype) {
                self.relation.insert(rl.changetype, RelationCount::new());
            }
            self.relation.get_mut(&rl.changetype).unwrap().add(&rl);
            
        }
    }
    
    fn add_minimal(&mut self, bl: &MinimalBlock) {
        for nd in &bl.nodes {
            let ct=get_changetype(nd.changetype as u64);
            
            if !self.node.contains_key(&ct) {
                self.node.insert(ct, NodeCount::new());
            }
            self.node.get_mut(&ct).unwrap().add_minimal(&nd);
        }
        for wy in &bl.ways {
            let ct=get_changetype(wy.changetype as u64);
            if !self.way.contains_key(&ct) {
                self.way.insert(ct, WayCount::new());
            }
            self.way.get_mut(&ct).unwrap().add_minimal(&wy);
            
        }
        for rl in &bl.relations {
            let ct=get_changetype(rl.changetype as u64);
            if !self.relation.contains_key(&ct) {
                self.relation.insert(ct, RelationCount::new());
            }
            self.relation.get_mut(&ct).unwrap().add_minimal(&rl);
            
        }
    }    
}

impl fmt::Display for CountChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        
        write!(f, "nodes:")?;
        for (a,b) in &self.node {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f, "\nways:")?;
        for (a,b) in &self.way {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f, "\nrelations:")?;
        for (a,b) in &self.relation {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f,"")
        
    }
}
