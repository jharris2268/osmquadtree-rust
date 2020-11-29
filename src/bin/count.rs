extern crate osmquadtree;
extern crate cpuprofiler;

use std::fs::File;

use std::env;

use osmquadtree::read_file_block::{FileBlock,read_all_blocks_prog,read_all_blocks_parallel_prog,ProgBarWrap,file_length};
use osmquadtree::read_pbf::{read_delta_packed_int};

use osmquadtree::elements::{Changetype,PrimitiveBlock,MinimalBlock,Bbox,Node,Way,Relation,get_changetype,MinimalNode,MinimalWay,MinimalRelation};
use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::{read_xml_change,ChangeBlock,get_file_locs};
use osmquadtree::utils::timestamp_string;

use osmquadtree::callback::{Callback,CallbackMerge,CallFinish};
use osmquadtree::utils::{ThreadTimer,MergeTimings,CallAll};


use std::io::{Error,ErrorKind,Result};
use std::io::BufReader;
use std::fmt;
use std::collections::BTreeMap;

use cpuprofiler::PROFILER;


    
    


#[derive(Debug)]
struct NodeCount {
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
    fn new() -> NodeCount {
        NodeCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, min_lon:1800000000,min_lat:900000000,max_lon:-1800000000,max_lat:-900000000}
    }
    fn add(&mut self, nd: &Node) {
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
    
    fn add_minimal(&mut self, nd: &MinimalNode) {
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
    
    fn add_other(&mut self, nc: &NodeCount) {
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
struct WayCount {
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
    fn new() -> WayCount {
        WayCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, num_refs:0, max_refs_len:-1,min_ref:-1,max_ref:-1}
    }
    fn add(&mut self, wy: &Way) {
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
    
    fn add_minimal(&mut self, wy: &MinimalWay) {
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
    
    fn add_other(&mut self, wc: &WayCount) {
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
struct RelationCount {
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
    fn new() -> RelationCount {
        RelationCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, num_empties: 0, num_mems: 0, max_mems_len: 0}
    }
    fn add(&mut self, rl: &Relation) {
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
    
    fn add_minimal(&mut self, rl: &MinimalRelation) {
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
    
    
    fn add_other(&mut self, rc: &RelationCount) {
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

trait CountBlocks {
    fn add_primitive(&mut self, bl: &PrimitiveBlock);
    fn add_minimal(&mut self, mb: &MinimalBlock);
}

#[derive(Debug)]
struct Count {
    pub node: NodeCount,
    pub way: WayCount,
    pub relation: RelationCount,
}
impl Count {
    fn new() -> Count {
        Count{node: NodeCount::new(), way: WayCount::new(), relation: RelationCount::new()}
    }
    fn add_other(&mut self, other: &Count) {
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
struct CountChange {
    pub node: BTreeMap<Changetype,NodeCount>,
    pub way: BTreeMap<Changetype,WayCount>,
    pub relation: BTreeMap<Changetype,RelationCount>,
}
impl CountChange {
    fn new() -> CountChange {
        CountChange{node: BTreeMap::new(), way: BTreeMap::new(), relation: BTreeMap::new()}
    }
    fn add_changeblock(&mut self, bl: &ChangeBlock) {
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

struct CountChangeMinimal {
    cc: Option<CountChange>,
    tm: f64
}

impl CountChangeMinimal {
    pub fn new() -> CountChangeMinimal {
        CountChangeMinimal{cc: Some(CountChange::new()), tm: 0.0}
    }
}



impl CallFinish for CountChangeMinimal {
    type CallType = MinimalBlock;
    type ReturnType = osmquadtree::utils::Timings::<CountChange>;
    
    fn call(&mut self, bl: MinimalBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = osmquadtree::utils::Timings::<CountChange>::new();
        tm.add("countchange", self.tm);
        tm.add_other("countchange", self.cc.take().unwrap());
        Ok(tm)
    }
}
  



struct CountPrim {
    cc: Option<Count>,
    tm: f64
}

impl CountPrim {
    pub fn new() -> CountPrim {
        CountPrim{cc: Some(Count::new()), tm: 0.0}
    }
}

type Timings = osmquadtree::utils::Timings<Count>;

impl CallFinish for CountPrim {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_primitive(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Timings> {
        let mut tm = Timings::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}
  
  
struct CountMinimal {
    cc: Option<Count>,
    tm: f64
}

impl CountMinimal {
    pub fn new() -> CountMinimal {
        CountMinimal{cc: Some(Count::new()), tm: 0.0}
    }
}


impl CallFinish for CountMinimal {
    type CallType = MinimalBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: MinimalBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Timings> {
        let mut tm = Timings::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}

fn make_convert_minimal_block<T: CallFinish<CallType=MinimalBlock,ReturnType=osmquadtree::utils::Timings<U>>,U: Sync+Send+'static>(ischange: bool, out: Box<T>) -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=osmquadtree::utils::Timings<U>>> {
    let convert_minimal = move |(i,fb): (usize, FileBlock)| -> MinimalBlock {
        if fb.block_type == "OSMData" {
            MinimalBlock::read(i as i64, fb.pos, &fb.data(),ischange).expect("?")
        } else {
            MinimalBlock::new()
        }
    };
    
    Box::new(CallAll::new(out, "convert minimal", Box::new(convert_minimal)))
    
}

fn make_convert_primitive_block<T: CallFinish<CallType=PrimitiveBlock,ReturnType=Timings>>(ischange: bool, out: Box<T>) -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>> {
    let convert_minimal = move |(i,fb): (usize, FileBlock)| -> PrimitiveBlock {
        if fb.block_type == "OSMData" {
            PrimitiveBlock::read(i as i64, fb.pos, &fb.data(),ischange,false).expect("?")
        } else {
            PrimitiveBlock::new(0,0)
        }
    };
    
    Box::new(CallAll::new(out, "convert primitive", Box::new(convert_minimal)))
    
}

fn parse_bbox(fstr: &str) -> Result<Bbox> {
    
    let vv:Vec<&str> = fstr.split(",").collect();
    if vv.len()!=4 {
        return Err(Error::new(ErrorKind::Other,"expected four vals"));
    }
    let mut vvi = Vec::new();
    for v in vv {
        vvi.push(v.parse().unwrap());
    }
    Ok(Bbox::new(vvi[0],vvi[1],vvi[2],vvi[3]))
}
    
    


fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
        
    
    let mut prof = String::from("");
    let mut minimal = false;
    let mut numchan = 4;
    let mut filter: Option<Bbox> = None;
    
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("prof=") {
                prof = args[i].substr(5,args[i].len());
            } else if args[i] == "minimal" {
                minimal=true;
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("filter=") {
                filter = Some(parse_bbox(&args[i].substr(7,args[i].len())).expect("failed to read filter"));
            }
        }
    }
     
    if prof.len()>0 {
        PROFILER.lock().unwrap().start(prof.clone()).expect("couldn't start");
    }
    
    let f = File::open(&fname).expect("file not present");
    
   
    let mut pb = ProgBarWrap::new(100);
    pb.set_range(100);
    
    if fname.ends_with(".osc") {
        let mut cn = CountChange::new();
        let mut fbuf = BufReader::with_capacity(1024*1024, f);
        let data = read_xml_change(&mut fbuf).expect("failed to read osc");
        
        cn.add_changeblock(&data);
        println!("{}", cn);
    } else if fname.ends_with(".osc.gz") {
        let mut cn = CountChange::new();
        let fbuf = BufReader::with_capacity(1024*1024, f);
        let mut gzbuf = BufReader::new(flate2::bufread::GzDecoder::new(fbuf));
        //Box::new(gzbuf) as Box<dyn std::io::BufRead>
        let data = read_xml_change(&mut gzbuf).expect("failed to read osc");
        
        cn.add_changeblock(&data);
        println!("{}", cn);
    } else if fname.ends_with(".pbfc") {
        pb.set_message(&format!("count change blocks minimal {}, numchan=1", &fname));
        let flen = file_length(&fname);
        let mut fbuf = BufReader::new(f);
        let cc = Box::new(CountChangeMinimal::new());
        let cn = Box::new(Callback::new(make_convert_minimal_block(true,cc)));
        let (mut a,_) = read_all_blocks_prog(&mut fbuf, flen, cn, &pb);
        pb.finish();
        let cn = std::mem::take(&mut a.others).pop().unwrap().1;
        
        println!("{}", cn);
        //println!("{:?}", cn.relation.get(&Changetype::Create));
        
    } else if std::fs::metadata(&fname).expect("failed to open file").is_file() {
        let flen = file_length(&fname);
        let mut cc = Count::new();
        if numchan == 0 {
            let mut fbuf=BufReader::new(f);
            
            let (a,_) = if minimal {
                pb.set_message(&format!("count blocks minimal {}, numchan=0", &fname));
                let cm = Box::new(CountMinimal::new());
                let cc = make_convert_minimal_block(false,cm);
                read_all_blocks_prog(&mut fbuf, flen, cc, &pb)
            } else {
                pb.set_message(&format!("count blocks primitive {}, numchan=0", &fname));
                let cm = Box::new(CountPrim::new());
                let cc = make_convert_primitive_block(false,cm);
                read_all_blocks_prog(&mut fbuf, flen, cc, &pb)
            };
            pb.finish();
            cc.add_other(&a.others[0].1);
            
            //cc = count_all(cc, read_file_block::ReadFileBlocks::new(&mut fbuf), 0, &fname, minimal, false);
        
        } else if numchan > 8 {
            panic!("numchan must be between 0 & 8");
        } else {
            
            let mut fbuf=f;
            
            
            
            
            let mut ccs: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
            for _ in  0..numchan {
                if minimal {
                    pb.set_message(&format!("count blocks minimal {}, numchan={}", &fname,numchan));
                    let cm = Box::new(CountMinimal::new());
                    ccs.push(Box::new(Callback::new(make_convert_minimal_block(false,cm))));
                } else {
                    pb.set_message(&format!("count blocks primitive {}, numchan={}", &fname,numchan));
                    let cm = Box::new(CountPrim::new());
                    ccs.push(Box::new(Callback::new(make_convert_primitive_block(false,cm))));
                }
            }
            let cm = Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new())));
            let (a,_)  = read_all_blocks_prog(&mut fbuf, flen, cm, &pb);
            pb.finish();
            for (_,x) in a.others {
                cc.add_other(&x);
            }
        
        }
        println!("{}", cc);
    } else {
        
        
        let (fbufs, locsv) = get_file_locs(&fname, filter).expect("?");
        
        
        
        
        
        let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        
        if minimal { 
            for _ in 0..numchan {
                pb.set_message(&format!("count blocks combine minimal {}, numchan={}", &fname,numchan));
                let cca = Box::new(CountMinimal::new());
                pps.push(Box::new(Callback::new(osmquadtree::convertblocks::make_read_minimal_blocks_combine_call_all(cca))));
            }
        
        } else {
            for _ in 0..numchan {
                pb.set_message(&format!("count blocks combine primitive {}, numchan={}", &fname,numchan));
                let cca = Box::new(CountPrim::new());
                pps.push(Box::new(Callback::new(osmquadtree::convertblocks::make_read_primitive_blocks_combine_call_all(cca))));
            }
        }
        let readb = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
        let (a,_b) = read_all_blocks_parallel_prog(fbufs, locsv, readb, &pb);
        pb.finish();
        
        let mut cc = Count::new();
        for (_,y) in &a.others {
            cc.add_other(y);
        }
        
        println!("{}", cc);
        
        
        
    }
        
    if prof.len()>0 {
        PROFILER.lock().unwrap().stop().expect("couldn't stop");
    }
    
    
    
    
}
