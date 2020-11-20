extern crate osmquadtree;
extern crate cpuprofiler;

use std::fs::File;

use std::env;
use osmquadtree::read_file_block;
use osmquadtree::read_pbf;
use osmquadtree::header_block;
//use osmquadtree::quadtree;
use osmquadtree::primitive_block;
use osmquadtree::node;
use osmquadtree::way;
use osmquadtree::relation;
use osmquadtree::minimal_block;
use osmquadtree::primitive_block::Changetype;
use osmquadtree::stringutils::StringUtils;

//use osmquadtree::dense;
//use osmquadtree::common;
//use osmquadtree::info;
//use osmquadtree::tags;
use std::io::Write;

use std::thread;
use std::sync::mpsc;

use std::io::BufReader;
use std::fmt;
use std::collections::BTreeMap;

use cpuprofiler::PROFILER;
use chrono::NaiveDateTime;

macro_rules! println_stderr(
    ($($arg:tt)*) => { {
        let r = writeln!(&mut ::std::io::stderr(), $($arg)*);
        r.expect("failed printing to stderr");
    } }
);

const TIMEFORMAT: &str = "%Y-%m-%dT%H:%M:%S";
fn timestamp_string(ts: i64) -> String {
    let dt = NaiveDateTime::from_timestamp(ts,0);
    dt.format(TIMEFORMAT).to_string()
}
    
    


#[derive(Debug)]
struct NodeCount {
    num: i64,
    min_id: i64,
    max_id: i64,
    min_ts: i64,
    max_ts: i64,
    min_lon: i64,
    min_lat: i64,
    max_lon: i64,
    max_lat: i64,
}
impl NodeCount {
    fn new() -> NodeCount {
        NodeCount{num:0, min_id:-1,max_id:-1, min_ts:-1, max_ts:-1, min_lon:1800000000,min_lat:900000000,max_lon:-1800000000,max_lat:-900000000}
    }
    fn add(&mut self, nd: &node::Node) {
        self.num += 1;
        if self.min_id==-1 || nd.common.id < self.min_id { self.min_id = nd.common.id; }
        if self.max_id==-1 || nd.common.id > self.max_id { self.max_id = nd.common.id; }
        
        if self.min_ts==-1 || nd.common.info.timestamp < self.min_ts { self.min_ts = nd.common.info.timestamp; }
        if self.max_ts==-1 || nd.common.info.timestamp > self.max_ts { self.max_ts = nd.common.info.timestamp; }
        
        if nd.lon < self.min_lon { self.min_lon = nd.lon; }
        if nd.lon > self.max_lon { self.max_lon = nd.lon; }
        if nd.lat < self.min_lat { self.min_lat = nd.lat; }
        if nd.lat > self.max_lat { self.max_lat = nd.lat; }
    }
    
    fn add_minimal(&mut self, nd: &minimal_block::MinimalNode) {
        self.num += 1;
        if self.min_id==-1 || nd.id < self.min_id { self.min_id = nd.id; }
        if self.max_id==-1 || nd.id > self.max_id { self.max_id = nd.id; }
        
        if self.min_ts==-1 || nd.timestamp < self.min_ts { self.min_ts = nd.timestamp; }
        if self.max_ts==-1 || nd.timestamp > self.max_ts { self.max_ts = nd.timestamp; }
        
        if (nd.lon as i64) < self.min_lon { self.min_lon = nd.lon as i64; }
        if (nd.lon as i64) > self.max_lon { self.max_lon = nd.lon as i64; }
        if (nd.lat as i64) < self.min_lat { self.min_lat = nd.lat as i64; }
        if (nd.lat as i64) > self.max_lat { self.max_lat = nd.lat as i64; }
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
        write!(f, "nodes: {} objects: {} => {} [{} => {}] {{{}, {}, {}, {}}}",
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
    fn add(&mut self, wy: &way::Way) {
        self.num += 1;
        if self.min_id==-1 || wy.common.id < self.min_id { self.min_id = wy.common.id; }
        if self.max_id==-1 || wy.common.id > self.max_id { self.max_id = wy.common.id; }
        
        if self.min_ts==-1 || wy.common.info.timestamp < self.min_ts { self.min_ts = wy.common.info.timestamp; }
        if self.max_ts==-1 || wy.common.info.timestamp > self.max_ts { self.max_ts = wy.common.info.timestamp; }
        
        self.num_refs += wy.refs.len() as i64;
        if self.max_refs_len==-1 || wy.refs.len() as i64>self.max_refs_len { self.max_refs_len = wy.refs.len() as i64; }
        
        for r in &wy.refs {
            if self.min_ref==-1 || *r < self.min_id { self.min_ref = *r; }
            if self.max_ref==-1 || *r > self.max_id { self.max_ref = *r; }
        }
    }
    
    fn add_minimal(&mut self, wy: &minimal_block::MinimalWay) {
        self.num += 1;
        if self.min_id==-1 || wy.id < self.min_id { self.min_id = wy.id; }
        if self.max_id==-1 || wy.id > self.max_id { self.max_id = wy.id; }
        
        if self.min_ts==-1 || wy.timestamp < self.min_ts { self.min_ts = wy.timestamp; }
        if self.max_ts==-1 || wy.timestamp > self.max_ts { self.max_ts = wy.timestamp; }
        
        
        let refs = read_pbf::read_delta_packed_int(&wy.refs_data);
        
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
        write!(f, "ways: {} objects: {} => {} [{} => {}] {{{} refs, {} to {}. Longest: {}}}",
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
    fn add(&mut self, rl: &relation::Relation) {
        self.num += 1;
        if self.min_id==-1 || rl.common.id < self.min_id { self.min_id = rl.common.id; }
        if self.max_id==-1 || rl.common.id > self.max_id { self.max_id = rl.common.id; }
        
        if self.min_ts==-1 || rl.common.info.timestamp < self.min_ts { self.min_ts = rl.common.info.timestamp; }
        if self.max_ts==-1 || rl.common.info.timestamp > self.max_ts { self.max_ts = rl.common.info.timestamp; }
        
        if rl.members.len() == 0 { self.num_empties += 1; }
        self.num_mems += rl.members.len() as i64;
        if self.max_mems_len==-1 || rl.members.len() as i64>self.max_mems_len { self.max_mems_len = rl.members.len() as i64; }
    }
    
    fn add_minimal(&mut self, rl: &minimal_block::MinimalRelation) {
        self.num += 1;
        if self.min_id==-1 || rl.id < self.min_id { self.min_id = rl.id; }
        if self.max_id==-1 || rl.id > self.max_id { self.max_id = rl.id; }
        
        if self.min_ts==-1 || rl.timestamp < self.min_ts { self.min_ts = rl.timestamp; }
        if self.max_ts==-1 || rl.timestamp > self.max_ts { self.max_ts = rl.timestamp; }
        
        //if rl.refs_data.len() == 0 { self.num_empties += 1; }
        let nr = read_pbf::read_delta_packed_int(&rl.refs_data).len() as i64;
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
        write!(f, "relation. {} object. {} => {} [{} => {}] {{Longest: {}, {} empties.}}",
            self.num, self.min_id, self.max_id, timestamp_string(self.min_ts), timestamp_string(self.max_ts),
            self.max_mems_len, self.num_empties)
        
    }
}
    
#[derive(Debug)]
struct Count {
    node: NodeCount,
    way: WayCount,
    relation: RelationCount,
}
impl Count {
    fn new() -> Count {
        Count{node: NodeCount::new(), way: WayCount::new(), relation: RelationCount::new()}
    }
    
    fn add_primitive(&mut self, bl: &primitive_block::PrimitiveBlock) {
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
    
    fn add_minimal(&mut self, mb: &minimal_block::MinimalBlock) {
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
    
    fn add_other(&mut self, other: &Count) {
        self.node.add_other(&other.node);
        self.way.add_other(&other.way);
        self.relation.add_other(&other.relation);
    }
}
impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}\n{}\n{}", self.node, self.way, self.relation)
    }
}
    
    
#[derive(Debug)]
struct CountChange {
    node: BTreeMap<Changetype,NodeCount>,
    way: BTreeMap<Changetype,WayCount>,
    relation: BTreeMap<Changetype,RelationCount>,
}
impl CountChange {
    fn new() -> CountChange {
        CountChange{node: BTreeMap::new(), way: BTreeMap::new(), relation: BTreeMap::new()}
    }
    
    fn add_primitive(&mut self, bl: &primitive_block::PrimitiveBlock) {
        for nd in &bl.nodes {
            if !self.node.contains_key(&nd.common.changetype) {
                self.node.insert(nd.common.changetype, NodeCount::new());
            }
            self.node.get_mut(&nd.common.changetype).unwrap().add(&nd);
        }
        for wy in &bl.ways {
            if !self.way.contains_key(&wy.common.changetype) {
                self.way.insert(wy.common.changetype, WayCount::new());
            }
            self.way.get_mut(&wy.common.changetype).unwrap().add(&wy);
            
        }
        for rl in &bl.relations {
            if !self.relation.contains_key(&rl.common.changetype) {
                self.relation.insert(rl.common.changetype, RelationCount::new());
            }
            self.relation.get_mut(&rl.common.changetype).unwrap().add(&rl);
            
        }
    }
    
    
}
impl fmt::Display for CountChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut p=Vec::new();
        for (a,b) in &self.node {
            p.push(format!("{} {}", a, b));
        }
        for (a,b) in &self.way {
            p.push(format!("{} {}", a, b));
        }
        for (a,b) in &self.relation {
            p.push(format!("{} {}", a, b));
        }
        
        write!(f, "{}",  p.join("\n"))
        
    }
}
fn as_secs(dur: std::time::Duration) -> f64 {
    (dur.as_secs() as f64)*1.0 + (dur.subsec_nanos() as f64)*0.000000001
}


fn add_fb(cc: &mut Count, idx: i64, fb: &osmquadtree::read_file_block::FileBlock, minimal: bool, firstthread: bool, lt: &mut std::time::SystemTime, st: &std::time::SystemTime) {
    let fbd = fb.data();
                
    if fb.block_type == "OSMHeader" {
        let hh = header_block::HeaderBlock::read(fb.pos+fb.len, &fbd).unwrap();
        println!("header_block(bbox: {:?}, writer: {}, features: {:?}, {} index entries)", hh.bbox, hh.writer, hh.features, hh.index.len());
    } else {
        //cc.node.num += fbd.len() as i64;
        
        if minimal {
            match minimal_block::MinimalBlock::read(idx, fb.pos+fb.len, &fbd, false) {
                Ok(mb) => {
                    if firstthread {
                        let lm=as_secs(lt.elapsed().unwrap());
                        if lm>5.0 {
                            println!("time {} minimal_block(index: {}, pos: {}, quadtree: {:?}, start_date: {}, end_date: {}): {} nodes {} ways {} relations",
                                as_secs(st.elapsed().unwrap()), mb.index, mb.location, mb.quadtree, mb.start_date, mb.end_date, mb.nodes.len(), mb.ways.len(), mb.relations.len());
                            *lt = std::time::SystemTime::now();
                        }
                    }
                    
                    cc.add_minimal(&mb);                        
                    //drop(mb);
                },
                Err(err) => println_stderr!("?? {:?}", err),
            }  
        } else {
        
        
            match primitive_block::PrimitiveBlock::read(idx, fb.pos+fb.len, &fbd, false, minimal) {
                Ok(pb) => {
                    if firstthread {
                        let lm=as_secs(lt.elapsed().unwrap());
                        if lm>5.0 {
                            println!("time {} primitive_block(index: {}, pos: {}, quadtree: {:?}, start_date: {}, end_date: {}): {} features",
                                as_secs(st.elapsed().unwrap()), pb.index, pb.location, pb.quadtree, pb.start_date, pb.end_date, pb.len());
                            *lt = std::time::SystemTime::now();
                        }
                    }
                    
                    cc.add_primitive(&pb);                         
                    
                },
                Err(err) => println_stderr!("?? {:?}", err),
            }  
        }
    }
}
    
    
    

fn count_all<I>(recv: I, idx: i64, minimal: bool) -> Count
where I: Iterator<Item=osmquadtree::read_file_block::FileBlock>,
{
    let firstthread=idx==0;
    let mut idx=idx;
    let mut cc = Count::new();
    
    let st = std::time::SystemTime::now();
    let mut lt = std::time::SystemTime::now();

    for fb in recv {
        add_fb(&mut cc, idx, &fb, minimal, firstthread, &mut lt, &st);
        idx+=4;
        
    }
    return cc;
    
}

/*trait StringUtils {
    fn substr(&self, start: usize, len: usize) -> Self;
}

impl StringUtils for String {
    fn substr(&self, start: usize, len: usize) -> Self {
        self.chars().skip(start).take(len).collect()
    }
}*/

fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
        
    
    let mut prof = String::from("");
    let mut minimal = false;
    let mut numchan = 4;
    
    
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("prof=") {
                prof = args[i].substr(5,args[i].len());
            } else if args[i] == "minimal" {
                minimal=true;
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            }
        }
    }
     
    if prof.len()>0 {
        PROFILER.lock().unwrap().start(prof.clone()).expect("couldn't start");
    }
    
    let f = File::open(&fname).expect("file not present");
    let mut fbuf = BufReader::new(f);
   
    
    
    if fname.ends_with(".osc") {
        let mut cn = CountChange::new();
        let data = osmquadtree::read_xml::read_xml_change(fbuf).expect("failed to read osc");
        
        cn.add_primitive(&data);
        println!("{}", cn);
    } else if fname.ends_with(".osc.gz") {
        let mut cn = CountChange::new();
        let gzbuf = BufReader::new(flate2::bufread::GzDecoder::new(fbuf));
        //Box::new(gzbuf) as Box<dyn std::io::BufRead>
        let data = osmquadtree::read_xml::read_xml_change(gzbuf).expect("failed to read osc");
        
        cn.add_primitive(&data);
        println!("{}", cn);
    } else {
        let mut cc = Count::new();
        if numchan == 0 {
        
            cc = count_all(read_file_block::ReadFileBlocks::new(&mut fbuf), 0, minimal);
        
        } else if numchan > 8 {
            panic!("numchan must be between 0 & 8");
        } else {
        
            let mut senders = Vec::new();
            let mut results = Vec::new();
            
            for i in 0..numchan {
                let (s,r) = mpsc::sync_channel(1);
                senders.push(s);
                results.push(thread::spawn(move || count_all(r.iter(), i as i64, minimal)));
            }
            
            
            for (i,fb) in read_file_block::ReadFileBlocks::new(&mut fbuf).enumerate() {
                senders[i%numchan].send(fb).unwrap();
            }        
            for s in senders {
                drop(s);
            }
            
            
            for r in results {
                match r.join() {
                    Ok(cci) => cc.add_other(&cci),
                    Err(e) => println!("?? {:?}", e),
                }
            }
            
        }
        println!("{}", cc);
    }
        
    if prof.len()>0 {
        PROFILER.lock().unwrap().stop().expect("couldn't stop");
    }
    
    
    
    
}
