use std::fs::File;

use crate::pbfformat::{
    file_length, read_all_blocks_parallel_with_progbar, read_all_blocks_prog_fpos, FileBlock
};

use crate::update::{get_file_locs, read_xml_change, ChangeBlock};

use channelled_callbacks::{CallFinish, Callback, CallbackMerge, MergeTimings};
use crate::pbfformat::{
    make_convert_minimal_block, make_convert_primitive_block,
    make_read_minimal_blocks_combine_call_all, make_read_primitive_blocks_combine_call_all,
};
use crate::utils::ThreadTimer;

use crate::elements::{
    Bbox, Changetype, MinimalBlock, MinimalNode, MinimalRelation, MinimalWay, Node, PrimitiveBlock,
    Relation, Way,
};
use std::io::BufReader;
use std::io::{Error, ErrorKind, Result};

use simple_protocolbuffers::read_delta_packed_int;

use crate::utils::timestamp_string;

use crate::message;

use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug)]
pub struct NodeCount {
    pub num: i64,
    pub min_id: i64,
    pub max_id: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_lon: i32,
    pub min_lat: i32,
    pub max_lon: i32,
    pub max_lat: i32,
}
impl NodeCount {
    pub fn new() -> NodeCount {
        NodeCount {
            num: 0,
            min_id: -1,
            max_id: -1,
            min_ts: -1,
            max_ts: -1,
            min_lon: 1800000000,
            min_lat: 900000000,
            max_lon: -1800000000,
            max_lat: -900000000,
        }
    }
    pub fn add(&mut self, nd: &Node) {
        self.num += 1;
        if self.min_id == -1 || nd.id < self.min_id {
            self.min_id = nd.id;
        }
        if self.max_id == -1 || nd.id > self.max_id {
            self.max_id = nd.id;
        }

        match &nd.info {
            Some(info) => {
                if self.min_ts == -1 || info.timestamp < self.min_ts {
                    self.min_ts = info.timestamp;
                }
                if self.max_ts == -1 || info.timestamp > self.max_ts {
                    self.max_ts = info.timestamp;
                }
            }
            None => {}
        }

        if nd.lon < self.min_lon {
            self.min_lon = nd.lon;
        }
        if nd.lon > self.max_lon {
            self.max_lon = nd.lon;
        }
        if nd.lat < self.min_lat {
            self.min_lat = nd.lat;
        }
        if nd.lat > self.max_lat {
            self.max_lat = nd.lat;
        }
    }

    pub fn add_minimal(&mut self, nd: &MinimalNode) {
        self.num += 1;
        if self.min_id == -1 || nd.id < self.min_id {
            self.min_id = nd.id;
        }
        if self.max_id == -1 || nd.id > self.max_id {
            self.max_id = nd.id;
        }

        if self.min_ts == -1 || nd.timestamp < self.min_ts {
            self.min_ts = nd.timestamp;
        }
        if self.max_ts == -1 || nd.timestamp > self.max_ts {
            self.max_ts = nd.timestamp;
        }

        if nd.lon < self.min_lon {
            self.min_lon = nd.lon;
        }
        if nd.lon > self.max_lon {
            self.max_lon = nd.lon;
        }
        if nd.lat < self.min_lat {
            self.min_lat = nd.lat;
        }
        if nd.lat > self.max_lat {
            self.max_lat = nd.lat;
        }
    }

    pub fn add_other(&mut self, nc: &NodeCount) {
        if nc.min_id == -1 {
            return;
        }

        self.num += nc.num;
        if self.min_id == -1 || nc.min_id < self.min_id {
            self.min_id = nc.min_id;
        }
        if self.max_id == -1 || nc.max_id > self.max_id {
            self.max_id = nc.max_id;
        }

        if self.min_ts == -1 || nc.min_ts < self.min_ts {
            self.min_ts = nc.min_ts;
        }
        if self.max_ts == -1 || nc.max_ts > self.max_ts {
            self.max_ts = nc.max_ts;
        }

        if nc.min_lon < self.min_lon {
            self.min_lon = nc.min_lon;
        }
        if nc.max_lon > self.max_lon {
            self.max_lon = nc.max_lon;
        }
        if nc.min_lat < self.min_lat {
            self.min_lat = nc.min_lat;
        }
        if nc.max_lat > self.max_lat {
            self.max_lat = nc.max_lat;
        }
    }
}

impl fmt::Display for NodeCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{{:-10}, {:-10}, {:-10}, {:-10}}}",
            self.num,
            self.min_id,
            self.max_id,
            timestamp_string(self.min_ts),
            timestamp_string(self.max_ts),
            self.min_lon,
            self.min_lat,
            self.max_lon,
            self.max_lat
        )
    }
}

#[derive(Debug)]
pub struct WayCount {
    pub num: i64,
    pub min_id: i64,
    pub max_id: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub num_refs: i64,
    pub max_refs_len: i64,
    pub min_ref: i64,
    pub max_ref: i64,
}

impl WayCount {
    pub fn new() -> WayCount {
        WayCount {
            num: 0,
            min_id: -1,
            max_id: -1,
            min_ts: -1,
            max_ts: -1,
            num_refs: 0,
            max_refs_len: -1,
            min_ref: -1,
            max_ref: -1,
        }
    }
    pub fn add(&mut self, wy: &Way) {
        self.num += 1;
        if self.min_id == -1 || wy.id < self.min_id {
            self.min_id = wy.id;
        }
        if self.max_id == -1 || wy.id > self.max_id {
            self.max_id = wy.id;
        }

        match &wy.info {
            Some(info) => {
                if self.min_ts == -1 || info.timestamp < self.min_ts {
                    self.min_ts = info.timestamp;
                }
                if self.max_ts == -1 || info.timestamp > self.max_ts {
                    self.max_ts = info.timestamp;
                }
            }
            None => {}
        }

        self.num_refs += wy.refs.len() as i64;
        if self.max_refs_len == -1 || wy.refs.len() as i64 > self.max_refs_len {
            self.max_refs_len = wy.refs.len() as i64;
        }

        for r in &wy.refs {
            if self.min_ref == -1 || *r < self.min_id {
                self.min_ref = *r;
            }
            if self.max_ref == -1 || *r > self.max_id {
                self.max_ref = *r;
            }
        }
    }

    pub fn add_minimal(&mut self, wy: &MinimalWay) {
        self.num += 1;
        if self.min_id == -1 || wy.id < self.min_id {
            self.min_id = wy.id;
        }
        if self.max_id == -1 || wy.id > self.max_id {
            self.max_id = wy.id;
        }

        if self.min_ts == -1 || wy.timestamp < self.min_ts {
            self.min_ts = wy.timestamp;
        }
        if self.max_ts == -1 || wy.timestamp > self.max_ts {
            self.max_ts = wy.timestamp;
        }

        let refs = read_delta_packed_int(&wy.refs_data);

        self.num_refs += refs.len() as i64;
        if self.max_refs_len == -1 || refs.len() as i64 > self.max_refs_len {
            self.max_refs_len = refs.len() as i64;
        }

        for r in &refs {
            if self.min_ref == -1 || *r < self.min_id {
                self.min_ref = *r;
            }
            if self.max_ref == -1 || *r > self.max_id {
                self.max_ref = *r;
            }
        }
    }

    pub fn add_other(&mut self, wc: &WayCount) {
        if wc.min_id == -1 {
            return;
        }

        self.num += wc.num;

        if self.min_id == -1 || wc.min_id < self.min_id {
            self.min_id = wc.min_id;
        }
        if self.max_id == -1 || wc.max_id > self.max_id {
            self.max_id = wc.max_id;
        }

        if self.min_ts == -1 || wc.min_ts < self.min_ts {
            self.min_ts = wc.min_ts;
        }
        if self.max_ts == -1 || wc.max_ts > self.max_ts {
            self.max_ts = wc.max_ts;
        }

        self.num_refs += wc.num_refs;
        if self.max_refs_len == -1 || wc.max_refs_len > self.max_refs_len {
            self.max_refs_len = wc.max_refs_len;
        }

        if self.min_ref == -1 || wc.min_ref < self.min_ref {
            self.min_ref = wc.min_ref;
        }
        if self.max_ref == -1 || wc.max_ref > self.max_ref {
            self.max_ref = wc.max_ref;
        }
    }
}

impl fmt::Display for WayCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{{} refs, {} to {}. Longest: {}}}",
            self.num,
            self.min_id,
            self.max_id,
            timestamp_string(self.min_ts),
            timestamp_string(self.max_ts),
            self.num_refs,
            self.min_ref,
            self.max_ref,
            self.max_refs_len
        )
    }
}

#[derive(Debug)]
pub struct RelationCount {
    pub num: i64,
    pub min_id: i64,
    pub max_id: i64,
    pub min_ts: i64,
    pub max_ts: i64,
    pub num_empties: i64,
    pub num_mems: i64,
    pub max_mems_len: i64,
}
impl RelationCount {
    pub fn new() -> RelationCount {
        RelationCount {
            num: 0,
            min_id: -1,
            max_id: -1,
            min_ts: -1,
            max_ts: -1,
            num_empties: 0,
            num_mems: 0,
            max_mems_len: 0,
        }
    }
    pub fn add(&mut self, rl: &Relation) {
        self.num += 1;
        if self.min_id == -1 || rl.id < self.min_id {
            self.min_id = rl.id;
        }
        if self.max_id == -1 || rl.id > self.max_id {
            self.max_id = rl.id;
        }

        match &rl.info {
            Some(info) => {
                if self.min_ts == -1 || info.timestamp < self.min_ts {
                    self.min_ts = info.timestamp;
                }
                if self.max_ts == -1 || info.timestamp > self.max_ts {
                    self.max_ts = info.timestamp;
                }
            }
            None => {}
        }

        if rl.members.len() == 0 {
            self.num_empties += 1;
        }
        self.num_mems += rl.members.len() as i64;
        if self.max_mems_len == -1 || rl.members.len() as i64 > self.max_mems_len {
            self.max_mems_len = rl.members.len() as i64;
        }
    }

    pub fn add_minimal(&mut self, rl: &MinimalRelation) {
        self.num += 1;
        if self.min_id == -1 || rl.id < self.min_id {
            self.min_id = rl.id;
        }
        if self.max_id == -1 || rl.id > self.max_id {
            self.max_id = rl.id;
        }

        if self.min_ts == -1 || rl.timestamp < self.min_ts {
            self.min_ts = rl.timestamp;
        }
        if self.max_ts == -1 || rl.timestamp > self.max_ts {
            self.max_ts = rl.timestamp;
        }

        //if rl.refs_data.len() == 0 { self.num_empties += 1; }
        let nr = read_delta_packed_int(&rl.refs_data).len() as i64;
        self.num_mems += nr;
        if nr == 0 {
            self.num_empties += 1;
        }
        if self.max_mems_len == -1 || nr > self.max_mems_len {
            self.max_mems_len = nr as i64;
        }
    }

    pub fn add_other(&mut self, rc: &RelationCount) {
        if rc.min_id == -1 {
            return;
        }
        self.num += rc.num;

        if self.min_id == -1 || rc.min_id < self.min_id {
            self.min_id = rc.min_id;
        }
        if self.max_id == -1 || rc.max_id > self.max_id {
            self.max_id = rc.max_id;
        }

        if self.min_ts == -1 || rc.min_ts < self.min_ts {
            self.min_ts = rc.min_ts;
        }
        if self.max_ts == -1 || rc.max_ts > self.max_ts {
            self.max_ts = rc.max_ts;
        }

        self.num_empties += rc.num_empties;
        self.num_mems += rc.num_mems;
        if self.max_mems_len == -1 || rc.max_mems_len > self.max_mems_len {
            self.max_mems_len = rc.max_mems_len;
        }
    }
}

impl fmt::Display for RelationCount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:10} objects: {:12} => {:12} [{:19} => {:19}] {{Longest: {}, {} empties.}}",
            self.num,
            self.min_id,
            self.max_id,
            timestamp_string(self.min_ts),
            timestamp_string(self.max_ts),
            self.max_mems_len,
            self.num_empties
        )
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
        Count {
            node: NodeCount::new(),
            way: WayCount::new(),
            relation: RelationCount::new(),
        }
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
        write!(
            f,
            "node:      {}\nway:       {}\nrelations: {}",
            self.node, self.way, self.relation
        )
    }
}

#[derive(Debug)]
pub struct CountChange {
    pub node: BTreeMap<Changetype, NodeCount>,
    pub way: BTreeMap<Changetype, WayCount>,
    pub relation: BTreeMap<Changetype, RelationCount>,
}
impl CountChange {
    pub fn new() -> CountChange {
        CountChange {
            node: BTreeMap::new(),
            way: BTreeMap::new(),
            relation: BTreeMap::new(),
        }
    }
    pub fn add_changeblock(&mut self, bl: &ChangeBlock) {
        for (_, nd) in &bl.nodes {
            if !self.node.contains_key(&nd.changetype) {
                self.node.insert(nd.changetype, NodeCount::new());
            }
            self.node.get_mut(&nd.changetype).unwrap().add(&nd);
        }
        for (_, wy) in &bl.ways {
            if !self.way.contains_key(&wy.changetype) {
                self.way.insert(wy.changetype, WayCount::new());
            }
            self.way.get_mut(&wy.changetype).unwrap().add(&wy);
        }
        for (_, rl) in &bl.relations {
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
            let ct = nd.changetype;

            if !self.node.contains_key(&ct) {
                self.node.insert(ct, NodeCount::new());
            }
            self.node.get_mut(&ct).unwrap().add_minimal(&nd);
        }
        for wy in &bl.ways {
            let ct = wy.changetype;
            if !self.way.contains_key(&ct) {
                self.way.insert(ct, WayCount::new());
            }
            self.way.get_mut(&ct).unwrap().add_minimal(&wy);
        }
        for rl in &bl.relations {
            let ct = rl.changetype;
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
        for (a, b) in &self.node {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f, "\nways:")?;
        for (a, b) in &self.way {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f, "\nrelations:")?;
        for (a, b) in &self.relation {
            write!(f, "\n  {:10} {}", a.to_string(), b)?;
        }
        write!(f, "")
    }
}

//use cpuprofiler::PROFILER;

struct CountChangeMinimal {
    cc: Option<CountChange>,
    tm: f64,
}

impl CountChangeMinimal {
    pub fn new() -> CountChangeMinimal {
        CountChangeMinimal {
            cc: Some(CountChange::new()),
            tm: 0.0,
        }
    }
}

impl CallFinish for CountChangeMinimal {
    type CallType = MinimalBlock;
    type ReturnType = channelled_callbacks::Timings<CountChange>;

    fn call(&mut self, bl: MinimalBlock) {
        let tx = ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Self::ReturnType::new();
        tm.add("countchange", self.tm);
        tm.add_other("countchange", self.cc.take().unwrap());
        Ok(tm)
    }
}

struct CountPrim {
    cc: Option<Count>,
    tm: f64,
}

impl CountPrim {
    pub fn new() -> CountPrim {
        CountPrim {
            cc: Some(Count::new()),
            tm: 0.0,
        }
    }
}

impl CallFinish for CountPrim {
    type CallType = PrimitiveBlock;
    type ReturnType = channelled_callbacks::Timings<Count>;

    fn call(&mut self, bl: PrimitiveBlock) {
        let tx = ThreadTimer::new();
        self.cc.as_mut().unwrap().add_primitive(&bl);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Self::ReturnType::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}

struct CountMinimal {
    cc: Option<Count>,
    tm: f64,
}

impl CountMinimal {
    pub fn new() -> CountMinimal {
        CountMinimal {
            cc: Some(Count::new()),
            tm: 0.0,
        }
    }
}

impl CallFinish for CountMinimal {
    type CallType = MinimalBlock;
    type ReturnType = channelled_callbacks::Timings<Count>;

    fn call(&mut self, bl: MinimalBlock) {
        let tx = ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Self::ReturnType::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}

pub enum CountAny {
    Count(Count),
    CountChange(CountChange),
}

impl fmt::Display for CountAny {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CountAny::Count(c) => c.fmt(f),
            CountAny::CountChange(c) => c.fmt(f)
        }
    }
}       
        
    
pub fn call_count(fname: &str,
    use_primitive: bool,
    numchan: usize,
    filter_in: Option<&str>,
) -> Result<CountAny> {
    
    let filter = match filter_in {
        None => None,
        Some(s) => Some(Bbox::from_str(s)?),
    };

    let f = File::open(fname).expect("file not present");

    if fname.ends_with(".osc") {
        let mut cn = CountChange::new();
        let mut fbuf = BufReader::with_capacity(1024 * 1024, f);
        let data = read_xml_change(&mut fbuf).expect("failed to read osc");

        cn.add_changeblock(&data);
        Ok(CountAny::CountChange(cn))
    } else if fname.ends_with(".osc.gz") {
        let mut cn = CountChange::new();
        let fbuf = BufReader::with_capacity(1024 * 1024, f);
        let mut gzbuf = BufReader::new(flate2::bufread::GzDecoder::new(fbuf));
        //Box::new(gzbuf) as Box<dyn std::io::BufRead>
        let data = read_xml_change(&mut gzbuf).expect("failed to read osc");

        cn.add_changeblock(&data);
        Ok(CountAny::CountChange(cn))
    } else if fname.ends_with(".pbfc") {
        
        let pg = crate::logging::messenger().start_progress_bytes(&format!("count change blocks minimal {}, numchan=1", fname), file_length(fname));
        
        let mut fbuf = BufReader::new(f);
        let cc = Box::new(CountChangeMinimal::new());
        let cn = Box::new(Callback::new(make_convert_minimal_block(true, cc)));
        let (mut a, _) = read_all_blocks_prog_fpos(&mut fbuf, cn, pg);
        
        let cn = std::mem::take(&mut a.others).pop().unwrap().1;

        Ok(CountAny::CountChange(cn))
    //message!("{:?}", cn.relation.get(&Changetype::Create));
    } else if std::fs::metadata(fname)
        .expect("failed to open file")
        .is_file()
        && filter.is_none()
    {
        let mut cc = Count::new();

        
        let pg = crate::logging::messenger().start_progress_bytes(
                &format!(
                        "count blocks {} {}, numchan={}",
                        (if use_primitive { "primitive" } else { "minimal"} ), fname, numchan
                    ),file_length(fname));

        if numchan == 0 {
            let mut fbuf = BufReader::new(f);

            let (a, _) = if use_primitive {
                
                let cm = Box::new(CountPrim::new());
                let cc = make_convert_primitive_block(false, cm);
                read_all_blocks_prog_fpos(&mut fbuf, cc, pg)
            } else {
                
                crate::logging::messenger().start_progress_bytes(&format!("count blocks minimal {}, numchan=0", fname), file_length(fname));
                let cm = Box::new(CountMinimal::new());
                let cc = make_convert_minimal_block(false, cm);
                read_all_blocks_prog_fpos(&mut fbuf, cc, pg)
            };
            //pb.finish();
            cc.add_other(&a.others[0].1);

        //cc = count_all(cc, read_file_block::ReadFileBlocks::new(&mut fbuf), 0, fname, minimal, false);
        } else if numchan > 8 {
            return Err(Error::new(
                ErrorKind::Other,
                "numchan must be between 0 and 8",
            ));
        } else {
            
            
            let mut fbuf = f;

            let mut ccs: Vec<
                Box<
                    dyn CallFinish<
                        CallType = (usize, FileBlock),
                        ReturnType = channelled_callbacks::Timings<Count>,
                    >,
                >,
            > = Vec::new();
            for _ in 0..numchan {
                if use_primitive {
                    
                    let cm = Box::new(CountPrim::new());
                    ccs.push(Box::new(Callback::new(make_convert_primitive_block(
                        false, cm,
                    ))));
                } else {
                    
                    let cm = Box::new(CountMinimal::new());
                    ccs.push(Box::new(Callback::new(make_convert_minimal_block(
                        false, cm,
                    ))));
                }
            }
            let cm = Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new())));
            let (a, _) = read_all_blocks_prog_fpos(&mut fbuf, cm, pg);
            //pb.finish();
            for (_, x) in a.others {
                cc.add_other(&x);
            }
        }
        Ok(CountAny::Count(cc))
    } else {
        let (mut fbufs, locsv, total_len) = get_file_locs(fname, filter, None).expect("?");

        let mut pps: Vec<
            Box<
                dyn CallFinish<
                    CallType = (usize, Vec<FileBlock>),
                    ReturnType = channelled_callbacks::Timings<Count>,
                >,
            >,
        > = Vec::new();
        let msg: String;
        if use_primitive {
            msg = format!(
                "count blocks combine primitive {}, numchan={}",
                fname, numchan
            );
            for _ in 0..numchan {
                let cca = Box::new(CountPrim::new());
                pps.push(Box::new(Callback::new(
                    make_read_primitive_blocks_combine_call_all(cca),
                )));
            }
        } else {
            msg = format!(
                "count blocks combine minimal {}, numchan={}",
                fname, numchan
            );
            for _ in 0..numchan {
                let cca = Box::new(CountMinimal::new());
                pps.push(Box::new(Callback::new(
                    make_read_minimal_blocks_combine_call_all(cca),
                )));
            }
        }
        let readb = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
        let a = read_all_blocks_parallel_with_progbar(&mut fbufs, &locsv, readb, &msg, total_len);

        let mut cc = Count::new();
        for (_, y) in &a.others {
            cc.add_other(y);
        }

        Ok(CountAny::Count(cc))
    }
    
}

    

pub fn run_count(
    fname: &str,
    use_primitive: bool,
    numchan: usize,
    filter_in: Option<&str>,
) -> Result<()> {
    
    
    //crate::logging::messenger().message(&format!("{}", call_count(fname,use_primitive, numchan, filter_in)?));
    message!("{}", call_count(fname,use_primitive, numchan, filter_in)?);
    Ok(())
}
    
    
