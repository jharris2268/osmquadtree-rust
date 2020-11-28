extern crate osmquadtree;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::{write_index_file,FilelistEntry, read_filelist, write_filelist,read_xml_change,check_index_file,ChangeBlock};
use osmquadtree::utils::{parse_timestamp,timestamp_string,Timings, date_string,MergeTimings,Timer,ThreadTimer};
use osmquadtree::callback::{Callback,CallbackMerge,CallFinish};
use osmquadtree::header_block;
use osmquadtree::elements::{IdSet,Quadtree,PrimitiveBlock,Changetype,Node,Way,Relation,ElementType,Bbox};
use osmquadtree::read_file_block;
use osmquadtree::sortblocks::{QuadtreeTree,WriteFileInternalLocs};
//use osmquadtree::convertblocks;
use std::env;
use std::fs::File;
use serde::{Deserialize,Serialize};
use std::collections::{BTreeSet,BTreeMap};
use std::io::{Error,ErrorKind,BufReader};//,Write,stdout};
use std::sync::Arc;
//use std::marker::PhantomData;
use std::fmt;

#[derive(Debug,Deserialize,Serialize)]
#[serde(rename_all = "PascalCase")]
struct Settings {
    pub initial_state: i64,
    pub diffs_location: String,
    pub source_prfx: String,
    pub round_time: bool
}


impl Settings {
    pub fn new(initial_state: i64, diffs_location: &str) -> Settings {
        Settings{
            initial_state: initial_state,
            diffs_location: String::from(diffs_location),
            source_prfx: String::from("https://planet.openstreetmap.org/replication/day/"),
            round_time: true
        }
    }
    
    pub fn from_file(prfx: &str) -> Settings {
        let ff = File::open(format!("{}settings.json", prfx)).expect("failed to open settings file");
        serde_json::from_reader(ff).expect("failed to parse json")
    }
    
    pub fn write(&self, prfx: &str) {
        let ff = File::create(format!("{}settings.json", prfx)).expect("failed to create settings file");
        serde_json::to_writer(ff, self).expect("failed to write json");
    }
        
}



fn check_state(settings: &Settings, filelist: &Vec<FilelistEntry>) -> (Vec<(String,i64,i64)>,i64) {
    let mut res = Vec::new();
    if filelist.is_empty() {
        panic!("empty filelist");
    }
    let last_state = filelist.last().unwrap().state;
    let prev_ts = parse_timestamp(&filelist.last().unwrap().end_date).expect("?");
    let state_ff = File::open(format!("{}state.csv", settings.diffs_location)).expect("failed to open state.csv file");
    for row in csv::Reader::from_reader(state_ff).records() {
        let row = row.expect("?");
        
        if row.len()==2 {
            let state: i64 = row[0].parse().unwrap();
            
            if state > last_state {
                let timestamp = parse_timestamp(&row[1]).expect("?");
                let fname = format!("{}{}.osc.gz", settings.diffs_location, state);
                res.push((fname, state,timestamp));
            }
        }
    }
    (res,prev_ts)
}

pub struct OrigData {
    pub node_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub way_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub relation_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub othernodes: BTreeMap<i64,Option<Node>>
}
impl OrigData {
    pub fn new() -> OrigData {
        OrigData{node_qts: BTreeMap::new(),way_qts: BTreeMap::new(),relation_qts: BTreeMap::new(),othernodes: BTreeMap::new()}
    }
    
    pub fn add(&mut self, pb: PrimitiveBlock, idset: &IdSet) {
        for n in pb.nodes {
            
            
            match n.changetype {
                
                Changetype::Normal | Changetype::Unchanged | Changetype::Modify | Changetype::Create => {
                    self.node_qts.insert(n.id,(n.quadtree,pb.quadtree));
                    if idset.is_exnode(n.id) {
                        let mut n = n.clone();
                        n.changetype=Changetype::Normal;
                        self.othernodes.insert(n.id,Some(n));
                    }
                },
                Changetype::Delete => {
                    self.node_qts.insert(n.id, (Quadtree::empty(), Quadtree::empty()));
                    if idset.is_exnode(n.id) {
                        self.othernodes.insert(n.id,None);
                    }
                },
                _ => {}
            }
            
            
        }
        for w in pb.ways {
            match w.changetype {
                Changetype::Normal | Changetype::Unchanged | Changetype::Modify | Changetype::Create => {
                    self.way_qts.insert(w.id,(w.quadtree,pb.quadtree));
                },
                Changetype::Delete => {
                    self.way_qts.insert(w.id, (Quadtree::empty(), Quadtree::empty()));
                    
                },
                _ => {}
            }
        }
        
        for r in pb.relations {
            match r.changetype {
                Changetype::Normal | Changetype::Unchanged | Changetype::Modify | Changetype::Create => {
                    self.relation_qts.insert(r.id,(r.quadtree,pb.quadtree));
                },
                Changetype::Delete => {
                    self.relation_qts.insert(r.id, (Quadtree::empty(), Quadtree::empty()));
                    
                },                    
                _ => {}
            }
        }
    }
    
    pub fn extend(&mut self, other: OrigData) {
        self.node_qts.extend(other.node_qts);
        self.way_qts.extend(other.way_qts);
        self.relation_qts.extend(other.relation_qts);
        self.othernodes.extend(other.othernodes);
    }
    
    fn get_quadtree(&self, t: &ElementType, r: &i64) -> Option<Quadtree> {
        match *t {
            ElementType::Node => get_quadtree(&self.node_qts, r),
            ElementType::Way => get_quadtree(&self.way_qts, r),
            ElementType::Relation => get_quadtree(&self.relation_qts, r),
        }
    }
    
    fn get_alloc(&self, t: &ElementType, r: &i64) -> Option<Quadtree> {
        match t {
            ElementType::Node => get_alloc(&self.node_qts, r),
            ElementType::Way => get_alloc(&self.way_qts, r),
            ElementType::Relation => get_alloc(&self.relation_qts, r),
        }
    }
    
    fn expand_node(&mut self, r: &i64, q: Quadtree) {
        expand_quadtree(&mut self.node_qts, r, q);
    }
    
    /*fn set_node(&mut self, r: &i64, q: Quadtree) {
        replace_quadtree(&mut self.node_qts, r, q);
    }*/
    fn set_way(&mut self, r: &i64, q: Quadtree)  {
        replace_quadtree(&mut self.way_qts, r, q);
    }
    
    fn expand_relation(&mut self, r: &i64, q: Quadtree)  {
        expand_quadtree(&mut self.relation_qts, r, q);
    }
    
    fn set_relation(&mut self, r: &i64, q: Quadtree)  {
        replace_quadtree(&mut self.relation_qts, r, q);
    }
    
    
    
}

fn expand_quadtree(curr: &mut BTreeMap<i64, (Quadtree,Quadtree)>, i: &i64, q: Quadtree) {
    match curr.get_mut(i) {
        None => {curr.insert(*i,(q,Quadtree::empty())); },
        Some(p) => {p.0 = p.0.common(&q); },
    }
}
fn replace_quadtree(curr: &mut BTreeMap<i64,(Quadtree,Quadtree)>, i: &i64, q: Quadtree) {
    match curr.get_mut(i) {
        None => {curr.insert(*i,(q,Quadtree::empty())); },
        Some(p) => {p.0 = q; },
    }
}
fn get_quadtree(curr: &BTreeMap<i64, (Quadtree,Quadtree)>, i: &i64) -> Option<Quadtree>{
    match curr.get(i) {
        Some(q) => {
            if q.0.as_int() < 0 {
                None
            } else {
                Some(q.0)
            }
        },
        None => None
    }
}
fn get_alloc(curr: &BTreeMap<i64, (Quadtree,Quadtree)>, i: &i64) -> Option<Quadtree> {
    match curr.get(i) {
        Some(q) => {
            if q.1.as_int() < 0 {
                None
            } else {
                Some(q.1)
            }
        },
        None => None
    }
}


impl fmt::Display for OrigData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OrigData {:10} node qts, {:8} way qts, {:7} rel qts, {:8} other nodes", self.node_qts.len(), self.way_qts.len(), self.relation_qts.len(), self.othernodes.len())
    }
}

struct ReadPB {
    
    origdata: Option<OrigData>,
    
    ids: Arc<IdSet>,
    ischange:bool,
    
    tm: f64
}
impl ReadPB {
    pub fn new(ischange: bool, ids: Arc<IdSet>) -> ReadPB {
        ReadPB{origdata: Some(OrigData::new()),ids:ids,ischange: ischange, tm:0.0}
    }
}


impl CallFinish for ReadPB {
    type CallType = (usize, read_file_block::FileBlock);
    type ReturnType = Timings<OrigData>;
    
    fn call(&mut self, idx_blocks: (usize, read_file_block::FileBlock)) {
        let tx=ThreadTimer::new();
        let b = PrimitiveBlock::read_check_ids(idx_blocks.0 as i64, idx_blocks.1.pos, &idx_blocks.1.data(),self.ischange, false,Some(self.ids.as_ref())).expect("?");
        
        self.origdata.as_mut().unwrap().add(b,self.ids.as_ref());
        
        self.tm+=tx.since();
        
    }
    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Timings::new();//self.out.finish()?;
        tm.add("read_primitive_blocks_combine", self.tm);
        tm.add_other("origdata", self.origdata.take().unwrap());
        Ok(tm)
    }
}




fn read_change_tiles(fname: &str, tiles: &BTreeSet<Quadtree>, idset: Arc<IdSet>, numchan: usize, pb: Option<&read_file_block::ProgBarWrap>) -> std::io::Result<(OrigData,f64)> {
    let ischange = fname.ends_with(".pbfc");
    let mut file = File::open(fname)?;
    let (p,fb) = read_file_block::read_file_block_with_pos(&mut file, 0)?;
    if fb.block_type != "OSMHeader" {
        return Err(Error::new(ErrorKind::Other, "first block not an OSMHeader"));
    }
    let head = header_block::HeaderBlock::read(p, &fb.data(), fname)?;
    if head.index.is_empty() {
        return Err(Error::new(ErrorKind::Other, "no locs in header"));
    }
    let mut locs = Vec::new();
    
    for ii in &head.index {
        if tiles.contains(&ii.quadtree) {
            locs.push(ii.location);
        }
    }
    let (mut tm,b) = if numchan == 0 {
        
        let convert = Box::new(ReadPB::new(ischange,idset));
        match pb {
            None => read_file_block::read_all_blocks_locs(&mut file, fname, locs, false, convert),
            Some(pb) => read_file_block::read_all_blocks_locs_prog(&mut file, fname, locs, convert, pb),
        }
    } else {
        
        let mut convs: Vec<Box<dyn CallFinish<CallType=(usize,read_file_block::FileBlock),ReturnType=Timings<OrigData>>>> = Vec::new();
        for _ in 0..numchan {
            convs.push(Box::new(Callback::new(Box::new(ReadPB::new(ischange,idset.clone())))));
        }
        let convsm = Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())));
        match pb {
            None => read_file_block::read_all_blocks_locs(&mut file, fname, locs, true, convsm),
            Some(pb) => read_file_block::read_all_blocks_locs_prog(&mut file, fname, locs, convsm, pb),
        }
    };
    
    let mut tls = tm.others.pop().unwrap().1;
    while !tm.others.is_empty() {
        tls.extend(tm.others.pop().unwrap().1);
    }
    Ok((tls,b))
}
    
    
    

fn collect_existing(
    prfx: &str, filelist: &Vec<FilelistEntry>,
    idset: Arc<IdSet>,
    numchan: usize) -> std::io::Result<(OrigData,f64,f64)> {
        
    
    let mut origdata = OrigData::new();
    let mut total_scan=0.0;
    let mut total_read=0.0;
    
    let mut pb = read_file_block::ProgBarWrap::new(147+348 + (filelist.len() as u64-1)*2);
    
    
    
    for (i,fle) in filelist.iter().enumerate() {
        let nc = if i==0 { numchan} else { 1 };
        
        
        let fnameidx = format!("{}{}-index.pbf", prfx, fle.filename);
        
        if i==0 {
            pb.set_range(147);
        } else {
            pb.set_range(1);
        }
        pb.set_message(&format!("check_index {}", fnameidx));
        
        
        let (a,c) = check_index_file(&fnameidx, idset.clone(), nc, Some(&pb))?;
        total_scan+=c;

        let mut ctiles = BTreeSet::new();
        ctiles.extend(a);
     
        let fname = format!("{}{}", prfx, fle.filename);
        if i==0 {
            pb.set_range(388);
        } else {
            pb.set_range(1);
        }
        pb.set_message(&format!("{}: {} tiles", &fname,ctiles.len()));
        
        let (bb,t) = read_change_tiles(&fname, &ctiles, idset.clone(), nc, Some(&pb))?;
        total_read+=t;
        origdata.extend(bb);
    }
    pb.finish();
    Ok((origdata,total_scan,total_read))
    
}


fn find_tile(tree: &QuadtreeTree, q: Option<Quadtree>) -> Option<Quadtree> {
    match q {
        None => None,
        Some(q) => {
            let (_,t) = tree.find(q);
            Some(t.qt)
        }
    }
}

fn find_node<'a>(changeblock: &'a ChangeBlock, id: &i64) -> std::io::Result<&'a Node> {
    match changeblock.nodes.get(id) {
        Some(n) => { 
            if n.changetype == Changetype::Delete {
                Err(Error::new(ErrorKind::Other,format!("node {} deleted", id)))
            } else {
                Ok(n)
            }
        },
        None => Err(Error::new(ErrorKind::Other,format!("node {} missing", id))),
    }
    
}


struct AllocBlocks {
    st: i64,
    et: i64,
    pub blocks: BTreeMap<Quadtree,PrimitiveBlock>
}

impl AllocBlocks {
    pub fn new(st: i64, et: i64) -> AllocBlocks {
        AllocBlocks{st:st,et:et,blocks:BTreeMap::new()}
    }

    fn add_node(&mut self, qt: Quadtree, nd: Node) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0,0);
                pb.quadtree=qt.clone();
                pb.start_date=self.st;
                pb.end_date=self.et;
                pb.nodes.push(nd);
                self.blocks.insert(qt, pb);
            },
            Some(pb) => { pb.nodes.push(nd); }
        }
    }
    fn add_way(&mut self, qt: Quadtree, wy: Way) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0,0);
                pb.quadtree=qt.clone();
                pb.start_date=self.st;
                pb.end_date=self.et;
                pb.ways.push(wy);
                self.blocks.insert(qt, pb);
            },
            Some(pb) => { pb.ways.push(wy); }
        }
    }
    fn add_relation(&mut self, qt: Quadtree, rl: Relation) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0,0);
                pb.quadtree=qt.clone();
                pb.start_date=self.st;
                pb.end_date=self.et;
                pb.relations.push(rl);
                self.blocks.insert(qt, pb);
            },
            Some(pb) => { pb.relations.push(rl); }
        }
    }
}

fn calc_qts(changeblock: &ChangeBlock, orig_data: &mut OrigData, tree: &QuadtreeTree, maxlevel: usize, buffer: f64, st: i64, et: i64) -> std::io::Result<BTreeMap<Quadtree, PrimitiveBlock>> {
    
    let mut nq = BTreeSet::new();
    let mut rel_rels = Vec::new();
    
    for (_,w) in changeblock.ways.iter() {
        if w.changetype != Changetype::Delete {
            let mut bbox = Bbox::empty();
            for r in w.refs.iter() {
                let n = find_node(changeblock, r)?;
                bbox.expand(n.lon, n.lat);
            }
            
            let q = Quadtree::calculate(&bbox, maxlevel, buffer);
            orig_data.set_way(&w.id, q);
            
            for r in w.refs.iter() {
                orig_data.expand_node(r, q);
                nq.insert(r);
            }
        }
    }
   
    for (_,n) in changeblock.nodes.iter() {
        if n.changetype != Changetype::Delete {
            if !nq.contains(&n.id) {
                let q = Quadtree::calculate_point(n.lon, n.lat, maxlevel, buffer);
                orig_data.expand_node(&n.id, q);
                   
            }
        }
    }
    
    
    for (_,r) in changeblock.relations.iter() {
        if r.changetype != Changetype::Delete {
            
            if r.members.is_empty() {
                orig_data.set_relation(&r.id, Quadtree::new(0));
            } else {
                
                let mut qt = Quadtree::empty();
                for m in r.members.iter() {
                    if m.mem_type == ElementType::Relation {
                        rel_rels.push((r.id, m.mem_ref));
                    } else {
                        match orig_data.get_quadtree(&m.mem_type, &m.mem_ref) {
                            Some(q) => { qt = qt.common(&q);},
                            None => { println!("missing member {:?} {} for relation {}", m.mem_type, m.mem_ref, r.id); }
                        }
                    }
                }
                
                orig_data.expand_relation(&r.id, qt);
            }
        }
    }
    
    for i in 0..5 {
        for (a,b) in rel_rels.iter() {
            match orig_data.get_quadtree(&ElementType::Relation, b) {
                Some(q) => { orig_data.expand_relation(a, q); },
                None => {
                    if i== 4 { 
                        println!("missing member {:?} {} for relation {}", ElementType::Relation, b, a);
                    }
                },
            }
        }
    }
    
    let mut unneeded_extra_nodes = 0;
    let mut create_delete = 0;
    let mut res = AllocBlocks::new(st, et);
    
    for (_,n) in changeblock.nodes.iter() {
        
        let q = orig_data.get_quadtree(&ElementType::Node, &n.id);
        
        let na = find_tile(&tree, q);
        
        let a = orig_data.get_alloc(&ElementType::Node, &n.id);
        
       
        
        match (n.changetype, a) {
            (Changetype::Normal, Some(alloc)) => {
                if n.quadtree == q.unwrap() {
                    unneeded_extra_nodes += 1;
                } else {
                    let mut n2 = n.clone();
                    n2.quadtree = q.unwrap();
                    n2.changetype = Changetype::Unchanged;
                    res.add_node(na.unwrap(), n2);
                    
                    if na.unwrap() != alloc {
                        let mut n3 = n.clone();
                        //n3.tags.clear();
                        n3.quadtree=Quadtree::new(0);
                        n3.changetype = Changetype::Remove;
                        res.add_node(alloc, n3);
                    }
                }
            },
            (Changetype::Delete, Some(alloc)) => {
                let mut n2=n.clone();
                n2.quadtree=Quadtree::new(0);
                res.add_node(alloc, n2);
            },
            (Changetype::Delete, None) => { create_delete+=1; },
            (Changetype::Modify, Some(alloc)) => {
                let mut n2 = n.clone();
                n2.quadtree = q.unwrap();
                res.add_node(na.unwrap(), n2);
                if na.unwrap() != alloc {
                    let mut n3 = n.clone();
                    //n3.tags.clear();
                    n3.quadtree=Quadtree::new(0);
                    n3.changetype = Changetype::Remove;
                    res.add_node(alloc, n3);
                   
                }
            },
            (Changetype::Modify, None) | (Changetype::Create, None) => {
                let mut n2 = n.clone();
                n2.quadtree=q.unwrap();
                res.add_node(na.unwrap(), n2);
            },
            (rt,ra) => { println!("dont understand {} {:?} {:?} {:?} {:?}", rt,ra,n,q,na); },
        }
        
        
    }
    
    for (_,w) in changeblock.ways.iter() {
        
        let q = orig_data.get_quadtree(&ElementType::Way, &w.id);
        
        let na = find_tile(&tree, q);
        
        let a = orig_data.get_alloc(&ElementType::Way, &w.id);
        
        match (w.changetype, a) {
            
            (Changetype::Delete, Some(alloc)) => {
                let mut w2=w.clone();
                w2.quadtree=Quadtree::new(0);
                res.add_way(alloc, w2);
            },
            (Changetype::Delete, None) => { create_delete+=1; },
            (Changetype::Modify, Some(alloc)) => {
                let mut w2 = w.clone();
                w2.quadtree = q.unwrap();
                res.add_way(na.unwrap(), w2);
                if na.unwrap() != alloc {
                    let mut w3 = w.clone();
                    //w3.tags.clear();
                    w3.quadtree=Quadtree::new(0);
                    w3.changetype = Changetype::Remove;
                    res.add_way(alloc, w3);
                   
                }
            },
            (Changetype::Modify, None) | (Changetype::Create, None) => {
                let mut w2 = w.clone();
                w2.quadtree=q.unwrap();
                res.add_way(na.unwrap(), w2);
            },
            (rt,ra) => { println!("dont understand {} {:?} {:?} {:?} {:?}", rt,ra,w,q,na); },
        }
        
        
    }
    for (_,r) in changeblock.relations.iter() {
        
        let q = orig_data.get_quadtree(&ElementType::Relation, &r.id);
        
        let na = find_tile(&tree, q);
        
        let a = orig_data.get_alloc(&ElementType::Relation, &r.id);
        
        match (r.changetype, a) {
            
            (Changetype::Delete, Some(alloc)) => {
                let mut r2=r.clone();
                r2.quadtree=Quadtree::new(0);
                res.add_relation(alloc, r2);
            },
            (Changetype::Delete, None) => { create_delete+=1; },
            (Changetype::Modify, Some(alloc)) => {
                let mut r2 = r.clone();
                r2.quadtree = q.unwrap();
                res.add_relation(na.unwrap(), r2);
                if na.unwrap() != alloc {
                    let mut r3 = r.clone();
                    //r3.tags.clear();
                    r3.quadtree=Quadtree::new(0);
                    r3.changetype = Changetype::Remove;
                    res.add_relation(alloc, r3);
                   
                }
            },
            (Changetype::Modify, None) | (Changetype::Create, None) => {
                let mut r2 = r.clone();
                r2.quadtree=q.unwrap();
                res.add_relation(na.unwrap(), r2);
            },
            (rt,ra) => { println!("dont understand {} {:?} {:?} {:?} {:?}", rt,ra,r,q,na); },
        }
        
        
    }
    
    println!("{} unneeded extra nodes, {} create_delete", unneeded_extra_nodes, create_delete);
    
    Ok(res.blocks)
}

        
    

fn prep_tree(prfx: &str, filelist: &Vec<FilelistEntry>) -> std::io::Result<QuadtreeTree> {
    let fname = format!("{}{}", prfx, filelist[0].filename);
    let mut fobj = File::open(&fname)?;
    let (x,fb) = read_file_block::read_file_block_with_pos(&mut fobj, 0)?;
    if fb.block_type != "OSMHeader" {
        return Err(Error::new(ErrorKind::Other,"first block not an OSMHeader"));
    }
    let head = header_block::HeaderBlock::read(x, &fb.data(), &fname)?;
    
    let mut tree = QuadtreeTree::new();
    for ii in &head.index {
        tree.add(ii.quadtree,1);
    }
    
    Ok(tree)
}

 
fn prep_idset(changeblock: &ChangeBlock) -> IdSet {
    let mut idset = IdSet::new();
    
    for (_,n) in changeblock.nodes.iter() {
        idset.nodes.insert(n.id);
    }
    println!("{}", idset);
    for (_,w) in changeblock.ways.iter() {
        idset.ways.insert(w.id);
        for n in w.refs.iter() {
            idset.nodes.insert(*n);
            if !changeblock.nodes.contains_key(n) {
                idset.exnodes.insert(*n);
            }
        }
        
        
    }
    println!("{}", idset);
    
    for (_,r) in changeblock.relations.iter() {
        idset.relations.insert(r.id);
        for m in r.members.iter() {
            match m.mem_type {
                ElementType::Node => {
                    idset.nodes.insert(m.mem_ref);
                },
                ElementType::Way => { idset.ways.insert(m.mem_ref); },
                ElementType::Relation => { idset.relations.insert(m.mem_ref); },
            }
        }
        
    }
    println!("{}", idset);
    idset
}

fn find_update(prfx: &str, filelist: &Vec<FilelistEntry>, change_filename: &str, prev_ts: i64, ts: i64, fname:&str, numchan: usize) -> std::io::Result<(f64,usize)> {
    let mut chgf = BufReader::new(File::open(change_filename)?);
    
    let tx=Timer::new();
        
    let mut changeblock = if change_filename.ends_with(".gz") {
        read_xml_change(&mut BufReader::new(flate2::bufread::GzDecoder::new(chgf)))
    } else {
        read_xml_change(&mut chgf)
    }?;
    
    let a = tx.since();
    
    let idset=Arc::new(prep_idset(&changeblock));
   
    let b = tx.since();
    
    let (mut orig_data,_,_) = collect_existing(prfx, filelist, idset, numchan)?;
    
    let c = tx.since();
    println!("{}", orig_data);
    
    let on = std::mem::take(&mut orig_data.othernodes);
    for (a,b) in on {
        match b {
            None=>{},
            Some(n) => { changeblock.nodes.insert(a,n); }
        }
    }
    //changeblock.nodes.append(&mut orig_data.othernodes);
    
    let d = tx.since();
    
    let tree = prep_tree(prfx, filelist)?;
    let e = tx.since();
    println!("{}", tree);
    
    
    let tiles = calc_qts(&changeblock, &mut orig_data, &tree, 18, 0.05, prev_ts, ts)?;
    let f = tx.since();
    
    println!("{} tiles, {} objs", tiles.len(), tiles.iter().map(|(_,b)| { b.nodes.len() }).sum::<usize>());
    
    let mut wf = WriteFileInternalLocs::new(&format!("{}{}", prfx, fname), true);
    for (k,v) in tiles.iter() {
        let pp = v.pack(true,true)?;
        let qq = read_file_block::pack_file_block("OSMData", &pp, true)?;
        wf.call((*k,qq))
    }
    wf.finish()?;
    
    let g = tx.since();
        
    println!("read xml: {:5.1}s\nprep_idset: {:5.1}s\ncollect_existing: {:5.1}s\nextend nodes: {:5.1}s\nprep_tree: {:5.1}s\ncalc_qts: {:5.1}s\npack and write{:5.1}s\nTOTAL: {:5.1}s",a,b-a,c-b,d-c,e-d,f-e,g-f,g);
    
    Ok((tx.since(), tiles.len()))
    
}

fn main() {
    
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        panic!("must specify operation and infn")
    }
    
    let op = args[1].clone();
    let prfx = args[2].clone();
    
    
    let mut numchan = 4;
    let mut infn = String::new();
    let mut timestamp = 0;
    let mut initial_state=0;
    let mut diffs_location = String::new();
    let mut limit=0;
    
    if args.len()>3 {
        for i in 3..args.len() {
            if args[i].starts_with("infn=") {
                infn = args[i].substr(5,args[i].len());
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("timestamp=") {
                timestamp = parse_timestamp(&args[i].substr(10,args[i].len())).expect("failed to read timestamp");
            }  else if args[i].starts_with("initial_state=") {
                initial_state = args[i].substr(14,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("diffs_location=") {
                diffs_location = args[i].substr(15,args[i].len());
            } else if args[i].starts_with("limit=") {
                limit = args[i].substr(6,args[i].len()).parse().unwrap();
            } else {
                println!("unexpected argument: {}", args[i]);
            }
        }
    }
    
    
    
    if op == "initial" {
        if infn.is_empty() || timestamp == 0 || initial_state==0 || diffs_location.is_empty() {
            panic!("must specify infn, timestamp, initial_state and diffs_locations");
        }
        let outfn = format!("{}{}-index.pbf", prfx, infn);
        let infn2 = format!("{}{}", prfx, infn);
        let num_tiles = write_index_file(&infn2, &outfn, numchan);
        
        let settings = Settings::new(initial_state,&diffs_location);
        println!("{:?}", settings);
        settings.write(&prfx);
        
        write_filelist(&prfx, &vec![FilelistEntry::new(infn.clone(), timestamp_string(timestamp), num_tiles, initial_state)]);
        
        
        
    } else if op == "update" || op == "update_demo" {
        
        let settings = Settings::from_file(&prfx);
        let mut filelist = read_filelist(&prfx);
        let mut suffix = String::new();
        if op == "update_demo" {
            
            filelist.pop();
            if limit > 1 {
                for _ in 1..limit {
                    filelist.pop();
                }
            }
            suffix=String::from("-rust");
            
        }
        
        let (mut to_update, mut prev_ts) = check_state(&settings, &filelist);
        if limit > 0 && to_update.len()>limit {
            to_update = to_update[..limit].to_vec();
        }
        println!("have {} in filelist, {} to update", filelist.len(), to_update.len());
        let mut tm = Timings::<()>::new();
        if !to_update.is_empty() {
            
            for (chgfn, state, ts) in to_update {
                let fname = format!("{}{}.pbfc",date_string(ts),suffix);
                println!("call find_update('{}',{} entries,'{}', {}, {}, {}, {})", prfx,filelist.len(),chgfn,prev_ts, ts,fname,numchan);
            
                let (tx, nt) = find_update(&prfx, &filelist, &chgfn, prev_ts, ts,&fname,numchan).expect("failed to write update");
                tm.add(&fname,tx);
                
                let idxfn=format!("{}{}-index.pbf",prfx,fname);
                let txx=ThreadTimer::new();
                write_index_file(&format!("{}{}",prfx,fname),&idxfn,numchan);
                tm.add(&idxfn,txx.since());
                
                filelist.push(FilelistEntry::new(fname, timestamp_string(ts), nt, state));
                prev_ts=ts;
            }
            if op == "update" {
                write_filelist(&prfx, &filelist);
            }
        }
        println!("{}",tm);
            
        
        //println!("{:?}\n{:?}\n{:?}", settings, filelist, to_update);
        
        //panic!("not impl");
    } else {
        panic!("unknown op {}", op);
    }
    
}
