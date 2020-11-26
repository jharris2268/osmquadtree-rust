extern crate osmquadtree;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::{write_index_file,FilelistEntry, read_filelist, write_filelist,read_xml_change,check_index_file};
use osmquadtree::utils::{parse_timestamp,timestamp_string,Timings, date_string,MergeTimings,ThreadTimer};
use osmquadtree::callback::{Callback,CallbackMerge,CallFinish};
use osmquadtree::header_block;
use osmquadtree::elements::{IdSet,Quadtree,PrimitiveBlock,Changetype,Node,ElementType};
use osmquadtree::read_file_block;
use osmquadtree::sortblocks::QuadtreeTree;
//use osmquadtree::convertblocks;
use std::env;
use std::fs::File;
use serde::{Deserialize,Serialize};
use std::collections::{BTreeSet,BTreeMap};
use std::io::{Error,ErrorKind,BufReader,Write,stdout};
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



fn check_state(settings: &Settings, filelist: &Vec<FilelistEntry>) -> Vec<(String,i64,i64)> {
    let mut res = Vec::new();
    if filelist.is_empty() {
        panic!("empty filelist");
    }
    let last_state = filelist.last().unwrap().state;
    
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
    res
}

/*struct CollectTiles {
    qts: BTreeMap<(ElementType,i64),(Quadtree,Quadtree)>,
    othernodes: Vec<Node>,
    
    //res: BTreeMap<Quadtree,PrimitiveBlock>,
}
impl CollectTiles {
    pub fn new() -> CollectTiles {
        CollectTiles{res: BTreeMap::new()}
    }
}
impl CallFinish for CollectTiles {
    type CallType=PrimitiveBlock;
    type ReturnType=Timings<BTreeMap<Quadtree,PrimitiveBlock>>;

    fn call(&mut self, bl: PrimitiveBlock) {
        self.res.insert(bl.quadtree, bl);
    }
    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Timings::new();
        tm.add_other("tiles", std::mem::take(&mut self.res));
        Ok(tm)
    }
} */  

pub struct OrigData {
    pub node_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub way_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub relation_qts: BTreeMap<i64,(Quadtree,Quadtree)>,
    pub othernodes: Vec<Node>
}
impl OrigData {
    pub fn new() -> OrigData {
        OrigData{node_qts: BTreeMap::new(),way_qts: BTreeMap::new(),relation_qts: BTreeMap::new(),othernodes: Vec::new()}
    }
    
    pub fn add(&mut self, pb: PrimitiveBlock, idset: &IdSet) {
        for n in pb.nodes {
            match n.changetype {
                Changetype::Normal | Changetype::Unchanged | Changetype::Modify | Changetype::Create => {
                    self.node_qts.insert(n.id,(n.quadtree,pb.quadtree));
                    if idset.is_exnode(n.id) {
                        let mut n = n;
                        n.changetype=Changetype::Normal;
                        self.othernodes.push(n);
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
                _ => {}
            }
        }
        
        for r in pb.relations {
            match r.changetype {
                Changetype::Normal | Changetype::Unchanged | Changetype::Modify | Changetype::Create => {
                    self.relation_qts.insert(r.id,(r.quadtree,pb.quadtree));
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
        
        //let b = read_primitive_blocks_combine(idx_blocks.0 as i64, idx_blocks.1, Some(self.ids.as_ref())).expect("?");
        //println!("block {} {} nodes, {} ways, {} relations", b.quadtree.as_string(),b.nodes.len(),b.ways.len(),b.relations.len());
        self.tm+=tx.since();
        //self.out.call(b);
    }
    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = Timings::new();//self.out.finish()?;
        tm.add("read_primitive_blocks_combine", self.tm);
        tm.add_other("origdata", self.origdata.take().unwrap());
        Ok(tm)
    }
}




fn read_change_tiles(fname: &str, tiles: &BTreeSet<Quadtree>, idset: Arc<IdSet>, numchan: usize) -> std::io::Result<(OrigData,f64)> {
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
        //let collect = Box::new(CollectTiles::new());
        let convert = Box::new(ReadPB::new(ischange,idset));//convertblocks::make_read_primitive_blocks_combine_call_all_idset(collect, idset);
        read_file_block::read_all_blocks_locs(&mut file, fname, locs, false, convert)
    } else {
        //let collect = CallbackSync::new(Box::new(CollectTiles::new()), numchan);
        let mut convs: Vec<Box<dyn CallFinish<CallType=(usize,read_file_block::FileBlock),ReturnType=Timings<OrigData>>>> = Vec::new();
        for _ in 0..numchan { //coll in collect {
            //let coll2 = Box::new(ReplaceNoneWithTimings::new(coll));
            convs.push(Box::new(Callback::new(Box::new(ReadPB::new(ischange,idset.clone())))));
                //convertblocks::make_read_primitive_blocks_combine_call_all_idset(coll2, idset.clone()))));
        }
        let convsm = Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())));
        read_file_block::read_all_blocks_locs(&mut file, fname, locs, true, convsm)
    };
    
    //println!("{}", tm);
    
    
    let mut tls = tm.others.pop().unwrap().1;
    while !tm.others.is_empty() {
        tls.extend(tm.others.pop().unwrap().1);
    }
    Ok((tls,b))
}
    
    
    

fn collect_existing_alt(
    prfx: &str, filelist: &Vec<FilelistEntry>,
    //tiles: &BTreeSet<Quadtree>,
    idset: Arc<IdSet>,
    numchan: usize) -> std::io::Result<OrigData> {
        
    //let mut changetiles = BTreeMap::new();
    let mut origdata = OrigData::new();
    
    //let mut tne=0;
    //let mut tnobj=0;
    let mut tt = 0.0;
    
    
    
    for (i,fle) in filelist.iter().enumerate() {
        let nc = if i==0 { numchan} else { 1 };
        
        
        let fnameidx = format!("{}{}-index.pbf", prfx, fle.filename);
        
        let (a,c) = check_index_file(&fnameidx, idset.clone(), nc)?;
        
        
        
        tt+=c;
        print!("\r{}: {:5.1}s {:5.1}s {} tiles", fnameidx,c, tt,a.len());
        stdout().flush().expect("");
        //idset=b;
        let mut ctiles = BTreeSet::new();
        ctiles.extend(a);
        
      //  let mut ne=0; let mut nobj=0;
        let fname = format!("{}{}", prfx, fle.filename);
        if i==0 {
            println!("");
        }
        let (bb,t) = read_change_tiles(&fname, &ctiles, idset.clone(), nc)?;
        print!("  {}: {} {:5.1}s", fname, bb, t);
        origdata.extend(bb);
        tt+=t;
        
        /*let bbl=bb.len();
        for (q,b) in bb {
            nobj+=b.len();
            if b.len() == 0 {
                ne+=1;
            } else if !changetiles.contains_key(&q) {
                changetiles.insert(q, b);
            } else {
                
                let (k,v) = changetiles.remove_entry(&q).unwrap();
                let m = apply_change_primitive(v, b);
                changetiles.insert(k, m);
            }
        }
        
        tt+=t;
        tne+=ne;
        tnobj+=nobj;
        if i==0 {
            println!("");
        }
        print!("    {}: {} tiles {} empty, {} objs {:5.1}s => {} tiles", fname, bbl, ne, nobj, tt, changetiles.len());*/
        
        stdout().flush().expect("");
    }
    //println!("\n{} tiles {} empty, {} objs {:5.1}s", changetiles.len(), tne, tnobj, tt);
    println!("");
    Ok(origdata)
    
}
            
            

/*
fn collect_existing(prfx: &str, filelist: &Vec<FilelistEntry>, tiles: &BTreeSet<Quadtree>, idset: Arc<IdSet>, numchan: usize) -> std::io::Result<BTreeMap<Quadtree,PrimitiveBlock>> {
    
    let mut locs = BTreeMap::new();
    let mut fbufs=Vec::new();
    
    for (i,fle) in filelist.iter().enumerate() {
        let fle_fn = format!("{}{}", prfx, fle.filename);
        let f = File::open(&fle_fn).expect("fail");
        let mut fbuf = BufReader::new(f);
        
        let fb = read_file_block::read_file_block(&mut fbuf).expect("?");
        let filepos = read_file_block::file_position(&mut fbuf).expect("?");
        let head = header_block::HeaderBlock::read(filepos, &fb.data(), &fle_fn).expect("?");
        
        
        
        for entry in head.index {
            if !locs.contains_key(&entry.quadtree) {
                if i != 0 {
                    panic!(format!("quadtree {} not in first file?", entry.quadtree.as_string()));
                }
                locs.insert(entry.quadtree.clone(), (locs.len(), Vec::new()));
            }
            
            locs.get_mut(&entry.quadtree).unwrap().1.push((i, entry.location));
        }
        
        fbufs.push(fbuf);
    }
    
    
    let mut locsv = Vec::new();
    for (a,(_b,c)) in &locs {
        if tiles.contains(&a) {
            locsv.push((locsv.len(),c.clone()));
        }
    }
        
    println!("{} files, {} / {} tiles", fbufs.len(), locsv.len(), locs.len());
    
    //let idsetw = Arc::new(idset);
    
    let colls = CallbackSync::new(Box::new(CollectTiles::new()),numchan);
    
    let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<read_file_block::FileBlock>),ReturnType=<CollectTiles as CallFinish>::ReturnType>>> = Vec::new();
    
    for coll in colls {
        let cca = Box::new(ReplaceNoneWithTimings::new(coll));
        pps.push(Box::new(Callback::new(convertblocks::make_read_primitive_blocks_combine_call_all_idset(cca, idset.clone()))));
    }
    
    let readb = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
    let (mut tm,b) = read_file_block::read_all_blocks_parallel(fbufs, locsv, readb);
    println!("{} {}",tm,b);
    let tls = tm.others.pop().unwrap().1;
    
    Ok(tls)
    
}*/

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
fn find_update(prfx: &str, filelist: &Vec<FilelistEntry>, change_filename: &str, _state: i64, _timestamp: i64, numchan: usize) -> std::io::Result<(f64,String,usize)> {
    let mut chgf = BufReader::new(File::open(change_filename)?);
    
        
    let changeblock = if change_filename.ends_with(".gz") {
        read_xml_change(&mut BufReader::new(flate2::bufread::GzDecoder::new(chgf)))
    } else {
        read_xml_change(&mut chgf)
    }?;
    
    let mut idset=IdSet::new();
    
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
        //println!("rel {} {} // {} mems ", r.id, r.members.len(), r.members.iter().filter(|m| { idset.contains(m.mem_type.clone(),m.mem_ref.clone()) }).count());
        idset.relations.insert(r.id);
        for m in r.members.iter() {
            match m.mem_type {
                ElementType::Node => { idset.nodes.insert(m.mem_ref); },
                ElementType::Way => { idset.ways.insert(m.mem_ref); },
                ElementType::Relation => { idset.relations.insert(m.mem_ref); },
            }
        }
        
    }
    println!("{}", idset);
    
    
    let idset = Arc::new(idset);
    
    //let mut tiles = BTreeSet::new();
    
    /*let mut tc=0.0;
    for fle in filelist {
        let fname = format!("{}{}-index.pbf", prfx, fle.filename);
        
        let (a,b,c) = check_index_file(&fname, idset, numchan)?;
        for q in &a {
            tiles.insert(*q);
        }
        tc+=c;
        print!("\r{}: {:5.1}s {:5.1}s {} tiles [now {}]", fname,c, tc,a.len(),tiles.len());
        stdout().flush().expect("");
        idset=b;
        
    }*/
    
    //println!("\nneed to check {} tiles",tiles.len());        
    
    let blocks = collect_existing_alt(prfx, filelist, /*&tiles, */idset, numchan)?;
    
    println!("{}", blocks);
    //println!("{} blocks, {} eles", blocks.len(), blocks.iter().map(|(_,b)| { b.len() as i64 }).sum::<i64>());
    
    let tree = prep_tree(prfx, filelist)?;
    println!("{}", tree);
    
    panic!("not impl");
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
        
        
        
    } else if op == "update" {
        
        let settings = Settings::from_file(&prfx);
        let mut filelist = read_filelist(&prfx);
        if filelist.len()>1 {
            filelist.pop();
        }
        
        let to_update = check_state(&settings, &filelist);
        println!("have {} in filelist, {} to update", filelist.len(), to_update.len());
        let mut tm = Timings::<()>::new();
        if !to_update.is_empty() {
            for (chgfn, st, ts) in to_update {
                println!("call find_update('{}',{} entries,'{}', {}, {}, {})", prfx,filelist.len(),chgfn,st,ts,numchan);
                let (tx, fname, nt) = find_update(&prfx, &filelist, &chgfn, st, ts,numchan).expect("failed to write update");
                tm.add(&fname,tx);
                
                filelist.push(FilelistEntry::new(fname, date_string(ts), nt, st));
                
            }
            
            write_filelist(&prfx, &filelist);
        }
        
            
        
        //println!("{:?}\n{:?}\n{:?}", settings, filelist, to_update);
        
        //panic!("not impl");
    } else {
        panic!("unknown op {}", op);
    }
    
}
