extern crate osmquadtree;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::{write_index_file,FilelistEntry, read_filelist, write_filelist,read_xml_change,check_index_file};
use osmquadtree::utils::{parse_timestamp,timestamp_string,Timings, date_string,ReplaceNoneWithTimings,MergeTimings};
use osmquadtree::callback::{Callback,CallbackMerge,CallbackSync,CallFinish};
use osmquadtree::header_block;
use osmquadtree::elements::{IdSet,Quadtree,PrimitiveBlock};
use osmquadtree::read_file_block;
use std::env;
use std::fs::File;
use serde::{Deserialize,Serialize};
use std::collections::{BTreeSet,BTreeMap};
use std::io::{Error,ErrorKind,BufReader};
use std::sync::Arc;

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

struct CollectTiles {
    res: BTreeMap<Quadtree,PrimitiveBlock>,
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
}   
    

fn collect_existing(prfx: &str, filelist: &Vec<FilelistEntry>, tiles: &BTreeSet<Quadtree>, idset: IdSet, numchan: usize) -> std::io::Result<(BTreeMap<Quadtree,PrimitiveBlock>,IdSet)> {
    
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
    
    let idsetw = Arc::new(idset);
    
    let colls = CallbackSync::new(Box::new(CollectTiles::new()),numchan);
    
    let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<read_file_block::FileBlock>),ReturnType=<CollectTiles as CallFinish>::ReturnType>>> = Vec::new();
    
    for coll in colls {
        let cca = Box::new(ReplaceNoneWithTimings::new(coll));
        pps.push(Box::new(Callback::new(osmquadtree::convertblocks::make_read_primitive_blocks_combine_call_all_idset(cca, idsetw.clone()))));
    }
    
    let readb = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
    let (mut tm,b) = read_file_block::read_all_blocks_parallel(fbufs, locsv, readb);
    println!("{} {}",tm,b);
    let tls = tm.others.pop().unwrap().1;
    
    match Arc::try_unwrap(idsetw) {
        Ok(idset) => Ok((tls,idset)),
        Err(_) => Err(Error::new(ErrorKind::Other,"can't retrive idset"))
    }
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
        idset.add_node(n);
    }
    for (_,w) in changeblock.ways.iter() {
        idset.add_way(w);
    }
    for (_,r) in changeblock.relations.iter() {
        idset.add_relation(r);
    }
    println!("{}", idset);
    idset.clip_exnodes();
    println!("{}", idset);
    
    let mut tiles = BTreeSet::new();
    
    for fle in filelist {
        let fname = format!("{}{}-index.pbf", prfx, fle.filename);
        
        let (a,b,c) = check_index_file(&fname, idset, numchan)?;
        for q in &a {
            tiles.insert(*q);
        }
        println!("{}: {:5.1}s {} tiles [now {}]", fname,c, a.len(),tiles.len());
        idset=b;
        
    }
    
    println!("need to check {} tiles",tiles.len());        
    
    let (blocks, _idset) = collect_existing(prfx, filelist, &tiles, idset, numchan)?;
    
    println!("{} blocks, {} eles", blocks.len(), blocks.iter().map(|(_,b)| { b.len() as i64 }).sum::<i64>());
    
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
