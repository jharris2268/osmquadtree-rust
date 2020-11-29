extern crate osmquadtree;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::{write_index_file,FilelistEntry, read_filelist, write_filelist,find_update};
use osmquadtree::utils::{parse_timestamp,timestamp_string,date_string,Timings,ThreadTimer};


use std::env;
use std::fs::File;
use serde::{Deserialize,Serialize};



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
            
       
    } else {
        panic!("unknown op {}", op);
    }
    
}
