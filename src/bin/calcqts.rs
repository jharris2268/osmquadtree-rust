extern crate osmquadtree;



use std::env;

use osmquadtree::stringutils::{StringUtils};
use osmquadtree::calcqts::{run_calcqts_inmem,run_calcqts,run_calcqts_load_existing};


fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
    
    let mut numchan = 4;
    let mut outfn = String::new();
    let mut use_simple=false;
    let mut load_existing=false;
    let mut qt_level=17usize;
    let mut qt_buffer=0.05;
    let mut seperate=false;
    let mut resort_waynodes=true;
    let mut inmem=false;
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("outfn=") {
                outfn = args[i].substr(6,args[i].len());
            } else if args[i] == "use_simple" {
                use_simple = true;
            } else if args[i] == "load_existing" {
                load_existing=true;
            } else if args[i].starts_with("qt_level=") {
                qt_level = args[i].substr(9,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("qt_buffer=") {
                qt_buffer = args[i].substr(10,args[i].len()).parse().unwrap();
            } else if args[i] == "seperate" {
                seperate=true
            } else if args[i] == "dont_resort_waynodes" {
                resort_waynodes=false;
            } else if args[i] == "inmem" {
                inmem=true;
            }
        }
    }
    if outfn.is_empty() {
        outfn = format!("{}-qts.pbf", &fname.substr(0, fname.len()-4));
    }
    if inmem {
        run_calcqts_inmem(&fname, &outfn, qt_level, qt_buffer, numchan).expect("?");
    } else if load_existing {
        run_calcqts_load_existing(&fname, &outfn, qt_level, qt_buffer, seperate, numchan).expect("?");
    } else {
        run_calcqts(&fname, &outfn, qt_level, qt_buffer, use_simple, seperate, resort_waynodes, numchan).expect("?");
    }
}








