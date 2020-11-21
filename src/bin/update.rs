extern crate osmquadtree;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::update::write_index_file;

use std::env;

fn main() {
    
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 3 {
        panic!("must specify operation and infn")
    }
    
    let op = args[1].clone();
    let infn = args[2].clone();
    
    
    let mut numchan = 4;
    let mut outfn = String::new();
    
    if args.len()>3 {
        for i in 3..args.len() {
            if args[i].starts_with("outfn=") {
                outfn = args[i].substr(6,args[i].len());
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else {
                println!("unexpected argument: {}", args[i]);
            }
        }
    }
    
    
    if op != "initial" {
        panic!("unknown op {}", op);
    }
    
    if op == "initial" {
        
        if outfn.is_empty() {
            outfn = format!("{}-index.pbf", infn);
        }
        
        write_index_file(&infn, &outfn, numchan);
    }
    
}
