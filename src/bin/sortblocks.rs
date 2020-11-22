extern crate osmquadtree;
use std::env;

use osmquadtree::stringutils::StringUtils;
use osmquadtree::utils::parse_timestamp;
use osmquadtree::sortblocks::{find_groups,sort_blocks, sort_blocks_inmem};



fn main() {
    let args: Vec<String> = env::args().collect();
    let mut infn = String::from("test.pbf");
    if args.len()>1 {
        infn = args[1].clone();
    }
    
    let mut numchan = 4;
    let mut maxdepth=17;
    let mut target=40000i64;
    let mut mintarget=-1;
    let mut outfn = String::new();
    let mut qtsfn = String::new();
    let mut use_inmem=false;
    let mut splitat = 0i64;
    let mut tempinmem=false;
    let mut limit=0usize;
    let mut timestamp=0;
    let mut haderr=false;
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().expect("failed to read numchan");
            } else if args[i].starts_with("outfn=") {
                outfn = args[i].substr(6,args[i].len());
            } else if args[i].starts_with("qtsfn=") {
                qtsfn = args[i].substr(6,args[i].len());
            } else if args[i].starts_with("maxdepth=") {
                maxdepth = args[i].substr(9,args[i].len()).parse().expect("failed to read maxdepth");
            } else if args[i].starts_with("target=") {
                target = args[i].substr(7,args[i].len()).parse().expect("failed to read target");
            } else if args[i].starts_with("mintarget=") {
                mintarget = args[i].substr(10,args[i].len()).parse().expect("failed to read absmintarget");
            } else if args[i] == "inmem" {
                use_inmem=true;
            } else if args[i].starts_with("splitat=") {
                splitat = args[i].substr(8, args[i].len()).parse().expect("failed to read splitat");
            } else if args[i].starts_with("limit=") {
                splitat = args[i].substr(6, args[i].len()).parse().expect("failed to read limit");
            } else if args[i] == "tempinmem" { 
                tempinmem = true;
            } else if args[i].starts_with("timestamp=") {
                timestamp = parse_timestamp(&args[i].substr(10,args[i].len())).expect("failed to read timestamp");
            } else {
                println!("unknown argument {}",args[i]);
                haderr=true;
            }
        }
    }
    if haderr {
        panic!("errors");
    }
    if splitat==0 {
        splitat = 3000000i64 / target;
        
    }
    
    if qtsfn.is_empty() {
        qtsfn = format!("{}-qts.pbf", &infn.substr(0, infn.len()-4));
    }
    if outfn.is_empty() {
        outfn = format!("{}-blocks.pbf", &infn.substr(0, infn.len()-4));
    }
    if mintarget < 0 {
        mintarget = target/2;
    }
    let groups = find_groups(&qtsfn, numchan, maxdepth, target, mintarget).expect("failed to find groups");
    println!("groups: {} {}", groups.len(), groups.total_weight());
    if limit == 0 {
        limit = 60000000usize / (groups.len() / (splitat as usize));
        if tempinmem {
            limit = usize::max(1000, limit/ 10);
        }
        
    }
    if use_inmem {
        sort_blocks_inmem(&infn, &qtsfn, &outfn, groups, numchan, timestamp).expect("failed to sort blocks");
    } else {
        sort_blocks(&infn, &qtsfn, &outfn, groups, numchan, splitat, tempinmem, limit, timestamp).expect("failed to sort blocks");
    }
}
    
