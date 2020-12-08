extern crate clap;

use clap::{App, SubCommand,AppSettings,Arg,value_t};
mod count;
use count::run_count;
/*
fn parse_numchan(numchan: Option<&str>) -> usize {
    match numchan {
        None => 4, 
        Some(n) => {
            let x = n.parse().expect("NUMCHAN must be a positive integer between 0 and 8");
            if x < 8 {
                x
            } else {
                panic!("NUMCHAN must be a positive integer between 0 and 8");
            }
        },
    }
}*/


//fn run_calcqts(input: String, qtsfn: Option<String>, numchan: usize, combined: bool, inmem: bool) {}


fn main() {
    // basic app information
    let app = App::new("osmquadtree")
        .version("0.1")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .author("James Harris")
        .subcommand(
            SubCommand::with_name("count")
                .about("uses osmquadtree to read an open street map pbf file and report basic information")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("PRIMITIVE").short("-p").long("--primitive").help("reads full primitiveblock data"))
                .arg(Arg::with_name("NUMCHAN").short("-c").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"))            
        )
        .subcommand(
            SubCommand::with_name("calcqts")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .args_from_usage("
                    <INPUT>                     'Sets the input pbf planet or extract to use'
                    -q, --qtsfn=[<QTSFN>]       'reads full primitiveblock data'
                    -c, --numchan=[<NUMCHAN>]   'uses NUMCHAN parallel threads'
                    --combined                  'writes combined nodewaynodes file'
                    --inmem                     'calculates in memory: only suitible for small input files
                    ")
        );
        
    
    
    match app.get_matches().subcommand() {
        ("count", Some(count)) => { run_count(count.value_of("INPUT").unwrap(), count.is_present("PRIMITIVE"), value_t!(count,"NUMCHAN",usize).unwrap_or(4), count.value_of("FILTER")).unwrap(); },
        ("calcqts", Some(calcqts)) => { println!("run_calcqts: {:?}", calcqts.args); },//run_calcqts(calcqts.get("INPUT"), calcqts.get("-q"), calcqts.get("-c"), calcqts.get("--combined"), calcqts.get("--inmem")); },
        _ => {println!("??")}
    }
    
    //println!("count: {:?}", matches);
}
