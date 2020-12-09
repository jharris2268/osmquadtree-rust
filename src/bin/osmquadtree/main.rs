extern crate clap;

use clap::{App, SubCommand,AppSettings,Arg,value_t};
mod count;
use count::run_count;

use osmquadtree::calcqts::{run_calcqts,run_calcqts_inmem};
use std::io::{Error,ErrorKind};
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
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"))            
        )
        .subcommand(
            SubCommand::with_name("calcqts")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("COMBINED").short("-c").long("--combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("SIMPLE").short("-s").long("--simple").help("simplier implementation, suitable for files <8gb"))
                .arg(Arg::with_name("QT_LEVEL").short("-l").long("--qt_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("QT_BUFFER").short("-b").long("--qt_buffer").takes_value(true).help("qt buffer, defaults to 0.05"))
        )
        .subcommand(
            SubCommand::with_name("calcqts_inmem")
                .about("calculates quadtrees for each element of an extract pbf file (maximum size 1gb)")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("QT_LEVEL").short("-l").long("--qt_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("QT_BUFFER").short("-b").long("--qt_buffer").takes_value(true).help("qt buffer, defaults to 0.05"))
        );
        
    let mut help=Vec::new();
    app.write_help(&mut help).expect("?");
    
    let res = 
        match app.get_matches().subcommand() {
            ("count", Some(count)) => { run_count(
                    count.value_of("INPUT").unwrap(),
                    count.is_present("PRIMITIVE"),
                    value_t!(count,"NUMCHAN",usize).unwrap_or(4),
                    count.value_of("FILTER"))
                    },
            ("calcqts", Some(calcqts)) => { run_calcqts(
                    calcqts.value_of("INPUT").unwrap(),
                    calcqts.value_of("QTSFN"),
                    value_t!(calcqts,"QT_LEVEL",usize).unwrap_or(17),
                    value_t!(calcqts,"QT_BUFFER",f64).unwrap_or(0.05),
                    !calcqts.is_present("COMBINED"), //seperate
                    calcqts.is_present("SIMPLE"),
                    true, //resort_waynodes
                    value_t!(calcqts,"NUMCHAN",usize).unwrap_or(4))
                    },
            ("calcqts_inmem", Some(calcqts)) => { run_calcqts_inmem(
                    calcqts.value_of("INPUT").unwrap(),
                    calcqts.value_of("QTSFN"),
                    value_t!(calcqts,"QT_LEVEL",usize).unwrap_or(17),
                    value_t!(calcqts,"QT_BUFFER",f64).unwrap_or(0.05),
                    value_t!(calcqts,"NUMCHAN",usize).unwrap_or(4))
                    },
            _ => { Err(Error::new(ErrorKind::Other,"??")) }
        };
    
    
    match res {
        Ok(()) => {},
        Err(err) => {
            println!("FAILED: {}", err);
            println!("{}", String::from_utf8(help).unwrap());
        }
    }
    
    //println!("count: {:?}", matches);
}
