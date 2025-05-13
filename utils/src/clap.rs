extern crate clap;

use crate::setup;
use crate::error::{Error, Result};
//mod klask_messages;

use clap::{Command, Arg, ArgMatches, ArgAction};
use sysinfo::{System};

use osmquadtree::count::run_count;

use osmquadtree::calcqts::{run_calcqts, run_calcqts_load_existing, run_calcqts_prelim};
use osmquadtree::pbfformat::{file_length,read_filelist, write_filelist, CompressionType};
use osmquadtree::sortblocks::{find_groups, sort_blocks, sort_blocks_inmem, QuadtreeTree};
use osmquadtree::update::{run_update, run_update_initial, write_index_file};
use osmquadtree::utils::{parse_timestamp, LogTimes};

//use osmquadtree::geometry::postgresql::{PostgresqlConnection, PostgresqlOptions,prepare_tables};
//use osmquadtree::geometry::{GeometryStyle, OutputType};
use osmquadtree::mergechanges::{
    run_mergechanges, run_mergechanges_sort, run_mergechanges_sort_from_existing,
    run_mergechanges_sort_inmem,
};
use osmquadtree::message;
use osmquadtree::defaultlogger::register_messenger_default;

//use crate::setup;

//use std::io::{Error, ErrorKind, Result};

use std::sync::Arc;
/*
fn process_geometry(
    prfx: &str,
    outfn: OutputType,
    filter: Option<&str>,
    timestamp: Option<&str>,
    find_minzoom: bool,
    style_name: Option<&str>,
    max_minzoom: Option<i64>,
    numchan: usize,
) -> Result<()> {
    osmquadtree::geometry::process_geometry(prfx, outfn, filter, timestamp, find_minzoom, style_name, max_minzoom, numchan)?;
    Ok(())
}
*/

fn get_compression_type(f: &clap::ArgMatches) -> CompressionType {
    let l = get_compression_type_int(f);
    println!("compression_type={:?}", l);
    return l;
}

fn get_compression_type_int(f: &clap::ArgMatches) -> CompressionType {

    let level = f.get_one::<u32>("COMPLEVEL");

    if f.contains_id("BROTLI") {

        //println!("brotli");
        if let Some(&l) = level {
            if l>11 {
                panic!("max compression level for brotli is 11")
            }
            return CompressionType::BrotliLevel(l);
        } else {
            return CompressionType::Brotli;
        }
    } else if f.contains_id("LZMA") {
        //println!("lzma");
        //return CompressionType::Lzma;
        if let Some(&l) = level {
            if l>9 {
                panic!("max compression level for lzma is 9")
            }
            return CompressionType::LzmaLevel(l);
        } else {
            return CompressionType::Lzma;
        }
    } else if f.contains_id("UNCOMPRESSED") {
        //println!("uncompressed");
        return CompressionType::Uncompressed;
    }/* else if f.is_present("LZ4") {
        println!("Lz4");
        return CompressionType::Lz4;
    }*/

    
    if let Some(&l) = level {
        if l>11 {
            panic!("max compression level for zlib is 9")
        }
        return CompressionType::ZlibLevel(l);
    }
    return CompressionType::Zlib;
    
    
}

fn run_sortblocks(
    infn: &str,
    qtsfn: Option<&str>,
    outfn: Option<&str>,
    maxdepth: usize,
    target: i64,
    mut mintarget: i64,
    use_inmem: bool,
    //splitat: i64, tempinmem: bool, limit: usize,
    timestamp: Option<&str>,
    numchan: usize,
    ram_gb: usize,
    keep_temps: bool,
    compression_type: CompressionType,
) -> Result<()> {
    let mut lt = LogTimes::new();

    let mut splitat = 0i64;
    let tempinmem = file_length(infn) < 32 * 1024 * 1024 * (ram_gb as u64);
    let mut limit = 0usize;

    if splitat == 0 {
        splitat = 1500000i64 / target;
    }
    //let write_at = 2000000;

    let qtsfn_ = match qtsfn {
        Some(q) => String::from(q),
        None => format!("{}-qts.pbf", &infn[0..infn.len() - 4]),
    };
    let qtsfn = &qtsfn_;

    let outfn_ = match outfn {
        Some(q) => String::from(q),
        None => format!("{}-blocks.pbf", &infn[0..infn.len() - 4]),
    };
    let outfn = &outfn_;

    let timestamp = match timestamp {
        Some(t) => parse_timestamp(t)?,
        None => 0,
    };

    if mintarget < 0 {
        mintarget = target / 2;
    }

    let groups: Arc<QuadtreeTree> = Arc::from(find_groups(
        &qtsfn, numchan, maxdepth, target, mintarget, &mut lt,
    )?);

    message!("groups: {} {}", groups.len(), groups.total_weight());
    if limit == 0 {
        limit = 4000000usize * ram_gb / (groups.len() / (splitat as usize));
        if tempinmem {
            limit = usize::max(1000, limit / 10);
        }
    }
    if use_inmem {
        sort_blocks_inmem(&infn, &qtsfn, &outfn, groups, numchan, timestamp, compression_type, &mut lt)?;
    } else {
        sort_blocks(
            &infn, &qtsfn, &outfn, groups, numchan, splitat, tempinmem, limit, /*write_at*/
            timestamp, keep_temps, compression_type,&mut lt,
        )?;
    }
    message!("{}", lt);
    Ok(())
}

fn run_update_droplast(prfx: &str) -> Result<()> {
    let mut fl = read_filelist(prfx);
    if fl.len() < 2 {
        return Err(Error::InvalidInputError(
            format!("{}filelist.json only has {} entries", prfx, fl.len())
        ));
    }
    fl.pop();
    write_filelist(prfx, &fl);
    Ok(())
}

fn write_index_file_w(prfx: &str, outfn: Option<&str>, numchan: usize) -> Result<()> {
    match outfn {
        Some(o) => write_index_file(prfx, o, numchan),
        None => write_index_file(prfx, "", numchan),
    };
    Ok(())
}
/*
fn dump_geometry_style(outfn: Option<&str>) -> Result<()> {
    let outfn = match outfn {
        Some(o) => String::from(o),
        None => String::from("default_style.json"),
    };
    let mut f = std::fs::File::create(&outfn)?;
    serde_json::to_writer_pretty(&mut f, &GeometryStyle::default())?;
    Ok(())
}

fn get_i64(x: Option<&str>) -> Option<i64> {
    match x {
        None => None,
        Some(t) => Some(t.parse().expect("expected integer argument")),
    }
}
*/
//const NUMCHAN_DEFAULT: usize = 4;
const RAM_GB_DEFAULT: usize= 8;
const QT_MAX_LEVEL_DEFAULT: usize = 18;
const QT_GRAPH_LEVEL_DEFAULT: usize = 17;
const QT_BUFFER_DEFAULT: f64 = 0.05;

pub struct Defaults {
    numchan_default: usize,
    ram_gb_default: usize
}
impl Defaults {
    pub fn new() -> Defaults {
        let numchan_default = num_cpus::get();
        let ram_gb_default = if sysinfo::IS_SUPPORTED_SYSTEM {
            let mut s = System::new_all();
            s.refresh_all();
            let tm = f64::round((s.total_memory() as f64) / 1024.0/1024.0);
            //message!("have {} mb total memory", tm);
            
            (tm/1024.0) as usize
        } else {
            RAM_GB_DEFAULT
        };
        message!("ram_gb_default={}",ram_gb_default);    
        Defaults{numchan_default, ram_gb_default}
    }
}

pub fn run(defaults: &Defaults, subcommand: &str, args: &ArgMatches) -> Result<()> {
    match Some((subcommand, args))   {
        //Some(("gui", _)) => run_gui(defaults),
        Some(("count", count)) => run_count(
                &fix_input(count.get_one::<String>("INPUT").unwrap()),
                count.contains_id("PRIMITIVE"),
                *count.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                count.get_one::<String>("FILTER").map(|x| x.as_str()),
                count.get_one::<String>("TIMESTAMP").map(|x| x.as_str())
            ).or_else(|e| Err(Error::from(e))),
        Some(("setup", _)) => setup::run(defaults.ram_gb_default, defaults.numchan_default),
        Some(("calcqts", calcqts)) => {
            match run_calcqts(
                calcqts.get_one::<String>("INPUT").unwrap(),
                calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
                *calcqts.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
                *calcqts.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
                calcqts.get_one::<String>("MODE").map(|x| x.as_str()),
                /* !calcqts.is_present("COMBINED"), //seperate
                true,                            //resort_waynodes*/
                calcqts.contains_id("KEEPTEMPS"),
                *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *calcqts.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
            ) {
                Ok((_,lt,_)) => { message!("{}", lt); Ok(()) },
                Err(e) => Err(Error::from(e))
            }
        },
        Some(("calcqts_prelim", calcqts)) => run_calcqts_prelim(
            calcqts.get_one::<String>("INPUT").unwrap(),
            calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
            *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ).or_else(|e| Err(Error::from(e))),
        
        Some(("calcqts_load_existing", calcqts)) => run_calcqts_load_existing(
            calcqts.get_one::<String>("INPUT").unwrap(),
            calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
            *calcqts.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
            *calcqts.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
            match calcqts.get_one::<u64>("STOP_AT") {
                Some(&s) => Some(s),
                None => None,
            },
            *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ).or_else(|e| Err(Error::from(e))),
        Some(("sortblocks", sortblocks)) => {
            run_sortblocks(
                sortblocks.get_one::<String>("INPUT").unwrap(),
                sortblocks.get_one::<String>("QTSFN").map(|x| x.as_str()),
                sortblocks.get_one::<String>("OUTFN").map(|x| x.as_str()),
                *sortblocks.get_one::<usize>("QT_MAX_LEVEL").unwrap_or(&QT_GRAPH_LEVEL_DEFAULT),
                *sortblocks.get_one::<i64>("TARGET").unwrap_or(&40000),
                *sortblocks.get_one::<i64>("MINTARGET").unwrap_or(&-1),
                false, //use_inmem
                sortblocks.get_one::<String>("TIMESTAMP").map(|x| x.as_str()),
                *sortblocks.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *sortblocks.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
                sortblocks.contains_id("KEEPTEMPS"),
                get_compression_type(sortblocks)
            )
        }
        Some(("sortblocks_inmem", sortblocks)) => {
            run_sortblocks(
                sortblocks.get_one::<String>("INPUT").unwrap(),
                sortblocks.get_one::<String>("QTSFN").map(|x| x.as_str()),
                sortblocks.get_one::<String>("OUTFN").map(|x| x.as_str()),
                *sortblocks.get_one::<usize>("QT_MAX_LEVEL").unwrap_or(&QT_GRAPH_LEVEL_DEFAULT),
                *sortblocks.get_one::<i64>("TARGET").unwrap_or(&40000),
                *sortblocks.get_one::<i64>("MINTARGET").unwrap_or(&-1),
                true, //use_inmem
                sortblocks.get_one::<String>("TIMESTAMP").map(|x| x.as_str()),
                *sortblocks.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *sortblocks.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
                false,
                get_compression_type(sortblocks)
            )
        }
        Some(("update_initial", update)) => {
            run_update_initial(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                update.get_one::<String>("INFN").unwrap(),
                update.get_one::<String>("TIMESTAMP").unwrap(),
                update.get_one::<i64>("INITIAL_STATE").map(|x| *x),
                update.get_one::<String>("DIFFS_SOURCE").map(|x| x.as_str()),
                update.get_one::<String>("DIFFS_LOCATION").unwrap(),
                *update.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
                *update.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        },
        Some(("update", update)) => {
            run_update(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                *update.get_one::<usize>("LIMIT").unwrap_or(&0),
                false, //as_demo
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        }
        Some(("update_demo", update)) => {
            run_update(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                *update.get_one::<usize>("LIMIT").unwrap_or(&0),
                true, //as_demo
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        }
        Some(("update_droplast", update)) => run_update_droplast(&fix_input(update.get_one::<String>("INPUT").unwrap())),
        Some(("write_index_file", write)) => write_index_file_w(
            write.get_one::<String>("INPUT").unwrap(),
            write.get_one::<String>("OUTFN").map(|x| x.as_str()),
            *write.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ),
        Some(("mergechanges_sort_inmem", filter)) => {
            run_mergechanges_sort_inmem(
                &fix_input(filter.get_one::<String>("INPUT").unwrap()),
                filter.get_one::<String>("OUTFN").unwrap(),
                filter.get_one::<String>("FILTER").map(|x| x.as_str()),
                filter.contains_id("FILTEROBJS"),
                filter.get_one::<String>("TIMESTAMP").map(|x| x.as_str()),
                *filter.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *filter.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
                get_compression_type(filter)
            ).or_else(|e| Err(Error::from(e)))
        },
        Some(("mergechanges_sort", filter)) => {
            run_mergechanges_sort(
                &fix_input(filter.get_one::<String>("INPUT").unwrap()),
                filter.get_one::<String>("OUTFN").unwrap(),
                filter.get_one::<String>("TEMPFN").map(|x| x.as_str()),
                filter.get_one::<String>("FILTER").map(|x| x.as_str()),
                filter.contains_id("FILTEROBJS"),
                filter.get_one::<String>("TIMESTAMP").map(|x| x.as_str()),
                filter.contains_id("KEEPTEMPS"),
                get_compression_type(filter),
                *filter.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *filter.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
                filter.contains_id("SINGLETEMPFILE")
            ).or_else(|e| Err(Error::from(e)))
        },
        Some(("mergechanges_sort_from_existing", filter)) => {
            run_mergechanges_sort_from_existing(
                &fix_input(filter.get_one::<String>("OUTFN").unwrap()),
                filter.get_one::<String>("TEMPFN").unwrap(),
                filter.contains_id("ISSPLIT"),
                get_compression_type(filter),
                *filter.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        },
        Some(("mergechanges", filter)) => {
            run_mergechanges(
                &fix_input(filter.get_one::<String>("INPUT").unwrap()),
                filter.get_one::<String>("OUTFN").unwrap(),
                filter.get_one::<String>("FILTER").map(|x| x.as_str()),
                filter.contains_id("FILTEROBJS"),
                filter.get_one::<String>("TIMESTAMP").map(|x| x.as_str()),
                get_compression_type(filter),
                *filter.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        },
       
        x => Err(Error::InvalidInputError(format!("?? {:?}", x))),
    }
}



//pub fn make_app() -> App<'static> {
//    App::new("osmquadtree")
pub fn make_app() -> Command {
    Command::new("osmquadtree")
        .version("0.1")
        //.setting(AppSettings::SubcommandRequiredElseHelp)
        .subcommand_required(true)
        .author("James Harris")
        
        .subcommand(
            Command::new("count")
                .about("uses osmquadtree to read an open street map pbf file and report basic information")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("PRIMITIVE").short('p').long("primitive").action(ArgAction::SetTrue).help("reads full primitiveblock data"))
                .arg(Arg::allow_hyphen_values(Arg::new("FILTER").short('f').long("filter").num_args(1).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                
                
        )
        .subcommand(
            Command::new("setup")
            .about("prepare an updatable osmquadtree instance")
        )
        .subcommand(
            Command::new("calcqts")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                //.arg(Arg::new("COMBINED").short("-c").long("combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("MODE").short('m').long("mode").num_args(1).help("simplier implementation, suitable for files <8gb"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
                .arg(Arg::new("KEEPTEMPS").short('k').long("keeptemps").action(ArgAction::SetTrue).help("keep temp files"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
        )
        .subcommand(
            Command::new("calcqts_prelim")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                
        )
        .subcommand(
            Command::new("calcqts_load_existing")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                //.arg(Arg::new("COMBINED").short("-c").long("combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("STOP_AT").short('s').long("stop_at").value_parser(clap::value_parser!(u64)).num_args(1).help("location of first file block without nodes"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
        )

        .subcommand(
            Command::new("sortblocks")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").num_args(1).help("specify output filename, defaults to <INPUT>-blocks.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("QT_MAX_LEVEL").short('l').long("qt_max_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 17"))
                .arg(Arg::new("TARGET").short('t').long("target").num_args(1).help("block target size, defaults to 40000"))
                .arg(Arg::new("MIN_TARGET").short('m').long("min_target").num_args(1).help("block min target size, defaults to TARGET/2"))

                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("KEEPTEMPS").short('k').long("keeptemps").action(ArgAction::SetTrue).help("keep temp files"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))

        )
        .subcommand(
            Command::new("sortblocks_inmem")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").num_args(1).help("specify output filename, defaults to <INPUT>-blocks.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("QT_MAX_LEVEL").short('l').long("qt_max_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 17"))
                .arg(Arg::new("TARGET").short('t').long("target").num_args(1).help("block target size, defaults to 40000"))
                .arg(Arg::new("MIN_TARGET").short('m').long("min_target").num_args(1).help("block min target size, defaults to TARGET/2"))

                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))
        )
        .subcommand(
            Command::new("update_initial")
                .about("calculate initial index")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("INFN").required(true).short('i').long("infn").num_args(1).help("specify filename of orig file").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("TIMESTAMP").required(true).short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("INITIAL_STATE").short('s').long("state_initial").num_args(1).help("initial state, if not specified will determine"))
                .arg(Arg::new("DIFFS_SOURCE").short('x').long("diffs_source").num_args(1).help("source for diffs to download, default planet.openstreetmap.org/replication/day/"))
                .arg(Arg::new("DIFFS_LOCATION").required(true).short('d').long("diffs_location").num_args(1).help("directory for downloaded osc.gz files"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("update")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("LIMIT").short('l').long("limit").value_parser(clap::value_parser!(usize)).num_args(1).help("only run LIMIT updates"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("update_demo")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("LIMIT").short('l').long("limit").value_parser(clap::value_parser!(usize)).num_args(1).help("only run LIMIT updates"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("update_droplast")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))


        )
        .subcommand(
            Command::new("write_index_file")
                .about("write pbf index file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").num_args(1).help("out filename, defaults to INPUT-index.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("mergechanges_sort_inmem")
                .about("prep_bbox_filter")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").required(true).num_args(1).help("out filename").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::allow_hyphen_values(Arg::new("FILTER").short('f').long("filter").required(true).num_args(1).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::new("FILTEROBJS").short('F').long("filterobjs").action(ArgAction::SetTrue).help("filter objects within blocks"))
                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))
        )
        .subcommand(
            Command::new("mergechanges_sort")
                .about("prep_bbox_filter")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").required(true).num_args(1).help("out filename, ").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("TEMPFN").short('T').long("tempfn").num_args(1).help("temp filename, defaults to OUTFN-temp.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::allow_hyphen_values(Arg::new("FILTER").short('f').long("filter").num_args(1).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::new("FILTEROBJS").short('F').long("filterobjs").action(ArgAction::SetTrue).help("filter objects within blocks"))
                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("KEEPTEMPS").short('k').long("keeptemps").action(ArgAction::SetTrue).help("keep temp files"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
                .arg(Arg::new("SINGLETEMPFILE").short('S').long("single_temp_file").action(ArgAction::SetTrue).help("write temp data to one file"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))
        )
        .subcommand(
            Command::new("mergechanges_sort_from_existing")
                .about("prep_bbox_filter")
                .arg(Arg::new("OUTFN").short('o').long("outfn").required(true).num_args(1).help("out filename, ").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("TEMPFN").short('T').long("tempfn").required(true).num_args(1).help("temp filename, defaults to OUTFN-temp.pbf"))
                .arg(Arg::new("ISSPLIT").short('s').long("issplit").action(ArgAction::SetTrue).help("temp files were split"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))
                
        )
        .subcommand(
            Command::new("mergechanges")
                .about("prep_bbox_filter")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").required(true).num_args(1).help("out filename, ").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::allow_hyphen_values(Arg::new("FILTER").short('f').long("filter").num_args(1).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::new("FILTEROBJS").short('F').long("filterobjs").action(ArgAction::SetTrue).help("filter objects within blocks"))
                .arg(Arg::new("TIMESTAMP").short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("BROTLI").short('B').long("brotli").action(ArgAction::SetTrue).help("use brotli compression"))
                .arg(Arg::new("LZMA").short('L').long("lzma").action(ArgAction::SetTrue).help("use lzma compression"))
                .arg(Arg::new("UNCOMPRESSED").short('U').long("uncompressed").action(ArgAction::SetTrue).help("don't use any compression"))
                //.arg(Arg::new("LZ4").short("-Z").long("lz4").help("use lz4 compression"))
                .arg(Arg::new("COMPLEVEL").short('C').long("compression_level").value_parser(clap::value_parser!(u32)).num_args(1).help("compression level"))
                
        )
}

fn fix_input(input_path: &String) -> String {
    
    if input_path.ends_with("/") {
        return String::from(input_path);
    }
    
    if std::path::Path::new(input_path).is_dir() {
        return format!("{}/", input_path);
    }
    String::from(input_path)
}
    
    
    

/*
fn run_gui(defaults: &Defaults) -> Result<()> {
    
    let app = make_app();
    
    let mut settings = klask::Settings::default();
    settings.style.override_text_style = Some(egui::style::TextStyle::Monospace);
    settings.style.visuals = egui::style::Visuals::light();
    
    klask::run_app(app, settings, |matches| {
        println!("klask::run_app");
        klask::output::progress_bar("progress_bar", 0.5);
        /*match klask_messages::register_messenger() {
            Ok(()) => {},
            Err(e) => {
                message!("register_messenger failed {}", e);
                return;
            }
        }*/
        
        match run(defaults, matches) {
            Ok(()) => {},
            Err(err) => {
                message!("FAILED: {}", err);
                //message!("{}", String::from_utf8(help).unwrap());
            }
        }
    });
    Ok(())
}*/

pub fn run_clap() {
    // basic app information
    
    
    /*if klask_messages::is_klask_child_app() {
        klask_messages::register_messenger_klask().expect("!!");
    } else {*/
        register_messenger_default().expect("!!");
    //}
    
    
    let defaults = Defaults::new();
    
    
    let mut app = make_app();
    /*    .subcommand(
            Command::new("gui")
                .about("gui")
        );
    */
       
    
    let mut help = Vec::new();
    app.write_help(&mut help).expect("?");
    
    if let Some((subcommand, args)) = app.get_matches().subcommand() {
        let res: Result<()> = run(&defaults, subcommand, args);

        match res {
            Ok(()) => {}
            Err(err) => {
                message!("FAILED: {}", err);
                message!("{}", String::from_utf8(help).unwrap());
            }
        }
    } else {
        message!("FAILED: invalid command??");
        message!("{}", String::from_utf8(help).unwrap());
    }
    //message!("count: {:?}", matches);
}
