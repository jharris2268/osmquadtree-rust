extern crate clap;

use clap::{value_t, App, AppSettings, Arg, SubCommand};

use osmquadtree::count::run_count;

use osmquadtree::calcqts::{run_calcqts, run_calcqts_inmem};
use osmquadtree::sortblocks::{find_groups, sort_blocks, sort_blocks_inmem,QuadtreeTree};
use osmquadtree::update::{run_update, run_update_initial,write_index_file};
use osmquadtree::utils::{parse_timestamp};
use osmquadtree::pbfformat::read_file_block::file_length;

use osmquadtree::mergechanges::{run_mergechanges_sort_inmem,run_mergechanges_sort,run_mergechanges_sort_from_existing,run_mergechanges};
use osmquadtree::geometry::{process_geometry,GeometryStyle,OutputType};
use osmquadtree::geometry::postgresql::{PostgresqlConnection,PostgresqlOptions};

use std::sync::Arc;
use std::io::{Error, ErrorKind, Result};

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
    keep_temps: bool,
) -> Result<()> {
    let mut splitat = 0i64;
    let tempinmem = file_length(infn) < 512*1024*1024;
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

    let groups: Arc<QuadtreeTree> = Arc::from(find_groups(&qtsfn, numchan, maxdepth, target, mintarget)?);
    println!("groups: {} {}", groups.len(), groups.total_weight());
    if limit == 0 {
        limit = 40000000usize / (groups.len() / (splitat as usize));
        if tempinmem {
            limit = usize::max(1000, limit / 10);
        }
    }
    if use_inmem {
        sort_blocks_inmem(&infn, &qtsfn, &outfn, groups, numchan, timestamp)?;
    } else {
        sort_blocks(
            &infn, &qtsfn, &outfn, groups, numchan, splitat, tempinmem, limit/*write_at*/, timestamp, keep_temps
        )?;
    }

    Ok(())
}

fn run_update_w(prfx: &str, limit: usize, as_demo: bool, numchan: usize) -> Result<()> {
    let t = run_update(prfx, limit, as_demo, numchan)?;
    for (a, b) in t {
        println!("{:-50}: {:0.1}s", a, b);
    }
    Ok(())
}

fn write_index_file_w(prfx: &str, outfn: Option<&str>, numchan: usize) -> Result<()> {
    match outfn {
        Some(o) => write_index_file(prfx, o, numchan),
        None => write_index_file(prfx, "", numchan)
    };
    Ok(())
}

fn dump_geometry_style(outfn: Option<&str>) -> Result<()> {
    let outfn = match outfn {
        Some(o) => String::from(o),
        None => String::from("default_style.json"),
    };
    let mut f = std::fs::File::create(&outfn)?;
    serde_json::to_writer_pretty(&mut f, &GeometryStyle::default())?;
    Ok(())
}
    

const NUMCHAN_DEFAULT: usize = 4;

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
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))            
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
        )
        .subcommand(
            SubCommand::with_name("sortblocks")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").takes_value(true).help("specify output filename, defaults to <INPUT>-blocks.pbf"))
                .arg(Arg::with_name("QT_MAX_LEVEL").short("-q").long("--qt_max_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("TARGET").short("-t").long("--target").takes_value(true).help("block target size, defaults to 40000"))
                .arg(Arg::with_name("MIN_TARGET").short("-m").long("--min_target").takes_value(true).help("block min target size, defaults to TARGET/2"))
                
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("KEEPTEMPS").short("-k").long("--keeptemps").help("keep temp files"))            
        )
        .subcommand(
            SubCommand::with_name("sortblocks_inmem")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").takes_value(true).help("specify output filename, defaults to <INPUT>-blocks.pbf"))
                .arg(Arg::with_name("QT_MAX_LEVEL").short("-q").long("--qt_max_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("TARGET").short("-t").long("--target").takes_value(true).help("block target size, defaults to 40000"))
                .arg(Arg::with_name("MIN_TARGET").short("-m").long("--min_target").takes_value(true).help("block min target size, defaults to TARGET/2"))
                
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("update_initial")   
                .about("calculate initial index")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("INFN").required(true).short("-i").long("--infn").takes_value(true).help("specify filename of orig file"))
                .arg(Arg::with_name("TIMESTAMP").required(true).short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("INITIAL_STATE").required(true).short("-s").long("--state_initial").takes_value(true).help("initial state"))
                .arg(Arg::with_name("DIFFS_LOCATION").required(true).short("-d").long("--diffs_location").takes_value(true).help("directory for downloaded osc.gz files"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        .subcommand(
            SubCommand::with_name("update")   
                .about("calculate update")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("LIMIT").short("-l").long("--limit").help("only run LIMIT updates"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        .subcommand(
            SubCommand::with_name("update_demo")   
                .about("calculate update")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("LIMIT").short("-l").long("--limit").help("only run LIMIT updates"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        .subcommand(
            SubCommand::with_name("write_index_file")   
                .about("write pbf index file")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").takes_value(true).help("out filename, defaults to INPUT-index.pbf"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        .subcommand(
            SubCommand::with_name("mergechanges_sort_inmem")   
                .about("prep_bbox_filter")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").required(true).takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))            
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        .subcommand(
            SubCommand::with_name("mergechanges_sort")   
                .about("prep_bbox_filter")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::with_name("TEMPFN").short("-T").long("--tempfn").takes_value(true).help("temp filename, defaults to OUTFN-temp.pbf"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").required(true).takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("KEEPTEMPS").short("-k").long("--keeptemps").help("keep temp files"))            
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("mergechanges_sort_from_existing")   
                .about("prep_bbox_filter")
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::with_name("TEMPFN").short("-T").long("--tempfn").required(true).takes_value(true).help("temp filename, defaults to OUTFN-temp.pbf"))
                .arg(Arg::with_name("ISSPLIT").short("-s").long("--issplit").help("temp files were split"))            
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("mergechanges")   
                .about("prep_bbox_filter")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").required(true).takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("FILTEROBJS").short("-F").long("filterobjs").help("filter objects within blocks"))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_null")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_json")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_tiled_json")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_pbffile")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_postgresqlblob")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("EXTENDED").short("-e").long("--exteded").help("extended table spec"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_postgresqlblob_pbf")   
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("EXTENDED").short("-e").long("--exteded").help("extended table spec"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        
        .subcommand(
            SubCommand::with_name("dump_geometry_style")
                .arg(Arg::with_name("OUTPUT").required(true))
        )
        ;
        

    let mut help = Vec::new();
    app.write_help(&mut help).expect("?");

    let res = match app.get_matches().subcommand() {
        ("count", Some(count)) => run_count(
            count.value_of("INPUT").unwrap(),
            count.is_present("PRIMITIVE"),
            value_t!(count, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            count.value_of("FILTER"),
        ),
        ("calcqts", Some(calcqts)) => {
            run_calcqts(
                calcqts.value_of("INPUT").unwrap(),
                calcqts.value_of("QTSFN"),
                value_t!(calcqts, "QT_LEVEL", usize).unwrap_or(17),
                value_t!(calcqts, "QT_BUFFER", f64).unwrap_or(0.05),
                calcqts.is_present("SIMPLE"),
                !calcqts.is_present("COMBINED"), //seperate
                true,                            //resort_waynodes
                value_t!(calcqts, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("calcqts_inmem", Some(calcqts)) => run_calcqts_inmem(
            calcqts.value_of("INPUT").unwrap(),
            calcqts.value_of("QTSFN"),
            value_t!(calcqts, "QT_LEVEL", usize).unwrap_or(17),
            value_t!(calcqts, "QT_BUFFER", f64).unwrap_or(0.05),
            value_t!(calcqts, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("sortblocks", Some(sortblocks)) => {
            run_sortblocks(
                sortblocks.value_of("INPUT").unwrap(),
                sortblocks.value_of("QTSFN"),
                sortblocks.value_of("OUTFN"),
                value_t!(sortblocks, "QT_MAX_LEVEL", usize).unwrap_or(17),
                value_t!(sortblocks, "TARGET", i64).unwrap_or(40000),
                value_t!(sortblocks, "MINTARGET", i64).unwrap_or(-1),
                false, //use_inmem
                sortblocks.value_of("TIMESTAMP"),
                value_t!(sortblocks, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
                sortblocks.is_present("KEEPTEMPS")
            )
        }
        ("sortblocks_inmem", Some(sortblocks)) => {
            run_sortblocks(
                sortblocks.value_of("INPUT").unwrap(),
                sortblocks.value_of("QTSFN"),
                sortblocks.value_of("OUTFN"),
                value_t!(sortblocks, "QT_MAX_LEVEL", usize).unwrap_or(17),
                value_t!(sortblocks, "TARGET", i64).unwrap_or(40000),
                value_t!(sortblocks, "MINTARGET", i64).unwrap_or(-1),
                true, //use_inmem
                sortblocks.value_of("TIMESTAMP"),
                value_t!(sortblocks, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
                false
            )
        }
        ("update_initial", Some(update)) => run_update_initial(
            update.value_of("INPUT").unwrap(),
            update.value_of("INFN").unwrap(),
            update.value_of("TIMESTAMP").unwrap(),
            value_t!(update, "INITIAL_STATE", i64).unwrap_or(0),
            update.value_of("DIFFS_LOCATION").unwrap(),
            value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("update", Some(update)) => {
            run_update_w(
                update.value_of("INPUT").unwrap(),
                value_t!(update, "LIMIT", usize).unwrap_or(0),
                false, //as_demo
                value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        },
        ("update_demo", Some(update)) => {
            run_update_w(
                update.value_of("INPUT").unwrap(),
                value_t!(update, "LIMIT", usize).unwrap_or(0),
                true, //as_demo
                value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
            
        },
        ("write_index_file", Some(write)) => {
            write_index_file_w(
                write.value_of("INPUT").unwrap(),
                write.value_of("OUTFN"),
                value_t!(write, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("mergechanges_sort_inmem", Some(filter)) => {
            run_mergechanges_sort_inmem(
                filter.value_of("INPUT").unwrap(),
                filter.value_of("OUTFN").unwrap(),
                filter.value_of("FILTER"),
                filter.value_of("TIMESTAMP"),
                value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        }
        ("mergechanges_sort", Some(filter)) => {
            run_mergechanges_sort(
                filter.value_of("INPUT").unwrap(),
                filter.value_of("OUTFN").unwrap(),
                filter.value_of("TEMPFN"),
                filter.value_of("FILTER"),
                filter.value_of("TIMESTAMP"),
                filter.is_present("KEEPTEMPS"),
                value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("mergechanges_sort_from_existing", Some(filter)) => {
            run_mergechanges_sort_from_existing(
                filter.value_of("OUTFN").unwrap(),
                filter.value_of("TEMPFN").unwrap(),
                filter.is_present("ISSPLIT"),
                value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("mergechanges", Some(filter)) => {
            run_mergechanges(
                filter.value_of("INPUT").unwrap(),
                filter.value_of("OUTFN").unwrap(),
                filter.value_of("FILTER"),
                filter.is_present("FILTEROBJS"),
                filter.value_of("TIMESTAMP"),
                value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_null", Some(geom)) => {
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::None,
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_json", Some(geom)) => {
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::Json(String::from(geom.value_of("OUTFN").unwrap())),
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_tiled_json", Some(geom)) => {
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::TiledJson(String::from(geom.value_of("OUTFN").unwrap())),
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_pbffile", Some(geom)) => {
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::PbfFile(String::from(geom.value_of("OUTFN").unwrap())),
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_postgresqlblob", Some(geom)) => {
            let pc = PostgresqlConnection::CopyFilePrfx(String::from(geom.value_of("OUTFN").unwrap()));
            let po = if geom.is_present("EXTENDED") {
                PostgresqlOptions::extended(pc, &GeometryStyle::default())
            } else {
                PostgresqlOptions::osm2pgsql(pc, &GeometryStyle::default())
            };
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::Postgresql(po),
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("process_geometry_postgresqlblob_pbf", Some(geom)) => {
            let pc = PostgresqlConnection::CopyFileBlob(String::from(geom.value_of("OUTFN").unwrap()));
            let po = if geom.is_present("EXTENDED") {
                PostgresqlOptions::extended(pc, &GeometryStyle::default())
            } else {
                PostgresqlOptions::osm2pgsql(pc, &GeometryStyle::default())
            };
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                OutputType::Postgresql(po),
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT)
            )
        },
        ("dump_geometry_style", Some(geom)) => {
            dump_geometry_style(geom.value_of("OUTPUT"))
        },
        _ => Err(Error::new(ErrorKind::Other, "??")),
    };

    match res {
        Ok(()) => {}
        Err(err) => {
            println!("FAILED: {}", err);
            println!("{}", String::from_utf8(help).unwrap());
        }
    }

    //println!("count: {:?}", matches);
}
