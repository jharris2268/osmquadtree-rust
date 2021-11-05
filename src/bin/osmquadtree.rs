extern crate clap;

use clap::{value_t, App, AppSettings, Arg, SubCommand};

use osmquadtree::count::run_count;

use osmquadtree::calcqts::{run_calcqts, run_calcqts_load_existing, run_calcqts_prelim};
use osmquadtree::pbfformat::file_length;
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

use std::io::{Error, ErrorKind, Result};
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
        sort_blocks_inmem(&infn, &qtsfn, &outfn, groups, numchan, timestamp, &mut lt)?;
    } else {
        sort_blocks(
            &infn, &qtsfn, &outfn, groups, numchan, splitat, tempinmem, limit, /*write_at*/
            timestamp, keep_temps, &mut lt,
        )?;
    }
    message!("{}", lt);
    Ok(())
}

fn run_update_droplast(prfx: &str) -> Result<()> {
    let mut fl = osmquadtree::update::read_filelist(prfx);
    if fl.len() < 2 {
        return Err(Error::new(
            ErrorKind::Other,
            format!("{}filelist.json only has {} entries", prfx, fl.len()),
        ));
    }
    fl.pop();
    osmquadtree::update::write_filelist(prfx, &fl);
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
const NUMCHAN_DEFAULT: usize = 4;
const RAM_GB_DEFAULT: usize= 8;
const QT_MAX_LEVEL_DEFAULT: usize = 18;
const QT_GRAPH_LEVEL_DEFAULT: usize = 17;
const QT_BUFFER_DEFAULT: f64 = 0.05;



fn main() {
    // basic app information
    register_messenger_default().expect("!!");
    
    
    let app = App::new("osmquadtree")
        .version("0.1")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .author("James Harris")
        .subcommand(
            SubCommand::with_name("count")
                .about("uses osmquadtree to read an open street map pbf file and report basic information")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("PRIMITIVE").short("-p").long("--primitive").help("reads full primitiveblock data"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
                
        )
        .subcommand(
            SubCommand::with_name("calcqts")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                //.arg(Arg::with_name("COMBINED").short("-c").long("--combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("MODE").short("-m").long("--mode").takes_value(true).help("simplier implementation, suitable for files <8gb"))
                .arg(Arg::with_name("QT_LEVEL").short("-l").long("--qt_level").takes_value(true).help("maximum qt level, defaults to 18"))
                .arg(Arg::with_name("QT_BUFFER").short("-b").long("--qt_buffer").takes_value(true).help("qt buffer, defaults to 0.05"))
                .arg(Arg::with_name("RAM_GB").short("-r").long("--ram").takes_value(true).help("can make use of RAM_GB gb memory"))
        )
        .subcommand(
            SubCommand::with_name("calcqts_prelim")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                
        )
        .subcommand(
            SubCommand::with_name("calcqts_load_existing")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                //.arg(Arg::with_name("COMBINED").short("-c").long("--combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("STOP_AT").short("-s").long("--stop_at").takes_value(true).help("location of first file block without nodes"))
                .arg(Arg::with_name("QT_LEVEL").short("-l").long("--qt_level").takes_value(true).help("maximum qt level, defaults to 18"))
                .arg(Arg::with_name("QT_BUFFER").short("-b").long("--qt_buffer").takes_value(true).help("qt buffer, defaults to 0.05"))
        )

        .subcommand(
            SubCommand::with_name("sortblocks")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").takes_value(true).help("specify output filename, defaults to <INPUT>-blocks.pbf"))
                .arg(Arg::with_name("QT_MAX_LEVEL").short("-l").long("--qt_max_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("TARGET").short("-t").long("--target").takes_value(true).help("block target size, defaults to 40000"))
                .arg(Arg::with_name("MIN_TARGET").short("-m").long("--min_target").takes_value(true).help("block min target size, defaults to TARGET/2"))

                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("KEEPTEMPS").short("-k").long("--keeptemps").help("keep temp files"))
                .arg(Arg::with_name("RAM_GB").short("-r").long("--ram").takes_value(true).help("can make use of RAM_GB gb memory"))
        )
        .subcommand(
            SubCommand::with_name("sortblocks_inmem")
                .about("sorts osmquadtree data into blocks")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::with_name("QTSFN").short("-q").long("--qtsfn").takes_value(true).help("specify output filename, defaults to <INPUT>-qts.pbf"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").takes_value(true).help("specify output filename, defaults to <INPUT>-blocks.pbf"))
                .arg(Arg::with_name("QT_MAX_LEVEL").short("-l").long("--qt_max_level").takes_value(true).help("maximum qt level, defaults to 17"))
                .arg(Arg::with_name("TARGET").short("-t").long("--target").takes_value(true).help("block target size, defaults to 40000"))
                .arg(Arg::with_name("MIN_TARGET").short("-m").long("--min_target").takes_value(true).help("block min target size, defaults to TARGET/2"))

                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("RAM_GB").short("-r").long("--ram").takes_value(true).help("can make use of RAM_GB gb memory"))
        )
        .subcommand(
            SubCommand::with_name("update_initial")
                .about("calculate initial index")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("INFN").required(true).short("-i").long("--infn").takes_value(true).help("specify filename of orig file"))
                .arg(Arg::with_name("TIMESTAMP").required(true).short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("INITIAL_STATE").required(true).short("-s").long("--state_initial").takes_value(true).help("initial state"))
                .arg(Arg::with_name("DIFFS_LOCATION").required(true).short("-d").long("--diffs_location").takes_value(true).help("directory for downloaded osc.gz files"))
                .arg(Arg::with_name("QT_LEVEL").short("-l").long("--qt_level").takes_value(true).help("maximum qt level, defaults to 18"))
                .arg(Arg::with_name("QT_BUFFER").short("-b").long("--qt_buffer").takes_value(true).help("qt buffer, defaults to 0.05"))
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
            SubCommand::with_name("update_droplast")
                .about("calculate update")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))


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
                .arg(Arg::with_name("FILTEROBJS").short("-F").long("--filterobjs").help("filter objects within blocks"))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("RAM_GB").short("-r").long("--ram").takes_value(true).help("can make use of RAM_GB gb memory"))

        )
        .subcommand(
            SubCommand::with_name("mergechanges_sort")
                .about("prep_bbox_filter")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("OUTFN").short("-o").long("--outfn").required(true).takes_value(true).help("out filename, "))
                .arg(Arg::with_name("TEMPFN").short("-T").long("--tempfn").takes_value(true).help("temp filename, defaults to OUTFN-temp.pbf"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("FILTEROBJS").short("-F").long("--filterobjs").help("filter objects within blocks"))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("KEEPTEMPS").short("-k").long("--keeptemps").help("keep temp files"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                .arg(Arg::with_name("RAM_GB").short("-r").long("--ram").takes_value(true).help("can make use of RAM_GB gb memory"))
                .arg(Arg::with_name("SINGLETEMPFILE").short("-S").long("--single_temp_file").help("write temp data to one file"))
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
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("FILTEROBJS").short("-F").long("--filterobjs").help("filter objects within blocks"))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
                
        )
        /*.subcommand(
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
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
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
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
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
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
                .arg(Arg::with_name("SORT").short("-S").long("--short").help("sort out pbffile"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_postgresqlnull")
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("EXTENDED").short("-e").long("--extended").help("extended table spec"))
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
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
                .arg(Arg::with_name("EXTENDED").short("-e").long("--extended").help("extended table spec"))
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
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
                .arg(Arg::with_name("EXTENDED").short("-e").long("--extended").help("extended table spec"))
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("process_geometry_postgresql")
                .about("process_geometry")
                .arg(Arg::with_name("INPUT").required(true).help("Sets the input directory to use"))
                .arg(Arg::with_name("CONNECTION").short("-c").long("--connection").required(true).takes_value(true).help("connection string"))
                .arg(Arg::with_name("TABLE_PREFIX").short("-p").long("--tableprefix").required(true).takes_value(true).help("table prfx"))
                .arg(Arg::allow_hyphen_values(Arg::with_name("FILTER").short("-f").long("--filter").takes_value(true).help("filters blocks by bbox FILTER"),true))
                .arg(Arg::with_name("TIMESTAMP").short("-t").long("--timestamp").takes_value(true).help("timestamp for data"))
                .arg(Arg::with_name("FIND_MINZOOM").short("-m").long("--minzoom").help("find minzoom"))
                .arg(Arg::with_name("STYLE_NAME").short("-s").long("--style").takes_value(true).help("style json filename"))
                .arg(Arg::with_name("EXTENDED").short("-e").long("--extended").help("extended table spec"))
                .arg(Arg::with_name("EXEC_INDICES").short("-I").long("--exec_inidices").help("execute indices [can be very slow for planet imports]"))
                .arg(Arg::with_name("MAX_MINZOOM").short("-M").long("--maxminzoom").takes_value(true).help("maximum minzoom value"))
                .arg(Arg::with_name("NUMCHAN").short("-n").long("--numchan").takes_value(true).help("uses NUMCHAN parallel threads"))
        )
        .subcommand(
            SubCommand::with_name("dump_geometry_style")
                .arg(Arg::with_name("OUTPUT").required(true))
        )
        .subcommand(
            SubCommand::with_name("show_after_queries")
                .arg(Arg::with_name("TABLE_PREFIX").short("-p").long("--tableprefix").takes_value(true).help("table prfx"))
                .arg(Arg::with_name("EXTENDED").short("-e").long("--extended").help("extended table spec"))
        )*/
        ;

    let mut help = Vec::new();
    app.write_help(&mut help).expect("?");

    let res = match app.get_matches().subcommand() {
        ("count", Some(count)) => run_count(
            count.value_of("INPUT").unwrap(),
            count.is_present("PRIMITIVE"),
            value_t!(count, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            count.value_of("FILTER"),
            count.value_of("TIMESTAMP"),
        ),
        ("calcqts", Some(calcqts)) => {
            match run_calcqts(
                calcqts.value_of("INPUT").unwrap(),
                calcqts.value_of("QTSFN"),
                value_t!(calcqts, "QT_LEVEL", usize).unwrap_or(QT_MAX_LEVEL_DEFAULT),
                value_t!(calcqts, "QT_BUFFER", f64).unwrap_or(QT_BUFFER_DEFAULT),
                calcqts.value_of("MODE"),
                /* !calcqts.is_present("COMBINED"), //seperate
                true,                            //resort_waynodes*/
                value_t!(calcqts, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
                value_t!(calcqts, "RAM_GB", usize).unwrap_or(RAM_GB_DEFAULT),
            ) {
                Ok(lt) => { message!("{}", lt); Ok(()) },
                Err(e) => Err(e)
            }
        }
        ("calcqts_prelim", Some(calcqts)) => run_calcqts_prelim(
            calcqts.value_of("INPUT").unwrap(),
            calcqts.value_of("QTSFN"),
            value_t!(calcqts, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("calcqts_load_existing", Some(calcqts)) => run_calcqts_load_existing(
            calcqts.value_of("INPUT").unwrap(),
            calcqts.value_of("QTSFN"),
            value_t!(calcqts, "QT_LEVEL", usize).unwrap_or(QT_MAX_LEVEL_DEFAULT),
            value_t!(calcqts, "QT_BUFFER", f64).unwrap_or(QT_BUFFER_DEFAULT),
            match value_t!(calcqts, "STOP_AT", u64) {
                Ok(s) => Some(s),
                Err(_) => None,
            },
            value_t!(calcqts, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("sortblocks", Some(sortblocks)) => {
            run_sortblocks(
                sortblocks.value_of("INPUT").unwrap(),
                sortblocks.value_of("QTSFN"),
                sortblocks.value_of("OUTFN"),
                value_t!(sortblocks, "QT_MAX_LEVEL", usize).unwrap_or(QT_GRAPH_LEVEL_DEFAULT),
                value_t!(sortblocks, "TARGET", i64).unwrap_or(40000),
                value_t!(sortblocks, "MINTARGET", i64).unwrap_or(-1),
                false, //use_inmem
                sortblocks.value_of("TIMESTAMP"),
                value_t!(sortblocks, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
                value_t!(sortblocks, "RAM_GB", usize).unwrap_or(RAM_GB_DEFAULT),
                sortblocks.is_present("KEEPTEMPS"),
            )
        }
        ("sortblocks_inmem", Some(sortblocks)) => {
            run_sortblocks(
                sortblocks.value_of("INPUT").unwrap(),
                sortblocks.value_of("QTSFN"),
                sortblocks.value_of("OUTFN"),
                value_t!(sortblocks, "QT_MAX_LEVEL", usize).unwrap_or(QT_GRAPH_LEVEL_DEFAULT),
                value_t!(sortblocks, "TARGET", i64).unwrap_or(40000),
                value_t!(sortblocks, "MINTARGET", i64).unwrap_or(-1),
                true, //use_inmem
                sortblocks.value_of("TIMESTAMP"),
                value_t!(sortblocks, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
                value_t!(sortblocks, "RAM_GB", usize).unwrap_or(RAM_GB_DEFAULT),
                false,
            )
        }
        ("update_initial", Some(update)) => run_update_initial(
            update.value_of("INPUT").unwrap(),
            update.value_of("INFN").unwrap(),
            update.value_of("TIMESTAMP").unwrap(),
            value_t!(update, "INITIAL_STATE", i64).unwrap_or(0),
            update.value_of("DIFFS_LOCATION").unwrap(),
            value_t!(update, "QT_LEVEL", usize).unwrap_or(QT_MAX_LEVEL_DEFAULT),
            value_t!(update, "QT_BUFFER", f64).unwrap_or(QT_BUFFER_DEFAULT),
            value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("update", Some(update)) => {
            run_update(
                update.value_of("INPUT").unwrap(),
                value_t!(update, "LIMIT", usize).unwrap_or(0),
                false, //as_demo
                value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("update_demo", Some(update)) => {
            run_update(
                update.value_of("INPUT").unwrap(),
                value_t!(update, "LIMIT", usize).unwrap_or(0),
                true, //as_demo
                value_t!(update, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("update_droplast", Some(update)) => run_update_droplast(update.value_of("INPUT").unwrap()),
        ("write_index_file", Some(write)) => write_index_file_w(
            write.value_of("INPUT").unwrap(),
            write.value_of("OUTFN"),
            value_t!(write, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("mergechanges_sort_inmem", Some(filter)) => run_mergechanges_sort_inmem(
            filter.value_of("INPUT").unwrap(),
            filter.value_of("OUTFN").unwrap(),
            filter.value_of("FILTER"),
            filter.is_present("FILTEROBJS"),
            filter.value_of("TIMESTAMP"),
            value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            value_t!(filter, "RAM_GB", usize).unwrap_or(RAM_GB_DEFAULT),
        ),
        ("mergechanges_sort", Some(filter)) => run_mergechanges_sort(
            filter.value_of("INPUT").unwrap(),
            filter.value_of("OUTFN").unwrap(),
            filter.value_of("TEMPFN"),
            filter.value_of("FILTER"),
            filter.is_present("FILTEROBJS"),
            filter.value_of("TIMESTAMP"),
            filter.is_present("KEEPTEMPS"),
            value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            value_t!(filter, "RAM_GB", usize).unwrap_or(RAM_GB_DEFAULT),
            filter.is_present("SINGLETEMPFILE")

        ),
        ("mergechanges_sort_from_existing", Some(filter)) => run_mergechanges_sort_from_existing(
            filter.value_of("OUTFN").unwrap(),
            filter.value_of("TEMPFN").unwrap(),
            filter.is_present("ISSPLIT"),
            value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            
        ),
        ("mergechanges", Some(filter)) => run_mergechanges(
            filter.value_of("INPUT").unwrap(),
            filter.value_of("OUTFN").unwrap(),
            filter.value_of("FILTER"),
            filter.is_present("FILTEROBJS"),
            filter.value_of("TIMESTAMP"),
            value_t!(filter, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        /*("process_geometry_null", Some(geom)) => process_geometry(
            geom.value_of("INPUT").unwrap(),
            OutputType::None,
            geom.value_of("FILTER"),
            geom.value_of("TIMESTAMP"),
            geom.is_present("FIND_MINZOOM"),
            geom.value_of("STYLE_NAME"),
            get_i64(geom.value_of("MAX_MINZOOM")),
            value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("process_geometry_json", Some(geom)) => process_geometry(
            geom.value_of("INPUT").unwrap(),
            OutputType::Json(String::from(geom.value_of("OUTFN").unwrap())),
            geom.value_of("FILTER"),
            geom.value_of("TIMESTAMP"),
            geom.is_present("FIND_MINZOOM"),
            geom.value_of("STYLE_NAME"),
            get_i64(geom.value_of("MAX_MINZOOM")),
            value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("process_geometry_tiled_json", Some(geom)) => process_geometry(
            geom.value_of("INPUT").unwrap(),
            OutputType::TiledJson(String::from(geom.value_of("OUTFN").unwrap())),
            geom.value_of("FILTER"),
            geom.value_of("TIMESTAMP"),
            geom.is_present("FIND_MINZOOM"),
            geom.value_of("STYLE_NAME"),
            get_i64(geom.value_of("MAX_MINZOOM")),
            value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
        ),
        ("process_geometry_pbffile", Some(geom)) => {
            
            let ot = if geom.is_present("SORT") {
                OutputType::PbfFileSorted(String::from(geom.value_of("OUTFN").unwrap()))
            } else {
                OutputType::PbfFile(String::from(geom.value_of("OUTFN").unwrap()))
            };
            
            process_geometry(
                geom.value_of("INPUT").unwrap(),
                ot,
                geom.value_of("FILTER"),
                geom.value_of("TIMESTAMP"),
                geom.is_present("FIND_MINZOOM"),
                geom.value_of("STYLE_NAME"),
                get_i64(geom.value_of("MAX_MINZOOM")),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        },
        ("process_geometry_postgresqlnull", Some(geom)) => {
            let pc = PostgresqlConnection::Null;
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
                get_i64(geom.value_of("MAX_MINZOOM")),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("process_geometry_postgresqlblob", Some(geom)) => {
            let pc =
                PostgresqlConnection::CopyFilePrfx(String::from(geom.value_of("OUTFN").unwrap()));
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
                get_i64(geom.value_of("MAX_MINZOOM")),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("process_geometry_postgresqlblob_pbf", Some(geom)) => {
            let pc =
                PostgresqlConnection::CopyFileBlob(String::from(geom.value_of("OUTFN").unwrap()));
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
                get_i64(geom.value_of("MAX_MINZOOM")),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("process_geometry_postgresql", Some(geom)) => {
            let pc = PostgresqlConnection::Connection((
                String::from(geom.value_of("CONNECTION").unwrap()),
                String::from(geom.value_of("TABLE_PREFIX").unwrap()),
                geom.is_present("EXEC_INDICES"),
            ));
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
                get_i64(geom.value_of("MAX_MINZOOM")),
                value_t!(geom, "NUMCHAN", usize).unwrap_or(NUMCHAN_DEFAULT),
            )
        }
        ("dump_geometry_style", Some(geom)) => dump_geometry_style(geom.value_of("OUTPUT")),
        
        ("show_after_queries", Some(geom)) => {
            (|| {
                let pc = PostgresqlConnection::Connection((String::new(),String::new(),true));
                let po = if geom.is_present("EXTENDED") {
                    PostgresqlOptions::extended(pc, &GeometryStyle::default())
                } else {
                    PostgresqlOptions::osm2pgsql(pc, &GeometryStyle::default())
                };
                let lz = if po.extended { Some(Vec::from([(String::from("lz6_"),6,true),(String::from("lz9_"),9,false),(String::from("lz11_"),11,false)])) } else {None};
                message!("{}", prepare_tables(geom.value_of("TABLE_PREFIX"), 
                    &po.table_spec, 
                    po.extended,
                    po.extended,
                    &lz)?.2.join("\n"));
                Ok(())
            })()
        },
        */
        _ => Err(Error::new(ErrorKind::Other, "??")),
        
    };

    match res {
        Ok(()) => {}
        Err(err) => {
            message!("FAILED: {}", err);
            message!("{}", String::from_utf8(help).unwrap());
        }
    }

    //message!("count: {:?}", matches);
}
