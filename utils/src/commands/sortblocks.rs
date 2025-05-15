
use clap::{Args,ValueHint};
use crate::commands::{RunCmd,Defaults,QT_GRAPH_LEVEL_DEFAULT};

use crate::error::Result;

use crate::commands::run_sortblocks::run_sortblocks;

#[derive(Args, Debug)]
pub struct SortblocksCommon {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    pub(crate) input: String,
    
    /// specify input quadtrees filename, defaults to <INPUT>-qts.pbf. If not present call osmquadtree-utils calcqts first
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    pub(crate) qtsfn: Option<String>,
    
    /// specify output filename, defaults to <INPUT>-blocks.pbf
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    pub(crate) outfn: Option<String>,
    
    /// qtlevel
    #[arg(short='l', long, default_value_t = QT_GRAPH_LEVEL_DEFAULT)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..18))]
    pub(crate) max_qt_graph_level: u16,
    
    
    /// target number of elements per block    
    #[arg(short='T', long, default_value_t = 40_000)]
    pub(crate) target: i64,   
    
    /// target minimum number of elements per block, defaults to TARGET/2
    #[arg(short, long)]
    pub(crate) min_target: Option<i64>,
    
    ///includes updates up to timestamp
    #[arg(short, long)]
    pub(crate) timestamp: Option<String>,
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short, long, value_parser = clap::value_parser!(u16).range(0..24))]
    pub(crate) numchan: Option<u16>,
    
    
    
    
    
    
    #[command(flatten)]
    pub(crate) compression_type: CompressionType,
    
    /// compression level
    #[arg(short='C', long, value_parser=clap::value_parser!(u32).range(0..10))]
    pub(crate) compression_level: Option<u32>,    
    
}

#[derive(Args, Debug)]
pub struct Sortblocks {
    #[command(flatten)]
    pub(crate) sortblocks: SortblocksCommon,
    
    /// keep temporary files
    #[arg(short, long)]
    pub(crate) keeptemps: bool,
    
    /// try to use less than <RAM_GB> GB of ram
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..32))]
    pub(crate) ram_gb: Option<u16>,
}
    


#[derive(Args, Debug)]
#[group(required=false, multiple=false)]
pub(crate) struct CompressionType {
    /// use brotli compression algorithm
    #[arg(short='B', long)]
    pub brotli: bool,
    
    /// use lzma compression algorithm
    #[arg(short='L', long)]
    pub lzma: bool,
    
    /// don't use any compression
    #[arg(short='U', long)]
    pub uncompressed: bool,
}

#[derive(Debug)]
pub(crate) enum SortblocksType {
    Normal((bool, Option<u16>)),
    Inmem
}


impl RunCmd for Sortblocks {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_sortblocks(&self.sortblocks, SortblocksType::Normal((self.keeptemps, self.ram_gb.clone())), defaults)?)
        
    }
}        

#[derive(Args, Debug)]
pub struct SortblocksInmem {
    #[command(flatten)]
    pub sortblocks: SortblocksCommon
}

impl RunCmd for SortblocksInmem {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_sortblocks(&self.sortblocks, SortblocksType::Inmem, defaults)?)
        
    }
}        



/*
        
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
*/
