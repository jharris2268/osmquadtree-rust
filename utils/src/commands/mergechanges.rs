
use clap::{Args,ValueHint};
use crate::commands::{RunCmd,Defaults, add_trailing_slash_to_directory};
use crate::commands::sortblocks::CompressionType;
use crate::commands::run_sortblocks::get_compression_type;
use crate::error::Result;

use osmquadtree::mergechanges::{
        run_mergechanges,   run_mergechanges_sort,
        run_mergechanges_sort_inmem, run_mergechanges_sort_from_existing
};

#[derive(Args, Debug)]
pub struct MergechangesCommon {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    pub(crate) input: String,
    
    /// Output filename
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    pub(crate) outfn: String,
    
    /// specify output filename, defaults to <INPUT>-blocks.pbf
    #[arg(short='f', long, allow_hyphen_values=true)]
    pub(crate) filter: Option<String>,
    
    #[arg(short='F', long)]
    pub(crate) filter_objs: bool,
    
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
pub struct MergechangesSortInmem {
    #[command(flatten)]
    mergechanges: MergechangesCommon,
    
    /// try to use less than <RAM_GB> GB of ram
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..32))]
    ram_gb: Option<u16>,
    
    
}

impl RunCmd for MergechangesSortInmem {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        Ok(run_mergechanges_sort_inmem(
            &add_trailing_slash_to_directory(&self.mergechanges.input), 
            &self.mergechanges.outfn,
            self.mergechanges.filter.as_deref(),
            self.mergechanges.filter_objs,
            self.mergechanges.timestamp.as_deref(),
            match self.mergechanges.numchan { Some(n) => n.into(), None => defaults.numchan_default},
            match self.ram_gb { Some(n) => n.into(), None => defaults.numchan_default},
            
            get_compression_type(&self.mergechanges.compression_type, &self.mergechanges.compression_level)
        )?)
    }
}
        


#[derive(Args, Debug)]
pub struct MergechangesSort {
    #[command(flatten)]
    mergechanges: MergechangesCommon,
    
    /// Temporary file location, defaults to OUTFN-temp.pbf
    #[arg(short='T', long, value_hint=ValueHint::FilePath)]
    tempfn: Option<String>,
    
    /// Don't delete temporary files
    #[arg(short='K', long)]
    keeptemps: bool,
    
    /// Use single temporary file
    #[arg(short='S', long)]
    single_temp_file: bool,
    
    /// try to use less than <RAM_GB> GB of ram
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..32))]
    ram_gb: Option<u16>,
    
}
impl RunCmd for MergechangesSort {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        Ok(run_mergechanges_sort    (
            &add_trailing_slash_to_directory(&self.mergechanges.input),
            &self.mergechanges.outfn,
            self.tempfn.as_deref(),
            self.mergechanges.filter.as_deref(),
            self.mergechanges.filter_objs,
            self.mergechanges.timestamp.as_deref(),
            self.keeptemps,
            get_compression_type(&self.mergechanges.compression_type, &self.mergechanges.compression_level),
            match self.mergechanges.numchan { Some(n) => n.into(), None => defaults.numchan_default},
            match self.ram_gb { Some(n) => n.into(), None => defaults.numchan_default},
            self.single_temp_file,
            
        )?)
    }
}

#[derive(Args, Debug)]
pub struct Mergechanges {
    #[command(flatten)]
    mergechanges: MergechangesCommon
}

impl RunCmd for Mergechanges {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        Ok(run_mergechanges(
            &add_trailing_slash_to_directory(&self.mergechanges.input), 
            &self.mergechanges.outfn,
            self.mergechanges.filter.as_deref(),
            self.mergechanges.filter_objs,
            self.mergechanges.timestamp.as_deref(),
            get_compression_type(&self.mergechanges.compression_type, &self.mergechanges.compression_level),
            match self.mergechanges.numchan { Some(n) => n.into(), None => defaults.numchan_default},
        )?)
    }
}


#[derive(Args, Debug)]
pub struct MergechangesSortFromExisting {
    /// Output file location
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    outfn: String,
    
    
    /// Existing temporary file location, defaults to OUTFN-temp.pbf
    #[arg(short='T', long, value_hint=ValueHint::FilePath)]
    tempfn: String,
    
    /// Temporary files were split
    #[arg(short='s', long)]
    is_split: bool,
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short, long, value_parser = clap::value_parser!(u16).range(0..24))]
    pub(crate) numchan: Option<u16>,
   
    
    #[command(flatten)]
    pub(crate) compression_type: CompressionType,
    
    /// compression level
    #[arg(short='C', long, value_parser=clap::value_parser!(u32).range(0..10))]
    pub(crate) compression_level: Option<u32>,    
}

impl RunCmd for MergechangesSortFromExisting {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_mergechanges_sort_from_existing(
            &self.outfn,
            &self.tempfn,
            self.is_split,
            get_compression_type(&self.compression_type, &self.compression_level),
            match self.numchan { Some(n) => n.into(), None => defaults.numchan_default},
        )?)
        
    }
}


/*
    
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
*/
