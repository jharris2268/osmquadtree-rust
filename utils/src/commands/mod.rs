mod count;
mod calcqts;
mod sortblocks;
mod run_sortblocks;
mod update;
mod update_initial;
mod mergechanges;
mod cli;


use clap::{Command,CommandFactory,Parser};

pub use count::Count;
pub use calcqts::{Calcqts,CalcqtsPrelim,CalcqtsLoadExisting};
pub use sortblocks::{Sortblocks,SortblocksInmem};
pub use update::{Update,UpdateDemo,UpdateDropLast};
pub use update_initial::{UpdateInitial, WriteIndexFile};
pub use mergechanges::{Mergechanges, MergechangesSortInmem, MergechangesSort, MergechangesSortFromExisting};
pub use cli::{Commands,Cli};


use sysinfo::{System};
use osmquadtree::message;
use crate::error::Result;


const RAM_GB_DEFAULT: usize= 8;
const QT_MAX_LEVEL_DEFAULT: u16 = 18;
const QT_GRAPH_LEVEL_DEFAULT: u16 = 17;
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

pub trait RunCmd {
    fn run(&self, defaults: &Defaults) -> Result<()>;
}
  

pub fn make_app() -> Command {
    Cli::command()
}

pub fn run_app<T: AsRef<std::ffi::OsStr> + std::fmt::Display >(defaults: &Defaults, args: &[T]) -> Result<()> {
    
    let cli = Cli::try_parse_from(args)?;
    
    cli.run(defaults)
}

pub fn add_trailing_slash_to_directory(input_path: &str) -> String {
    if input_path.ends_with("/") {
        return String::from(input_path);
    }
    
    if std::path::Path::new(input_path).is_dir() {
        return format!("{}/", input_path);
    }
    String::from(input_path)
}
    
