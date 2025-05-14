use clap::{Parser, Subcommand};

use crate::commands::{Count,RunCmd,Defaults,Calcqts,CalcqtsPrelim,CalcqtsLoadExisting, Sortblocks, SortblocksInmem};
use crate::error::Result;


#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// uses osmquadtree to read an open street map pbf file and report basic information
    Count(Count),
    /// prepare an updatable osmquadtree instance
    Setup,
    
    /// calculates quadtrees for each element of a planet or extract pbf file
    Calcqts(Calcqts),
    
    /// prepares way-nodes file for calculating quadtrees: see calcqts-load-existing
    CalcqtsPrelim(CalcqtsPrelim),
    
    /// calculates quadtrees for each element of a planet or extract pbf file, continuing from calcqts-prelim
    CalcqtsLoadExisting(CalcqtsLoadExisting),
    
    /// Incorporate quadtrees into planet file, sort by quadtree value into blocks
    Sortblocks(Sortblocks),
    
    /// Incorporate quadtrees into planet file, sort by quadtree value into blocks. Run in memory.
    SortblocksInmem(SortblocksInmem)
}

impl RunCmd for Cli {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        match &self.command {
            
            Commands::Count(count) => count.run(defaults),
            Commands::Setup => crate::setup::run(defaults.ram_gb_default, defaults.numchan_default),
            Commands::Calcqts(calcqts) => calcqts.run(defaults),
            Commands::CalcqtsPrelim(calcqts_prelim) => calcqts_prelim.run(defaults),
            Commands::CalcqtsLoadExisting(calcqts_load_existing) => calcqts_load_existing.run(defaults),
            Commands::Sortblocks(sortblocks) => sortblocks.run(defaults),
            Commands::SortblocksInmem(sortblocks) => sortblocks.run(defaults),
        }
    }
}
        
