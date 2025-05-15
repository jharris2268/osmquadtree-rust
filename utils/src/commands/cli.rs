use clap::{Parser, Subcommand};

use crate::commands::{RunCmd,Defaults};
use crate::commands::Count;
use crate::commands::{Calcqts,CalcqtsPrelim,CalcqtsLoadExisting};
use crate::commands::{Sortblocks, SortblocksInmem};
use crate::commands::{Update, UpdateDemo, UpdateDropLast};
use crate::commands::{UpdateInitial, WriteIndexFile};
use crate::commands::{Mergechanges, MergechangesSort, MergechangesSortInmem, MergechangesSortFromExisting};
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
    SortblocksInmem(SortblocksInmem),
    
    
    /// Prepare sorted osmquadtree planet for updates
    UpdateInitial(UpdateInitial),
    
    /// Calculate update
    Update(Update),
    
    /// Calculate update, write file with '-rust' suffix and don't add to filelist
    UpdateDemo(UpdateDemo), 
    
    /// Remove last upate
    UpdateDropLast(UpdateDropLast),
    
    /// Write index file for sorted pbf
    WriteIndexFile(WriteIndexFile),
    
    
    /// Merge and filter sorted planet and updates, leave in quadtree blocks
    Mergechanges(Mergechanges),
    
    /// Merge and filter sorted planet and updates, sort back into normal order
    MergechangesSort(MergechangesSort),

    /// Merge and filter sorted planet and updates, sort back into normal order. Run in memory.
    MergechangesSortInmem(MergechangesSortInmem),
    
    /// Merge and filter from temporary files retained after mergechanges-sort
    MergechangesSortFromExisting(MergechangesSortFromExisting)
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
            
            Commands::Update(update) => update.run(defaults),
            Commands::UpdateDemo(update) => update.run(defaults),
            Commands::UpdateDropLast(update) => update.run(defaults),
            
            Commands::UpdateInitial(update) => update.run(defaults),
            Commands::WriteIndexFile(write) => write.run(defaults),
            
            
            Commands::Mergechanges(merge) => merge.run(defaults),
            Commands::MergechangesSort(merge) => merge.run(defaults),
            Commands::MergechangesSortInmem(merge) => merge.run(defaults),
            Commands::MergechangesSortFromExisting(merge) => merge.run(defaults),
        }
    }
}
        
