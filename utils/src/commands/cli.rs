use clap::{Args, Parser, Subcommand,ValueHint};

use crate::commands::{Count,RunCmd,Defaults};
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
}

impl RunCmd for Cli {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        match &self.command {
            
            Commands::Count(count) => count.run(defaults),
        }
    }
}
        
