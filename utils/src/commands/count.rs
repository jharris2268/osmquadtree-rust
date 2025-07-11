use osmquadtree::count::run_count;
use clap::{Args,ValueHint};
use crate::commands::{RunCmd,Defaults,add_trailing_slash_to_directory};
use crate::error::Result;


#[derive(Args, Debug)]
pub struct Count {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    ///reads full primitiveblock data 
    #[arg(short)] #[arg(long)]
    primitive: bool,
    
    ///filters blocks by bbox FILTER
    #[arg(short)] #[arg(long)]
    #[arg(allow_hyphen_values=true)]
    filter: Option<String>,
    
    ///includes updates up to timestamp
    #[arg(short)] #[arg(long)]
    timestamp: Option<String>,
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short)] #[arg(long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>
}
impl RunCmd for Count {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_count(
            &add_trailing_slash_to_directory(&self.input),
            self.primitive,
            match self.numchan { None => defaults.numchan_default, Some(n) => n.into() },
            self.filter.as_deref(),
            self.timestamp.as_deref(),
            
        )?)
        
        
    }
}        
