use clap::{Args, Parser, Subcommand,ValueHint};
use osmquadtree_utils::error::Result;
use osmquadtree::{message, defaultlogger::register_messenger_default};

use osmquadtree_utils::commands::{Cli,Commands,Defaults,RunCmd};





fn main() {
    
    
    
    
    register_messenger_default().expect("!!");
    let defaults = Defaults::new();
    
    let cli = Cli::parse();

    // You can check for the existence of subcommands, and if found use their
    // matches just as you would the top level cmd
    let res = cli.run(&defaults);
    
    match res {
        Ok(()) => {},
        Err(e) => {
            message!("failed {:?}", e);
        }
    }
}


