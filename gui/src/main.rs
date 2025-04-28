
use osmquadtree_utils::clap::{make_app, run, Defaults};
use osmquadtree_utils::error::{Error, Result};


   

fn run_cmd(cmd: &[&str]) -> Result<()> {
    
    let app = make_app();
    let xx = app.get_matches_from(cmd);
    if let Some((a,b)) = xx.subcommand() {
        let defaults = Defaults::new();
        println!("call {} with {:?}", a, b);
        run(&defaults, a, b)
    } else {
        Err(Error::InvalidInputError(format!("{} invalid", cmd.join(" "))))
    }
}



fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    
    
    let app = make_app();
    
    for c in &app.subcommands {
        println!("{:?}", c);
    }
    
    run_cmd(&["osmquadtree-utils", "count", "/home/james/data/planet-feb2025/"])?;
    
    Ok(())
}


