
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

fn dump_cmd(indent: usize, cmd: &clap::Command) {
    println!("{}app: {}, {} subcommands, {} args", " ".repeat(indent*2), cmd.get_name(), cmd.get_subcommands().count(), cmd.get_arguments().count());
    for a in cmd.get_arguments() {
        println!("{}{} {} {} {} {   }", 
            " ".repeat(indent*2+4),
            a.get_id(),
            match a.get_short() {Some(s) => format!("{}",s), None => String::from("-")},
            match a.get_long() {Some(s) => s.into(), None => "-"},
            match a.get_num_args() {Some(s) => format!("{}",s), None => String::from("-")},
            match a.get_help() {Some(s) => format!("{}",s), None => String::from("-")},
        );
    }
    
    for c in cmd.get_subcommands() {
        dump_cmd(indent+1, c);
    }
        
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    
    
    let app = make_app();
    dump_cmd(0, &app);
    
    
    
    run_cmd(&["osmquadtree-utils", "count", "/home/james/data/planet-feb2025/", "--filter", "-1.5,49.5,3.0,52.5"])?;
    
    Ok(())
}


