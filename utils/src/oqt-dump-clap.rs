use osmquadtree_utils::clap::{make_app, run, Defaults};
use osmquadtree::defaultlogger::register_messenger_default;

fn main() {
    register_messenger_default().expect("!!");
    
    let cmd = make_app();
    dump_cmd(0, &cmd);
    
    let cmd2 = make_app();
    let aa = vec!["osmquadtree-utils", "count", "/home/james/data/planet-feb2025", "-f", "-1.5,49.5,2.5,54.0"];
    let xx = cmd2.get_matches_from(&aa);
    if let Some((a,b)) = xx.subcommand() {
        let defaults = Defaults::new();
        println!("call {} with {:?}", a, b);
        match run(&defaults, a, b) {
            Ok(_) => {},
            Err(err) => {
                println!("FAILED: {}", err);
                
            }
        }
    } else {
        println!("?? {:?}", xx);
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
