//use osmquadtree_utils::clap::{make_app, run, Defaults};
use osmquadtree_utils::commands::{make_app};//, run_app, Defaults};
use osmquadtree::defaultlogger::register_messenger_default;

fn main() {
    register_messenger_default().expect("!!");
    
    let cmd = make_app();
    dump_cmd(0, &cmd);
    
/*    
    let aa = vec!["osmquadtree-utils", "count", "/home/james/data/planet-feb2025/", "-f", "-0.5,51.2,0.5,51.8"];
    let defaults = Defaults::new();
    match run_app(&defaults, &aa) {
        Ok(_) => {},
        Err(err) => {
            println!("FAILED: {}", err);
            
        }
    }*/
}

fn dump_cmd(indent: usize, cmd: &clap::Command) {
    println!("{}app: {}, {} subcommands, {} args", " ".repeat(indent*2), cmd.get_name(), cmd.get_subcommands().count(), cmd.get_arguments().count());
    for a in cmd.get_arguments() {
        println!("{}\t{:2} | {:18.18} | {} | {:18.18} | {} | {:8.8} | {:7.7} | {:50.50} | {}", 
            " ".repeat(indent*2+4),
            match a.get_display_order() { 999 => "-".into(), x => format!("{}",x) },
            a.get_id(),
            match a.get_short() {Some(s) => format!("{}",s), None => String::from("-")},
            match a.get_long() {Some(s) => s.into(), None => "-"},
            match a.get_num_args() {Some(s) => format!("{}",s), None => String::from("-")},
            format!("{:?}", a.get_value_hint()),
            format!("{:?}", a.get_action()),
            format!("{:?}", a.get_value_parser()),
            match a.get_help() {Some(s) => format!("{}",s), None => String::from("-")},
        );
    }
    
    for c in cmd.get_subcommands() {
        dump_cmd(indent+1, c);
    }
        
        
    

}
