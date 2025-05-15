use osmquadtree::update::{run_update};
use osmquadtree::pbfformat::{read_filelist, write_filelist};
use clap::{Args,ValueHint};
use crate::commands::{RunCmd,Defaults};
use crate::error::{Error, Result};


#[derive(Args, Debug)]
pub struct Update {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    /// only run LIMIT updates
    #[arg(short, long, value_parser = clap::value_parser!(u16).range(1..))]
    limit: Option<u16>, 
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short)] #[arg(long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>
}

impl RunCmd for Update {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_update(
            &self.input,
            match self.limit { None => 0, Some(l) => l.into() },
            false,
            match self.numchan { None => defaults.numchan_default, Some(n) => n.into() },
        )?)        
    }
}

#[derive(Args, Debug)]
pub struct UpdateDemo {
    #[command(flatten)]
    update: Update
}

impl RunCmd for UpdateDemo {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_update(
            &self.update.input,
            match self.update.limit { None => 0, Some(l) => l.into() },
            true,
            match self.update.numchan { None => defaults.numchan_default, Some(n) => n.into() },
        )?)        
    }
}

#[derive(Args, Debug)]
pub struct UpdateDropLast {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
}




impl RunCmd for UpdateDropLast {
    fn run(&self, _defaults: &Defaults) -> Result<()> {
        
        let mut fl = read_filelist(&self.input);
        if fl.len() < 2 {
            return Err(Error::InvalidInputError(
                format!("{}filelist.json only has {} entries", &self.input, fl.len())
            ));
        }
        fl.pop();
        write_filelist(&self.input, &fl);
        Ok(())
        
        
    }
}



/*
        .subcommand(
            Command::new("update")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("LIMIT").short('l').long("limit").value_parser(clap::value_parser!(usize)).num_args(1).help("only run LIMIT updates"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("update_demo")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("LIMIT").short('l').long("limit").value_parser(clap::value_parser!(usize)).num_args(1).help("only run LIMIT updates"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )
        .subcommand(
            Command::new("update_droplast")
                .about("calculate update")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))


        )
        
        Some(("update", update)) => {
            run_update(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                *update.get_one::<usize>("LIMIT").unwrap_or(&0),
                false, //as_demo
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        }
        Some(("update_demo", update)) => {
            run_update(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                *update.get_one::<usize>("LIMIT").unwrap_or(&0),
                true, //as_demo
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        }
        Some(("update_droplast", update)) => run_update_droplast(&fix_input(update.get_one::<String>("INPUT").unwrap())),
        
*/
