use osmquadtree::update::{run_update_initial, write_index_file};
use clap::{Args,ValueHint};
use crate::commands::{RunCmd,Defaults, QT_MAX_LEVEL_DEFAULT, QT_BUFFER_DEFAULT};
use crate::error::Result;


#[derive(Args, Debug)]
pub struct UpdateInitial {
    /// Sets prefix for updates. Inlcude trailing / for directory
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    /// Sets the input sorted planet file to use
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    infn: String,
    
    /// Sets timestamp for input planet file
    #[arg(short, long)]
    timestamp: String,
    
    /// Sets initial state (i.e. sequenceNumber from replication state.txt) which matches planet file. If not specified will determine.
    #[arg(short, long)]
    initial_state: Option<i64>,
    
    /// Replication xml-change source to download, default planet.openstreetmap.org/replication/day/
    #[arg(short='x', long)]
    diffs_source: Option<String>,
    
    /// Directory for downloaded osc.gz files
    #[arg(short, long)]
    diffs_location: String,
    
    /// qtlevel
    #[arg(short='l', long, default_value_t = QT_MAX_LEVEL_DEFAULT)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..19))]
    qt_level: u16,
    
    /// qtbuffer
    #[arg(short='b',long, default_value_t = QT_BUFFER_DEFAULT)]
    qt_buffer: f64,
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short)] #[arg(long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>
}

impl RunCmd for UpdateInitial {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        Ok(run_update_initial(
            &self.input,
            &self.infn,
            &self.timestamp,
            self.initial_state.clone(),
            self.diffs_source.as_deref(),
            &self.diffs_location,
            self.qt_level.into(),
            self.qt_buffer,
            match self.numchan { None => defaults.numchan_default, Some(n) => n.into() },
        )?)        
    }
}


#[derive(Args, Debug)]
pub struct WriteIndexFile {
    ///Sets the input file  to use
    #[arg(value_hint=ValueHint::FilePath)]
    input: String,
    
    /// Out filename, defaults to INPUT-index.pbf
    #[arg(short, long, value_hint=ValueHint::FilePath)]
    outfn: Option<String>,
    
    ///uses <NUMCHAN> parallel threads
    #[arg(short)] #[arg(long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>
}

impl RunCmd for WriteIndexFile {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        write_index_file(
            &self.input,
            match &self.outfn { Some(o) => o, None => "" },
            match self.numchan { None => defaults.numchan_default, Some(n) => n.into() },
        );
        Ok(())
    }
}



/*
        .subcommand(
            Command::new("update_initial")
                .about("calculate initial index")
                .arg(Arg::new("INPUT").required(true).help("Sets the input directory to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("INFN").required(true).short('i').long("infn").num_args(1).help("specify filename of orig file").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("TIMESTAMP").required(true).short('t').long("timestamp").num_args(1).help("timestamp for data"))
                .arg(Arg::new("INITIAL_STATE").short('s').long("state_initial").num_args(1).help("initial state, if not specified will determine"))
                .arg(Arg::new("DIFFS_SOURCE").short('x').long("diffs_source").num_args(1).help("source for diffs to download, default planet.openstreetmap.org/replication/day/"))
                .arg(Arg::new("DIFFS_LOCATION").required(true).short('d').long("diffs_location").num_args(1).help("directory for downloaded osc.gz files"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )

        .subcommand(
            Command::new("write_index_file")
                .about("write pbf index file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("OUTFN").short('o').long("outfn").num_args(1).help("out filename, defaults to INPUT-index.pbf").value_hint(clap::ValueHint::FilePath))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))

        )


        Some(("update_initial", update)) => {
            run_update_initial(
                &fix_input(update.get_one::<String>("INPUT").unwrap()),
                update.get_one::<String>("INFN").unwrap(),
                update.get_one::<String>("TIMESTAMP").unwrap(),
                update.get_one::<i64>("INITIAL_STATE").map(|x| *x),
                update.get_one::<String>("DIFFS_SOURCE").map(|x| x.as_str()),
                update.get_one::<String>("DIFFS_LOCATION").unwrap(),
                *update.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
                *update.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
                *update.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
            ).or_else(|e| Err(Error::from(e)))
        },

        Some(("write_index_file", write)) => write_index_file_w(
            write.get_one::<String>("INPUT").unwrap(),
            write.get_one::<String>("OUTFN").map(|x| x.as_str()),
            *write.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ),
*/
