/*Command::new("calcqts")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                //.arg(Arg::new("COMBINED").short("-c").long("combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("MODE").short('m').long("mode").num_args(1).help("simplier implementation, suitable for files <8gb"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
                .arg(Arg::new("KEEPTEMPS").short('k').long("keeptemps").action(ArgAction::SetTrue).help("keep temp files"))
                .arg(Arg::new("RAM_GB").short('r').long("ram").value_parser(clap::value_parser!(usize)).num_args(1).help("can make use of RAM_GB gb memory"))
        )
        .subcommand(
            Command::new("calcqts_prelim")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use"))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                
        )
        .subcommand(
            Command::new("calcqts_load_existing")
                .about("calculates quadtrees for each element of a planet or extract pbf file")
                .arg(Arg::new("INPUT").required(true).help("Sets the input file (or directory) to use").value_hint(clap::ValueHint::AnyPath))
                .arg(Arg::new("QTSFN").short('q').long("qtsfn").num_args(1).help("specify output filename, defaults to <INPUT>-qts.pbf").value_hint(clap::ValueHint::FilePath))
                //.arg(Arg::new("COMBINED").short("-c").long("combined").help("writes combined NodeWayNodes file"))
                .arg(Arg::new("NUMCHAN").short('n').long("numchan").value_parser(clap::value_parser!(usize)).num_args(1).help("uses NUMCHAN parallel threads"))
                .arg(Arg::new("STOP_AT").short('s').long("stop_at").value_parser(clap::value_parser!(u64)).num_args(1).help("location of first file block without nodes"))
                .arg(Arg::new("QT_LEVEL").short('l').long("qt_level").value_parser(clap::value_parser!(usize)).num_args(1).help("maximum qt level, defaults to 18"))
                .arg(Arg::new("QT_BUFFER").short('b').long("qt_buffer").value_parser(clap::value_parser!(f64)).num_args(1).help("qt buffer, defaults to 0.05"))
        )
        
        
    Some(("setup", _)) => setup::run(defaults.ram_gb_default, defaults.numchan_default),
        Some(("calcqts", calcqts)) => {
            match run_calcqts(
                calcqts.get_one::<String>("INPUT").unwrap(),
                calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
                *calcqts.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
                *calcqts.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
                calcqts.get_one::<String>("MODE").map(|x| x.as_str()),
                /* !calcqts.is_present("COMBINED"), //seperate
                true,                            //resort_waynodes*/
                calcqts.contains_id("KEEPTEMPS"),
                *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
                *calcqts.get_one::<usize>("RAM_GB").unwrap_or(&defaults.ram_gb_default),
            ) {
                Ok((_,lt,_)) => { message!("{}", lt); Ok(()) },
                Err(e) => Err(Error::from(e))
            }
        },
        Some(("calcqts_prelim", calcqts)) => run_calcqts_prelim(
            calcqts.get_one::<String>("INPUT").unwrap(),
            calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
            *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ).or_else(|e| Err(Error::from(e))),
        
        Some(("calcqts_load_existing", calcqts)) => run_calcqts_load_existing(
            calcqts.get_one::<String>("INPUT").unwrap(),
            calcqts.get_one::<String>("QTSFN").map(|x| x.as_str()),
            *calcqts.get_one::<usize>("QT_LEVEL").unwrap_or(&QT_MAX_LEVEL_DEFAULT),
            *calcqts.get_one::<f64>("QT_BUFFER").unwrap_or(&QT_BUFFER_DEFAULT),
            match calcqts.get_one::<u64>("STOP_AT") {
                Some(&s) => Some(s),
                None => None,
            },
            *calcqts.get_one::<usize>("NUMCHAN").unwrap_or(&defaults.numchan_default),
        ).or_else(|e| Err(Error::from(e))),
*/


use osmquadtree::calcqts;
use osmquadtree::calcqts::{run_calcqts, run_calcqts_prelim, run_calcqts_load_existing};
use clap::{Args, Parser, Subcommand,ValueHint, ValueEnum};
use crate::commands::{RunCmd,Defaults, QT_MAX_LEVEL_DEFAULT, QT_BUFFER_DEFAULT};
use crate::error::Result;
use osmquadtree::message;


#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
enum Mode {
    /// Choose based on file size, (Default)
    Choose,
    
    /// Keep in memory, only works for small files
    Inmem,
    
    /// Simple
    Simple,
    
    /// 40 bit integers in a vector
    Flatvec,
}
impl Mode {
    fn as_orig(&self) -> calcqts::Mode {
        match self {
            Mode::Choose => calcqts::Mode::Choose,
            Mode::Inmem => calcqts::Mode::Inmem,
            Mode::Simple => calcqts::Mode::Simple,
            Mode::Flatvec => calcqts::Mode::Flatvec
        }
    }
}
        

#[derive(Args, Debug)]
pub struct Calcqts {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    /// specify output filename, defaults to <INPUT>-qts.pbf
    #[arg(short, long, value_hint=ValueHint::AnyPath)]
    qtsfn: Option<String>,

    
    /// uses <NUMCHAN> parallel threads
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>,
   
    /// reads full primitiveblock data 
    #[arg(short, long, value_enum, default_value_t = Mode::Choose)]
    mode: Mode,
    
    /// qtlevel
    #[arg(short='l', long, default_value_t = QT_MAX_LEVEL_DEFAULT)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..18))]
    qt_level: u16,
    
    /// qtbuffer
    #[arg(short='b',long, default_value_t = QT_BUFFER_DEFAULT)]
    qt_buffer: f64,
    
    /// keep temporary files
    #[arg(short, long)]
    keeptemps: bool,
    
    /// uses <NUMCHAN> parallel threads
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..32))]
    ram_gb: Option<u16>,
}
impl RunCmd for Calcqts {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        message!("run calcqts? {:?}", &self);
        
        
        let (a,b,c) = run_calcqts(
            &self.input,
            self.qtsfn.as_deref(),
            self.qt_level.into(),
            self.qt_buffer,
            self.mode.as_orig(),
            self.keeptemps,
            match self.numchan { Some(n) => n.into(), None => defaults.numchan_default},
            match self.ram_gb { Some(n) => n.into(), None => defaults.numchan_default},
        )?;
        
        message!("{} {} {}", a,b, c);
        Ok(())
        
        
    }
}        


#[derive(Args, Debug)]
pub struct CalcqtsPrelim {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    /// specify output filename, defaults to <INPUT>-qts.pbf
    #[arg(short, long, value_hint=ValueHint::AnyPath)]
    qtsfn: Option<String>,
    
    
    /// uses <NUMCHAN> parallel threads
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>,
    
}
impl RunCmd for CalcqtsPrelim {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        run_calcqts_prelim(
            &self.input,
            self.qtsfn.as_deref(),
            match self.numchan { Some(n) => n.into(), None => defaults.numchan_default},
        )?;
        Ok(())
    }
}  

#[derive(Args, Debug)]
pub struct CalcqtsLoadExisting {
    ///Sets the input file (or directory) to use
    #[arg(value_hint=ValueHint::AnyPath)]
    input: String,
    
    /// specify output filename, defaults to <INPUT>-qts.pbf
    #[arg(short, long, value_hint=ValueHint::AnyPath)]
    qtsfn: Option<String>,
    
    
    /// uses <NUMCHAN> parallel threads
    #[arg(short, long)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..24))]
    numchan: Option<u16>,
    
    
    /// stop looking for nodes after <STOPAT> bytes
    #[arg(short, long)]
    stopat: Option<u64>,
    
    /// qtlevel
    #[arg(short='l', long, default_value_t = QT_MAX_LEVEL_DEFAULT)]
    #[arg(value_parser = clap::value_parser!(u16).range(0..18))]
    qt_level: u16,
    
    /// qtbuffer
    #[arg(short='b',long, default_value_t = QT_BUFFER_DEFAULT)]
    qt_buffer: f64,
    
}
impl RunCmd for CalcqtsLoadExisting {
    fn run(&self, defaults: &Defaults) -> Result<()> {
        
        run_calcqts_load_existing(
            &self.input,
            self.qtsfn.as_deref(),
            self.qt_level.into(),
            self.qt_buffer,
            self.stopat,
            match self.numchan { Some(n) => n.into(), None => defaults.numchan_default},
        )?;
        Ok(())
    }
}  
