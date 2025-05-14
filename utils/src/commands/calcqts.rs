use osmquadtree::calcqts;
use osmquadtree::calcqts::{run_calcqts, run_calcqts_prelim, run_calcqts_load_existing};
use clap::{Args, ValueHint, ValueEnum};
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
    #[arg(short, long, value_hint=ValueHint::FilePath)]
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
    
    /// try to use less than <RAM_GB> GB of ram
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
