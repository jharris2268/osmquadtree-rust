use std::fmt;
use std::io::Read;
//use std::io::{Error, ErrorKind, Result};





#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Io(std::io::Error),
    Ureq(ureq::Error),
    SerdeJson(serde_json::Error),
    QuickXml(quick_xml::Error),
    NotImplementedError,
    
    Utf8Error(std::str::Utf8Error),
    
    TimeFormatError(std::string::String),
    PbfDataError(std::string::String),
    XmlDataError(std::string::String),
    MissingDataError(std::string::String),
    UserSelectionError(std::string::String),
    ExternalCallError(std::string::String),
    UnexpectedResponseError(std::string::String),
    ChannelledCallbackError(std::string::String),
    InvalidDataError(std::string::String)
}

impl std::error::Error for Error {
    
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        
        match self {
            Error::Io(e) => Some(e), 
            Error::Ureq(e) => Some(e), 
            _ => None
        }
    }
    
}

impl std::convert::From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        return Error::Io(e)
    }
}
impl std::convert::From<ureq::Error> for Error {
    fn from(e: ureq::Error) -> Self {
        return Error::Ureq(e)
    }
}

impl std::convert::From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        return Error::SerdeJson(e)
    }
}

impl std::convert::From<channelled_callbacks::Error::<Error>> for Error {
    fn from(e: channelled_callbacks::Error::<Error>) -> Self {
        match e {
            channelled_callbacks::Error::<Error>::OtherError(x) => x,
            channelled_callbacks::Error::<Error>::ChannelledCallbackError(s) =>
                Error::ChannelledCallbackError(s)
        }
    }
}
impl std::convert::From<quick_xml::Error> for Error {
    fn from(e: quick_xml::Error) -> Self {
        return Error::QuickXml(e)
    }
}
impl std::convert::From<std::str::Utf8Error> for Error {
    fn from(e: std::str::Utf8Error) -> Self {
        return Error::Utf8Error(e)
    }
}

impl std::fmt::Display for Error {
    
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::Io(e) => write!(f, "std::io::Error {}", e),
            Error::Ureq(e) => write!(f, "ureq::Error {}", e),
            Error::SerdeJson(e) => write!(f, "serde_json::Error {}", e),
            Error::QuickXml(e) => write!(f, "quick_xml::Error {}", e),
            Error::Utf8Error(e) => write!(f, "std::str::Utf8Error {}", e),
            Error::NotImplementedError => write!(f, "NotImplementedError"),
            Error::TimeFormatError(e) => write!(f, "TimeFormatError: {}", e),
            Error::PbfDataError(e) => write!(f, "PbfDataError: {}", e),
            Error::XmlDataError(e) => write!(f, "XmlDataError: {}", e),
            Error::MissingDataError(e) => write!(f, "MissingDataError: {}", e),
            Error::UserSelectionError(e) => write!(f, "UserSelectionError: {}", e),
            Error::ExternalCallError(e) => write!(f, "ExternalCallError: {}", e),
            Error::UnexpectedResponseError(e) => write!(f, "UnexpectedResponseError: {}", e),
            Error::ChannelledCallbackError(e) => write!(f, "ChannelledCallbackError: {}", e),
            Error::InvalidDataError(e) => write!(f, "InvalidDataError: {}", e),
        }
        
    }
}

#[cfg(feature = "python")]
impl std::convert::From<Error> for pyo3::PyErr {
    
    fn from(err: Error) -> pyo3::PyErr {
        pyo3::exceptions::PyOSError::new_err(format!("{}", err))
    }
}
    
    

    

pub type Result<T> = std::result::Result<T, Error>;

/*
impl<T> std::convert::From<channelled_callbacks::Result<T, Error>> for Result<T> {
    fn from(r: channelled_callbacks::Result<T, Error>) -> Self {
        match r {
            Ok(t) => Ok(t),
            Err(e) => Err(Error::from(e))
        }
    }
}
*/

fn as_secs(dur: std::time::Duration) -> f64 {
    (dur.as_secs() as f64) * 1.0 + (dur.subsec_nanos() as f64) * 0.000000001
}

pub struct Timer(std::time::SystemTime);

impl Timer {
    pub fn new() -> Timer {
        Timer(std::time::SystemTime::now())
    }

    pub fn since(&self) -> f64 {
        as_secs(self.0.elapsed().unwrap())
    }

    pub fn reset(&mut self) {
        self.0 = std::time::SystemTime::now();
    }
}

pub struct LogTimes {
    pub timer: Timer,
    pub msgs: Vec<(String, f64)>,
    pub longest: usize,
}
impl LogTimes {
    pub fn new() -> LogTimes {
        LogTimes {
            timer: Timer::new(),
            msgs: Vec::new(),
            longest: 6,
        }
    }
    pub fn add(&mut self, msg: &str) {
        self.longest = usize::max(self.longest, msg.len());
        self.msgs.push((String::from(msg), self.timer.since()));
        self.timer.reset();
    }
}
impl fmt::Display for LogTimes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut tot = 0.0;
        let mut others=0.0;
        for (a, b) in &self.msgs {
            if *b > 0.1 {
                write!(
                    f,
                    "{}:{}{:6.2}s\n",
                    a,
                    " ".repeat(self.longest - a.len()),
                    b
                )?;
            } else {
                others += b;
            }
            tot += b;
        }
        if others > 0.0 {
            write!(f, "OTHERS:{}{:6.2}s\n", " ".repeat(self.longest - 6), others)?;
        }
        write!(f, "TOTAL:{}{:6.2}s", " ".repeat(self.longest - 5), tot)
    }
}

pub struct ThreadTimer(cpu_time::ThreadTime);

impl ThreadTimer {
    pub fn new() -> ThreadTimer {
        ThreadTimer(cpu_time::ThreadTime::now())
    }

    pub fn since(&self) -> f64 {
        as_secs(self.0.elapsed())
    }
}

pub(crate) struct Checktime {
    
    st: Timer,
    lt: Timer,
    thres: f64,
}

impl Checktime {
    pub fn new() -> Checktime {
        Self::with_threshold(2.0)
    }
    pub fn with_threshold(thres: f64) -> Checktime {
        Checktime {
            st: Timer::new(),
            lt: Timer::new(),
            thres: thres,
        }
    }

    pub fn checktime(&mut self) -> Option<f64> {
        let lm = self.lt.since();
        if lm > self.thres {
            self.lt.reset();
            return Some(self.st.since());
        }

        None
    }
    pub fn gettime(&self) -> f64 {
        self.st.since()
    }
}

use chrono::prelude::*;

const TIMEFORMAT: &str = "%Y-%m-%dT%H:%M:%S";
const TIMEFORMATZ: &str = "%Y-%m-%dT%H:%M:%SZ";
const TIMEFORMAT_ALT: &str = "%Y-%m-%dT%H-%M-%S";
const DATEFORMAT: &str = "%Y%m%d";
pub fn parse_timestamp(ts: &str) -> Result<i64> {
    match NaiveDateTime::parse_from_str(ts, TIMEFORMAT) {
        Ok(tm) => {
            return Ok(tm.and_utc().timestamp());
        }
        Err(_) => {} //message!("{:?}", e)}
    }
    
    match NaiveDateTime::parse_from_str(ts, TIMEFORMATZ) {
        Ok(tm) => {
            return Ok(tm.and_utc().timestamp());
        }
        Err(_) => {} //message!("{:?}", e)}
    }

    match NaiveDateTime::parse_from_str(ts, TIMEFORMAT_ALT) {
        Ok(tm) => {
            return Ok(tm.and_utc().timestamp());
        }
        Err(_) => {
            //message!("{:?}", e)
        }
    }
    
    match NaiveDate::parse_from_str(ts, DATEFORMAT) {
        Ok(tm) => {
            
            return Ok(
                tm.and_hms_opt(0,0,0)
                    .ok_or(Error::TimeFormatError("and_hms_opt failes".to_string()))?
                    .and_utc().timestamp()
            );
        }
        Err(_) => {
            //message!("{:?}", e)
        }
    }
    
    return Err(Error::TimeFormatError(
        format!("can't read {}: use \"{}\", \"{}\", \"{}\" or \"{}\"", ts, TIMEFORMAT, TIMEFORMATZ, TIMEFORMAT_ALT, DATEFORMAT),
    ));
}

pub fn timestamp_string(ts: i64) -> String {
    match DateTime::<Utc>::from_timestamp(ts, 0) {
        Some(dt) => dt.format(TIMEFORMAT).to_string(),
        None => String::from("??")
    }
        
}
pub fn timestamp_string_alt(ts: i64) -> String {
    match DateTime::<Utc>::from_timestamp(ts, 0) {
        Some(dt) => dt.format(TIMEFORMAT_ALT).to_string(),
        None => String::from("??")
    }
}
pub fn date_string(ts: i64) -> String {
    match DateTime::<Utc>::from_timestamp(ts, 0) {
        Some(dt) => dt.format(DATEFORMAT).to_string(),
        None => String::from("??")
    }
    
}

pub fn as_int(v: f64) -> i32 {
    if v < 0.0 {
        return ((v * 10000000.0) - 0.5) as i32;
    }

    return ((v * 10000000.0) + 0.5) as i32;
}


pub fn at_end_of_file<R: Read>(f: &mut R) -> Result<bool> {
    
    let mut x: [u8;1] = [0];
    
    match f.read(&mut x) {
        Ok(0) => Ok(true),
        Ok(_) => Ok(false),
        Err(e) => Err(Error::Io(e))
    }
}
