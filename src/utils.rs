use std::fmt;
use std::io::{Error, ErrorKind, Result};


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

use chrono::NaiveDateTime;

const TIMEFORMAT: &str = "%Y-%m-%dT%H:%M:%S";
const TIMEFORMAT_ALT: &str = "%Y-%m-%dT%H-%M-%S";
const DATEFORMAT: &str = "%Y%m%d";
pub fn parse_timestamp(ts: &str) -> Result<i64> {
    match NaiveDateTime::parse_from_str(ts, TIMEFORMAT) {
        Ok(tm) => {
            return Ok(tm.timestamp());
        }
        Err(_) => {} //message!("{:?}", e)}
    }

    match NaiveDateTime::parse_from_str(ts, TIMEFORMAT_ALT) {
        Ok(tm) => {
            return Ok(tm.timestamp());
        }
        Err(_) => {
            //message!("{:?}", e)
        }
    }
    
    match NaiveDateTime::parse_from_str(ts, DATEFORMAT) {
        Ok(tm) => {
            return Ok(tm.timestamp());
        }
        Err(_) => {
            //message!("{:?}", e)
        }
    }
    
    return Err(Error::new(
        ErrorKind::Other,
        format!("use \"{}\" or \"{}\"", TIMEFORMAT, TIMEFORMAT_ALT),
    ));
}

pub fn timestamp_string(ts: i64) -> String {
    let dt = NaiveDateTime::from_timestamp(ts, 0);
    dt.format(TIMEFORMAT).to_string()
}
pub fn timestamp_string_alt(ts: i64) -> String {
    let dt = NaiveDateTime::from_timestamp(ts, 0);
    dt.format(TIMEFORMAT_ALT).to_string()
}
pub fn date_string(ts: i64) -> String {
    let dt = NaiveDateTime::from_timestamp(ts, 0);
    dt.format(DATEFORMAT).to_string()
}

pub fn as_int(v: f64) -> i32 {
    if v < 0.0 {
        return ((v * 10000000.0) - 0.5) as i32;
    }

    return ((v * 10000000.0) + 0.5) as i32;
}
