use crate::update::{write_index_file,find_update};
use crate::pbfformat::{read_filelist, write_filelist, FilelistEntry};
use crate::utils::{
    date_string, parse_timestamp, timestamp_string, timestamp_string_alt, LogTimes,
};
use crate::message;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Error, ErrorKind, Result, Write};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
struct Settings {
    pub initial_state: i64,
    pub diffs_location: String,
    pub source_prfx: String,
    pub round_time: bool,
    pub max_qt_level: usize,
    pub qt_buffer: f64
}

impl Settings {
    pub fn new(initial_state: i64, diffs_location: &str, max_qt_level: usize, qt_buffer: f64) -> Settings {
        Settings {
            initial_state: initial_state,
            diffs_location: String::from(diffs_location),
            source_prfx: String::from("https://planet.openstreetmap.org/replication/day/"),
            round_time: true,
            max_qt_level: max_qt_level,
            qt_buffer: qt_buffer
        }
    }

    pub fn from_file(prfx: &str) -> Settings {
        let ff =
            File::open(format!("{}settings.json", prfx)).expect("failed to open settings file");
        serde_json::from_reader(ff).expect("failed to parse json")
    }

    pub fn write(&self, prfx: &str) {
        let ff =
            File::create(format!("{}settings.json", prfx)).expect("failed to create settings file");
        serde_json::to_writer(ff, self).expect("failed to write json");
    }
}

fn fetch_new_diffs(
    source_prfx: &str,
    diffs_location: &str,
    current_state: i64,
    csv_rec: &mut Vec<(String, i64, i64)>,
    tms: &mut LogTimes,
) -> Result<()> {
    let (state, timestamp) = get_state(source_prfx, None)?;

    message!(
        "lastest state {} {}, current diff {}, add {}",
        state,
        timestamp,
        current_state,
        state - current_state
    );

    if state > current_state {
        for st in (current_state + 1)..(state + 1) {
            let f = fetch_diff(source_prfx, diffs_location, st)?;

            tms.add(&format!("fetch {}", f.0));
            csv_rec.push(f);
        }
    }
    Ok(())
}

fn fetch_diff(
    source_prfx: &str,
    diffs_location: &str,
    state_in: i64,
) -> Result<(String, i64, i64)> {
    let (state, ts) = get_state(source_prfx, Some(state_in))?;
    let diff_url = get_diff_url(source_prfx, state);
    let outfn = format!("{}{}.osc.gz", diffs_location, state);

    let output = std::process::Command::new("wget")
        .arg("-O")
        .arg(&outfn)
        .arg(&diff_url)
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit())
        .output()?;

    if !output.status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            format!("wget -O {} {} failed", outfn, diff_url),
        ));
    }

    OpenOptions::new()
        .append(true)
        .open(format!("{}state.csv", diffs_location))?
        .write_all(format!("{},{}\n", state, timestamp_string_alt(ts)).as_bytes())?;

    return Ok((outfn, state, ts));
}

fn get_diff_state_url(source_prfx: &str, state: Option<i64>) -> String {
    match state {
        None => format!("{}state.txt", source_prfx),
        Some(state) => {
            let a = state / 1000000;
            let b = (state % 1000000) / 1000;
            let c = state % 1000;

            format!("{}{:03}/{:03}/{:03}.state.txt", source_prfx, a, b, c)
        }
    }
}
fn get_diff_url(source_prfx: &str, state: i64) -> String {
    let a = state / 1000000;
    let b = (state % 1000000) / 1000;
    let c = state % 1000;

    format!("{}{:03}/{:03}/{:03}.osc.gz", source_prfx, a, b, c)
}

fn get_state(source_prfx: &str, state: Option<i64>) -> Result<(i64, i64)> {
    let state_url = get_diff_state_url(source_prfx, state);

    let state_response = ureq::get(&state_url).call().into_string()?;

    let mut seq_num: Option<i64> = None;
    let mut timestamp: Option<i64> = None;

    for l in state_response.lines() {
        if l.starts_with("sequenceNumber=") {
            match l[15..].parse() {
                Ok(s) => {
                    seq_num = Some(s);
                }
                Err(e) => {
                    return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
                }
            }
        } else if l.starts_with("timestamp=") {
            let tss = String::from(&l[10..l.len() - 1].replace("\\:", "-"));
            let ts = parse_timestamp(&tss)?;
            timestamp = Some(ts);
        }
    }

    if seq_num.is_none() {
        return Err(Error::new(
            ErrorKind::Other,
            format!("{} missing sequenceNumber?", state_url),
        ));
    }
    if timestamp.is_none() {
        return Err(Error::new(
            ErrorKind::Other,
            format!("{} missing timestamp?", state_url),
        ));
    }
    Ok((seq_num.unwrap(), timestamp.unwrap()))
}

fn read_csv_list(diffs_location: &str, last_state: i64) -> Vec<(String, i64, i64)> {
    let mut res = Vec::new();

    let state_ff =
        File::open(format!("{}state.csv", diffs_location)).expect("failed to open state.csv file");

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(state_ff);

    for row in rdr.records() {
        let row = row.expect("?");

        if row.len() == 2 {
            let state: i64 = row[0].parse().unwrap();

            if state > last_state {
                let timestamp = parse_timestamp(&row[1]).expect("?");
                let fname = format!("{}{}.osc.gz", diffs_location, state);
                res.push((fname, state, timestamp));
            }
        }
    }
    res
}

fn check_state(
    settings: &Settings,
    filelist: &Vec<FilelistEntry>,
) -> (LogTimes, Vec<(String, i64, i64)>, i64) {
    let mut tms = LogTimes::new();
    if filelist.is_empty() {
        panic!("empty filelist");
    }
    let last_state = filelist.last().unwrap().state;
    let prev_ts = parse_timestamp(&filelist.last().unwrap().end_date).expect("?");
    let mut csv_rec = read_csv_list(&settings.diffs_location, last_state);
    let last_state_available = if csv_rec.is_empty() {
        last_state
    } else {
        csv_rec.last().unwrap().1
    };

    tms.add("found filelist");
    fetch_new_diffs(
        &settings.source_prfx,
        &settings.diffs_location,
        last_state_available,
        &mut csv_rec,
        &mut tms,
    )
    .expect("!!");

    (tms, csv_rec, prev_ts)
}

pub fn run_update_initial(
    prfx: &str,
    infn: &str,
    timestamp: &str,
    initial_state: i64,
    diffs_location: &str,
    max_qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Result<()> {
    let timestamp = parse_timestamp(timestamp)?;

    let outfn = format!("{}{}-index.pbf", prfx, infn);
    let infn2 = format!("{}{}", prfx, infn);
    let num_tiles = write_index_file(&infn2, &outfn, numchan);

    let settings = Settings::new(initial_state, &diffs_location, max_qt_level, qt_buffer);
    message!("{:?}", settings);
    settings.write(prfx);

    write_filelist(
        prfx,
        &vec![FilelistEntry::new(
            String::from(infn),
            timestamp_string(timestamp),
            num_tiles,
            initial_state,
        )],
    );
    Ok(())
}

pub fn run_update(prfx: &str, limit: usize, as_demo: bool, numchan: usize) -> Result<()> {
    let settings = Settings::from_file(prfx);
    let mut filelist = read_filelist(prfx);
    let mut suffix = String::new();
    if as_demo {
        filelist.pop();
        if limit > 1 {
            for _ in 1..limit {
                filelist.pop();
            }
        }
        suffix = String::from("-rust");
    }

    let (mut logtimes, mut to_update, mut prev_ts) = check_state(&settings, &filelist);
    if limit > 0 && to_update.len() > limit {
        to_update = to_update[..limit].to_vec();
    }
    message!(
        "have {} in filelist, {} to update",
        filelist.len(),
        to_update.len()
    );

    if !to_update.is_empty() {
        for (chgfn, state, ts) in to_update {
            let fname = format!("{}{}.pbfc", date_string(ts), suffix);
            message!(
                "call find_update('{}',{} entries,'{}', {}, {}, {}, {}, {}, {})",
                prfx,
                filelist.len(),
                chgfn,
                prev_ts,
                ts,
                settings.max_qt_level,
                settings.qt_buffer,
                fname,
                numchan
            );

            let (_tx, nt) = find_update(prfx, &filelist, &chgfn, prev_ts, ts, settings.max_qt_level, settings.qt_buffer, &fname, numchan)?;
            logtimes.add(&fname);

            let idxfn = format!("{}{}-index.pbf", prfx, fname);
            //let txx = ThreadTimer::new();
            write_index_file(&format!("{}{}", prfx, fname), &idxfn, numchan);
            logtimes.add(&format!("{}-index.pbf", fname));

            filelist.push(FilelistEntry::new(fname, timestamp_string(ts), nt, state));
            prev_ts = ts;
        }
        if !as_demo {
            write_filelist(prfx, &filelist);
        }
    }
    message!("{}", logtimes);
    Ok(())
}
