use serde::{Deserialize, Serialize};
use serde_json;

use crate::elements::{Bbox, Quadtree};
use crate::pbfformat::HeaderBlock;
use crate::pbfformat::{file_position, read_file_block};
use crate::utils::parse_timestamp;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Result};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct FilelistEntry {
    pub filename: String,
    pub end_date: String,
    pub num_tiles: usize,
    pub state: i64,
}

impl FilelistEntry {
    pub fn new(filename: String, end_date: String, num_tiles: usize, state: i64) -> FilelistEntry {
        FilelistEntry {
            filename,
            end_date,
            num_tiles,
            state,
        }
    }
}

pub fn read_filelist(prfx: &str) -> Vec<FilelistEntry> {
    let ff = File::open(format!("{}filelist.json", prfx)).expect("failed to open filelist file");
    let mut ffb = BufReader::new(ff);
    serde_json::from_reader(&mut ffb).expect("failed to read filelist")
}

pub fn write_filelist(prfx: &str, filelist: &Vec<FilelistEntry>) {
    let flfile =
        File::create(format!("{}filelist.json", prfx)).expect("failed to create filelist file");
    serde_json::to_writer(&flfile, &filelist).expect("failed to write filelist json");
}

pub type ParallelFileLocs = (
    Vec<BufReader<File>>,
    Vec<(Quadtree, Vec<(usize, u64)>)>,
    u64,
);

pub fn get_file_locs_single(infn: &str, filter: Option<Bbox>) -> Result<ParallelFileLocs> {
    let cap = match filter {
        Some(_) => 8 * 1024,
        None => 5 * 1024 * 1024,
    };

    let mut locs = BTreeMap::new();
    let f = File::open(infn)?;
    let mut fbuf = BufReader::with_capacity(cap, f);

    let fb = read_file_block(&mut fbuf)?;
    let filepos = file_position(&mut fbuf)?;
    let head = HeaderBlock::read(filepos, &fb.data(), infn)?;
    if head.index.is_empty() {
        return Err(Error::new(ErrorKind::Other, "no locations in header"));
    }

    let mut total_len = 0;

    for entry in head.index {
        if filter.as_ref().is_none()
            || filter
                .as_ref()
                .unwrap()
                .overlaps(&entry.quadtree.as_bbox(0.05))
        {
            locs.insert(
                entry.quadtree.clone(),
                (locs.len(), vec![(0, entry.location)]),
            );
            total_len += entry.length;
        }
    }
    let mut locsv = Vec::new();
    for (a, (_b, c)) in locs {
        locsv.push((a, c));
    }
    Ok((vec![fbuf], locsv, total_len))
}

pub fn get_file_locs(
    prfx: &str,
    filter: Option<Bbox>,
    timestamp: Option<i64>,
) -> Result<ParallelFileLocs> {
    if prfx.len() > 4 && &prfx[prfx.len() - 4..] == ".pbf" {
        if !timestamp.is_none() {
            return Err(Error::new(
                ErrorKind::Other,
                "can't specify timestamp with single file",
            ));
        }
        return get_file_locs_single(prfx, filter);
    }

    let filelist = read_filelist(&prfx);

    let mut fbufs = Vec::new();
    let mut locs = BTreeMap::new();

    let cap = match filter {
        Some(_) => 8 * 1024,
        None => 5 * 1024 * 1024,
    };
    let mut all_locs = 0;
    let mut total_len = 0;
    for (i, fle) in filelist.iter().enumerate() {
        let fle_ts = parse_timestamp(&fle.end_date)?;
        if !timestamp.is_none() && fle_ts > timestamp.unwrap() {
            break;
        }

        let fle_fn = format!("{}{}", prfx, fle.filename);
        let f = File::open(&fle_fn)?;
        let mut fbuf = BufReader::with_capacity(cap, f);

        let fb = read_file_block(&mut fbuf)?;
        let filepos = file_position(&mut fbuf)?;
        let head = HeaderBlock::read(filepos, &fb.data(), &fle_fn)?;

        if head.index.is_empty() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("no locations in header for {}", &fle_fn),
            ));
        }

        all_locs += head.index.len();
        for entry in head.index {
            if i == 0 {
                if filter.as_ref().is_none()
                    || filter
                        .as_ref()
                        .unwrap()
                        .overlaps(&entry.quadtree.as_bbox(0.05))
                {
                    locs.insert(entry.quadtree.clone(), (locs.len(), Vec::new()));
                    locs.get_mut(&entry.quadtree)
                        .unwrap()
                        .1
                        .push((i, entry.location));
                    total_len += entry.length;
                }
            } else {
                if locs.contains_key(&entry.quadtree) {
                    locs.get_mut(&entry.quadtree)
                        .unwrap()
                        .1
                        .push((i, entry.location));
                    total_len += entry.length;
                }
            }
        }

        fbufs.push(fbuf);
    }

    let mut locsv = Vec::new();

    for (a, (_b, c)) in locs {
        locsv.push((a, c));
    }

    println!(
        "{} files, {} / {} tiles, {:0.1} mb",
        fbufs.len(),
        locsv.len(),
        all_locs,
        (total_len as f64) / 1024.0 / 1024.0
    );

    Ok((fbufs, locsv, total_len))
}
