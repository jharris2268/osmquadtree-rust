use serde_json;
use serde::{Deserialize,Serialize};

//use std::io::{Read,Write};
use std::fs::File;
use std::io::{Result,BufReader};
use std::collections::BTreeMap;
use crate::elements::Bbox;
use crate::header_block;
use crate::read_file_block;

#[derive(Debug,Deserialize,Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct FilelistEntry {
    pub filename: String,
    pub end_date: String, 
    pub num_tiles: usize,
    pub state: i64
}
    
impl FilelistEntry { 
    pub fn new(filename: String, end_date: String, num_tiles: usize, state: i64) -> FilelistEntry {
        FilelistEntry{filename,end_date,num_tiles,state}
    }
}

pub fn read_filelist(prfx: &str) -> Vec<FilelistEntry> {
    
    let ff = File::open(format!("{}filelist.json", prfx)).expect("failed to open filelist file");
    serde_json::from_reader(ff).expect("failed to read filelist")
}


pub fn write_filelist(prfx: &str, filelist: &Vec<FilelistEntry>) {
    let flfile = File::create(format!("{}filelist.json", prfx)).expect("failed to create filelist file");
    serde_json::to_writer(&flfile, &filelist).expect("failed to write filelist json");
}


pub fn get_file_locs(prfx: &str, filter: Option<Bbox>) -> Result<(Vec<BufReader<File>>, Vec<(usize,Vec<(usize,u64)>)>)> {
    let filelist = read_filelist(&prfx);
    
    //let pf = 100.0 / (std::fs::metadata(&format!("{}{}", fname, filelist[0].filename)).expect("fail").len() as f64);

    let mut fbufs = Vec::new();
    let mut locs = BTreeMap::new();
    
    let cap = match filter {
        Some(_) => 8*1024,
        None => 5*1024*1024
    };
    
    for (i,fle) in filelist.iter().enumerate() {
        let fle_fn = format!("{}{}", prfx, fle.filename);
        let f = File::open(&fle_fn).expect("fail");
        let mut fbuf = BufReader::with_capacity(cap, f);
        
        let fb = read_file_block::read_file_block(&mut fbuf).expect("?");
        let filepos = read_file_block::file_position(&mut fbuf).expect("?");
        let head = header_block::HeaderBlock::read(filepos, &fb.data(), &fle_fn).expect("?");
        
        
        
        for entry in head.index {
            if !locs.contains_key(&entry.quadtree) {
                if i != 0 {
                    panic!(format!("quadtree {} not in first file?", entry.quadtree.as_string()));
                }
                locs.insert(entry.quadtree.clone(), (locs.len(), Vec::new()));
            }
            
            locs.get_mut(&entry.quadtree).unwrap().1.push((i, entry.location));
        }
        
        fbufs.push(fbuf);
    }
    
    //let bbox=osmquadtree::elements::Bbox::new(-5000000,495000000,13000000,535000000);
    
    
    let mut locsv = Vec::new();
    for (a,(_b,c)) in &locs {
        match filter {
            None => {locsv.push((locsv.len(),c.clone())); },
            Some(ref bbox) => { 
                if bbox.overlaps(&a.as_bbox(0.05)) {
                    locsv.push((locsv.len(),c.clone()));
                }
            }
        }
    }
        
    println!("{} files, {} / {} tiles", fbufs.len(), locsv.len(), locs.len());
    
    Ok((fbufs,locsv))
}