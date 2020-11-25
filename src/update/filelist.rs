use serde_json;
use serde::{Deserialize,Serialize};

//use std::io::{Read,Write};
use std::fs::File;


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
