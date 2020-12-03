

use super::callback::CallFinish;
use super::header_block::{make_header_block,HeaderType};
use super::utils::ThreadTimer;
use super::read_file_block::pack_file_block;

use std::fs::File;
use serde_json;
use std::io::{Result,Write,Seek,SeekFrom};
use std::collections::HashMap;

pub type FileLocs = Vec<(i64,Vec<(u64,u64)>)>;

pub struct WriteFile {
    outf: File,
    write_external_locs: bool,
    locs: HashMap<i64,Vec<(u64,u64)>>,
    tm: f64,
    fname: String
}

impl WriteFile {
    pub fn new(outfn: &str, header_type: HeaderType) -> WriteFile {
        let mut outf = File::create(outfn).expect("failed to create");
        let mut write_external_locs=false;
        match header_type {
            HeaderType::None => {},
            HeaderType::NoLocs => {
                outf.write_all(&pack_file_block("OSMHeader", &make_header_block(false),true).expect("?")).expect("?");
            }
            HeaderType::ExternalLocs => {
                outf.write_all(&pack_file_block("OSMHeader", &make_header_block(true),true).expect("?")).expect("?");
                write_external_locs=true;
            },
            HeaderType::InternalLocs => {
                panic!("use WriteFileInternalLocs")
            }
        }
        
        WriteFile{outf: outf, tm: 0.0, write_external_locs: write_external_locs, locs:HashMap::new(), fname: String::from(outfn)}
    }
    
    fn add_loc(&mut self, i: i64, l: u64) {
        let p = self.outf.seek(SeekFrom::Current(0)).expect("??");
        if self.locs.contains_key(&i) {
            self.locs.get_mut(&i).unwrap().push((p,l));
        } else {
            self.locs.insert(i,vec![(p,l)]);
        }
        
    }
}

impl CallFinish for WriteFile {
    type CallType = Vec<(i64,Vec<u8>)>;
    type ReturnType = (f64,FileLocs);
    
    fn call(&mut self, bls: Vec<(i64,Vec<u8>)>) {
        let c = ThreadTimer::new();
        for (i,d) in &bls {
            self.add_loc(*i, d.len() as u64);
            self.outf.write_all(d).expect("failed to write block");
        }
        
        self.tm += c.since();
    }
    
    fn finish(&mut self) -> Result<Self::ReturnType> {
        
        
        let mut ls = Vec::new();
        let mut lf = Vec::new();
        for (a,b) in std::mem::take(&mut self.locs) {
            
            for (c,d) in &b {
                lf.push((a,*c,*d));
            }
            ls.push((a,b));
        }
        
        //o.locs.extend(self.locs.iter().map(|(a,b)|{(*a,*b)}));
        
        ls.sort();
        
        if self.write_external_locs {
            lf.sort_by_key(|p| { p.1 });
            let jf = File::create(format!("{}-filelocs.json", self.fname)).expect("failed to create filelocs file");
            serde_json::to_writer(jf, &lf).expect("failed to write filelocs json");
        }
        
        Ok((self.tm,ls))
    }
}
