extern crate serde_json;

use std::fs::File;
use std::io::{Seek,SeekFrom,Write};
use std::io;
use std::collections::HashMap;
mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::elements::{PrimitiveBlock,Quadtree};
use osmquadtree::read_file_block::{pack_file_block};
use osmquadtree::callback::{CallFinish,};
use osmquadtree::utils::{Checktime,CallAll,ThreadTimer};
use osmquadtree::write_pbf;

use super::{Timings,OtherData};


fn pack_bbox()->Vec<u8> {
    let mut res=Vec::new();
    write_pbf::pack_value(&mut res, 1, write_pbf::zig_zag(-180000000000));
    write_pbf::pack_value(&mut res, 2, write_pbf::zig_zag(180000000000));
    write_pbf::pack_value(&mut res, 3, write_pbf::zig_zag(90000000000));
    write_pbf::pack_value(&mut res, 4, write_pbf::zig_zag(-90000000000));
    res
}

fn make_header_block(withlocs: bool) -> Vec<u8> {
    let mut res=Vec::new();
    
    write_pbf::pack_data(&mut res, 1, &pack_bbox());
    write_pbf::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    write_pbf::pack_data(&mut res, 4, b"DenseNodes");
    write_pbf::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    if withlocs {
        write_pbf::pack_data(&mut res, 23, b"-filelocs.json");
    }
    
    res
}

fn pack_index_item(q: &Quadtree, _ischange: bool, l: u64) -> Vec<u8> {
    let mut res=Vec::with_capacity(25);
    //if ischange { write_pbf::pack_value(&mut res, 2, 1); }
    write_pbf::pack_value(&mut res, 2, 0);
    write_pbf::pack_value(&mut res, 3, write_pbf::zig_zag(l as i64));
    write_pbf::pack_value(&mut res, 4, write_pbf::zig_zag(q.as_int()));
    res
}

fn make_header_block_stored_locs(ischange: bool, locs: Vec<(Quadtree,u64)>) -> Vec<u8> {
    let mut res=Vec::new();
    
    write_pbf::pack_data(&mut res, 1, &pack_bbox());
    write_pbf::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    write_pbf::pack_data(&mut res, 4, b"DenseNodes");
    write_pbf::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    for (a,b) in &locs {
        write_pbf::pack_data(&mut res, 22, &pack_index_item(a,ischange,*b));
    }
    
    res
}

pub enum HeaderType {
    None,
    NoLocs,
    InternalLocs,
    ExternalLocs
}

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
    type ReturnType = Timings;
    
    fn call(&mut self, bls: Vec<(i64,Vec<u8>)>) {
        let c = Checktime::new();
        for (i,d) in &bls {
            self.add_loc(*i, d.len() as u64);
            self.outf.write_all(d).expect("failed to write block");
        }
        
        self.tm += c.gettime();
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let mut o = Timings::new();
        o.add("write",self.tm);
        
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
        o.add_other("locations", OtherData::FileLocs(ls));
        
        Ok(o)
    }
}

pub struct WriteFileInternalLocs {
    fname: String,
    ischange:bool,
    data: Vec<(Quadtree,Vec<u8>)>,
    
}

impl WriteFileInternalLocs {
    pub fn new(fname: &str,ischange:bool) -> WriteFileInternalLocs {
        WriteFileInternalLocs{fname: String::from(fname), ischange:ischange,data: Vec::new()}
    }
}

impl CallFinish for WriteFileInternalLocs {
    type CallType = (Quadtree, Vec<u8>);
    type ReturnType = Timings;
    
    fn call(&mut self, q_d: Self::CallType) {
        self.data.push(q_d);
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let tx=ThreadTimer::new();
        self.data.sort_by_key(|p| { p.0 });
        let mut locs = Vec::with_capacity(self.data.len());
        for (a,b) in &self.data {
            locs.push((a.clone(),b.len() as u64));
        }
        let mut outf = File::create(&self.fname).expect("failed to create");
        let hb = pack_file_block("OSMHeader", &make_header_block_stored_locs(self.ischange,locs),true).expect("?");
        let mut pos = hb.len() as u64;
        outf.write_all(&hb).expect("?");
        
        let mut ls = Vec::with_capacity(self.data.len());
        for (a,b) in &self.data {
            ls.push((a.as_int(),vec![(b.len() as u64,pos)]));
            pos += b.len() as u64;
            outf.write_all(&b).expect("?");
        }
        
        let mut tm = Timings::new();
        tm.add("writefileinternallocs", tx.since());
        tm.add_other("locations", OtherData::FileLocs(ls));
        Ok(tm)
    }
}
    

pub fn make_packprimblock<T: CallFinish<CallType=Vec<(i64,Vec<u8>)>,ReturnType=Timings>>(out: Box<T>, includeqts: bool)
    -> Box<impl CallFinish<CallType=PrimitiveBlock,ReturnType=Timings>> {
    
    let conv = Box::new(move |bl: PrimitiveBlock| {
        let xx = bl.pack(includeqts, false).expect("failed to pack");
        let ob = pack_file_block("OSMData", &xx, true).expect("failed to pack fb");
        vec![(bl.index as i64, ob)]
    });
    return Box::new(CallAll::new(out, "pack", conv));
}

pub fn make_packprimblock_many<T: CallFinish<CallType=Vec<(i64,Vec<u8>)>,ReturnType=Timings>>(out: Box<T>, includeqts: bool)
    -> Box<impl CallFinish<CallType=Vec<PrimitiveBlock>,ReturnType=Timings>> {
    
    let conv = Box::new(move |bls: Vec<PrimitiveBlock>| {
        let mut res=Vec::new();
        for bl in bls {
            let xx = bl.pack(includeqts, false).expect("failed to pack");
            let ob = pack_file_block("OSMData", &xx, true).expect("failed to pack fb");
            res.push((bl.index as i64, ob));
        }
        res
    });
    return Box::new(CallAll::new(out, "pack", conv));
}
