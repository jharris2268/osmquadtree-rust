extern crate serde_json;

use std::fs::File;
use std::io::{Seek,SeekFrom,Write};
use std::io;
use std::collections::HashMap;
mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::elements::PrimitiveBlock;
use osmquadtree::read_file_block::{pack_file_block};
use osmquadtree::callback::{CallFinish,};
use osmquadtree::utils::{Checktime,CallAll};
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

pub struct WriteFile {
    outf: File,
    withlocs: bool,
    locs: HashMap<i64,Vec<(u64,u64)>>,
    tm: f64,
    fname: String
}

impl WriteFile {
    pub fn new(outfn: &str, withlocs: bool) -> WriteFile {
        let mut outf = File::create(outfn).expect("failed to create");
        outf.write_all(&pack_file_block("OSMHeader", &make_header_block(withlocs),true).expect("?")).expect("?");
        WriteFile{outf: outf, tm: 0.0, withlocs:withlocs, locs:HashMap::new(), fname: String::from(outfn)}
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
            if self.withlocs {
                self.add_loc(*i, d.len() as u64);
            }
            self.outf.write_all(d).expect("failed to write block");
        }
        
        self.tm += c.gettime();
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let mut o = Timings::new();
        o.add("write",self.tm);
        if self.withlocs {
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
            lf.sort_by_key(|p| { p.1 });
            let jf = File::create(format!("{}-filelocs.json", self.fname)).expect("failed to create filelocs file");
            serde_json::to_writer(jf, &lf).expect("failed to write filelocs json");
                        
            o.add_other("locations", OtherData::FileLocs(ls));
        }
        Ok(o)
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
