mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::elements::{MinimalBlock,Quadtree};
use osmquadtree::write_pbf::{pack_data,pack_value,pack_delta_int, zig_zag};
use osmquadtree::read_pbf::{IterTags,DeltaPackedInt,PbfTag,un_zig_zag};
use osmquadtree::read_file_block::{pack_file_block, read_all_blocks, FileBlock};
use osmquadtree::callback::{Callback,CallbackMerge,CallbackSync,CallFinish};
use osmquadtree::utils::{ThreadTimer,ReplaceNoneWithTimings,MergeTimings,CallAll};

use osmquadtree::update::ChangeBlock;

pub enum ResultType {
    NumTiles(usize),
    CheckIndexResult((Vec<Quadtree>,ChangeBlock))
}

type Timings = osmquadtree::utils::Timings<ResultType>;

use std::fs::File;
use std::io::{Result,Write};
use std::collections::BTreeSet;

fn prep_index_block(mb: &MinimalBlock) -> Vec<u8> {
    let mut res = Vec::with_capacity(20+5*mb.len());
    
    pack_value(&mut res, 1, zig_zag(mb.quadtree.as_int()));
    if !mb.nodes.is_empty() {
        pack_data(&mut res, 2, &pack_delta_int(mb.nodes.iter().map(|n| {n.id})));
    }
    if !mb.ways.is_empty() {
        pack_data(&mut res, 3, &pack_delta_int(mb.ways.iter().map(|w| {w.id})));
    }
    if !mb.relations.is_empty() {
        pack_data(&mut res, 4, &pack_delta_int(mb.relations.iter().map(|r| {r.id})));
    }
    
    res.shrink_to_fit();
    
    res
}

fn check_index_block(bl: Vec<u8>, changeblock: &ChangeBlock, exnodes: &BTreeSet<i64>) -> Option<Quadtree> {
    
    let mut qt = Quadtree::new(-2);
    for tg in IterTags::new(&bl, 0) {
        match tg {
            PbfTag::Value(1, q) => qt = Quadtree::new(un_zig_zag(q)),
            PbfTag::Data(2, nn) => {
                for n in DeltaPackedInt::new(&nn) {
                    if changeblock.nodes.contains_key(&n) {
                        return Some(qt);
                    } else if exnodes.contains(&n) {
                        return Some(qt);
                    }
                }
            },
            PbfTag::Data(3, ww) => {
                for w in DeltaPackedInt::new(&ww) {
                    if changeblock.ways.contains_key(&w) {
                        return Some(qt);
                    }
                }
            },
            PbfTag::Data(4, rr) => {
                for r in DeltaPackedInt::new(&rr) {
                    if changeblock.relations.contains_key(&r) {
                        return Some(qt);
                    }
                }
            }
            _ => {}
        }
    }
    return None;
}
    
    
        
            


struct WF {
    f: File,
    nt: usize,
    tm: f64
}

impl WF {
    pub fn new(outfn: &str) -> WF {
        WF{f: File::create(outfn).expect("failed to create file"), nt: 0, tm: 0.0}
    }
}

impl CallFinish for WF {
    type CallType = Vec<u8>;
    type ReturnType = Timings;

    fn call(&mut self, d: Vec<u8>) {
        if d.is_empty() {return; }
        let tx=ThreadTimer::new();
        self.f.write_all(&d).expect("failed to write data");
        self.tm += tx.since();
        self.nt+=1;
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = Timings::new();
        tms.add("write", self.tm);
        tms.add_other("num_tiles", ResultType::NumTiles(self.nt));
        Ok(tms)
    }
}

fn convert_indexblock(i_fb: (usize,FileBlock)) -> Vec<u8> {
    if i_fb.1.block_type != "OSMData" {
        return Vec::new();
    }
    
    let mb = MinimalBlock::read(i_fb.0 as i64, i_fb.1.pos, &i_fb.1.data(), false).expect("MinimalBlock::read failed");
    let d = prep_index_block(&mb);
    pack_file_block("IndexBlock", &d, true).expect("pack_file_block failed")
}

pub fn write_index_file(infn: &str, outfn: &str, numchan: usize) -> usize {
    
    let (mut tm,br) = 
        if numchan == 0 {
            let wf = Box::new(WF::new(outfn));
            let pack = Box::new(CallAll::new(wf, "convert", Box::new(convert_indexblock)));
            read_all_blocks(infn, pack)
        } else {
            let wfs = CallbackSync::new(Box::new(WF::new(outfn)), numchan);
    
            let mut packs: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=Timings>>> = Vec::new();
    
            for wf in wfs {
                let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
                packs.push(Box::new(Callback::new(Box::new(CallAll::new(wf2, "convert", Box::new(convert_indexblock))))));
            }
            let mt = Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new())));
            read_all_blocks(infn, mt)
        };
    
    println!("{} bytes: {}", br, tm);
    match tm.others.pop().unwrap().1 {
        ResultType::NumTiles(nt) => { return nt; }
        _ => { panic!("??"); }
    }
        
}

struct CheckIndexFile {
    changeblock: Option<ChangeBlock>, 
    exnodes: BTreeSet<i64>,
    quadtrees: Vec<Quadtree>,
    tm: f64,
}

impl CheckIndexFile {
    pub fn new(changeblock: ChangeBlock, exnodes: BTreeSet<i64>) -> CheckIndexFile {
        CheckIndexFile{changeblock: Some(changeblock), exnodes: exnodes, quadtrees: Vec::new(), tm: 0.0}
    }
}

impl CallFinish for CheckIndexFile {
    type CallType = Vec<u8>;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: Vec<u8>) {
        let tx=ThreadTimer::new();
        match check_index_block(bl, self.changeblock.as_ref().unwrap(), &self.exnodes) {
            Some(q) => self.quadtrees.push(q),
            None => {}
        }
        self.tm += tx.since();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        
        let mut t = Timings::new();
        t.add("check index", self.tm);
        
        let cb = self.changeblock.take();
        let qts = std::mem::take(&mut self.quadtrees);
        t.add_other("result", ResultType::CheckIndexResult((qts,cb.unwrap())));
        Ok(t)
    }
}

fn unpack_fb(i_fb: (usize,FileBlock)) -> Vec<u8> {
    if i_fb.1.block_type != "IndexBlock" {
        return Vec::new();
    }
    i_fb.1.data()
}


pub fn check_index_file(indexfn: &str, changeblock: ChangeBlock, exnodes: BTreeSet<i64>, numchan: usize) -> (Vec<Quadtree>,ChangeBlock) {
    if numchan == 0 {
        let ci = Box::new(CheckIndexFile::new(changeblock, exnodes));
        
        let ca = Box::new(CallAll::new(ci, "unpack", Box::new(unpack_fb)));
        
        let (mut tm,x) = read_all_blocks(indexfn, ca);
        
        println!("{} {}", tm, x);
        
        if tm.others.len()!=1 {
            panic!("!!");
        }
        match tm.others.pop().unwrap().1 {
            ResultType::CheckIndexResult(r) => {return r;},
            _ => { panic!("??"); }
        }
    } else {
        
        panic!("not impl");
    }
        
        
}
        
        
    
    
