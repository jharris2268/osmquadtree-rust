
use std::fs::File;


use std::io;
use std::io::{BufReader,Write};
use std::collections::BTreeMap;

mod osmquadtree { 
    pub use super::super::super::*;
}

use osmquadtree::elements::Quadtree;
use osmquadtree::sortblocks::quadtreetree::{QuadtreeTree,find_tree_groups};
use osmquadtree::elements::QuadtreeBlock;
use osmquadtree::read_file_block::{FileBlock,ReadFileBlocks};
use osmquadtree::callback::{CallbackSync,CallFinish,Callback,CallbackMerge};
use osmquadtree::utils::{Checktime,ReplaceNoneWithTimings,CallAll,MergeTimings,Timer};

use osmquadtree::sortblocks::{Timings,OtherData};
    
struct AddAll {
    groups: Option<Box<QuadtreeTree>>,
    tot: i64,
    ct: Checktime,
    tm: f64,
}

impl AddAll {
    pub fn new() -> AddAll {
        AddAll{groups: Some(Box::new(QuadtreeTree::new())), tot: 0, ct:Checktime::new(), tm:0.0}
    }
}

impl CallFinish for AddAll {
    type CallType = PrepedBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, mb: Self::CallType) {
        let tx = Timer::new();
        let groups = self.groups.as_mut().unwrap();
        for (q,w) in mb.2 {
            groups.add(q,w);
            self.tot+=w as i64;
        }
        
        match self.ct.checktime() {
            Some(d) => {
                print!("\r{:5.1}s: {:7.1}mb {} [tot={}]", d, (mb.1 as f64)/1024./1024.0, groups, self.tot);
                io::stdout().flush().expect("");
            },
            None => {},
        }
        self.tm += tx.since();
            
        
    }
    fn finish(&mut self) -> io::Result<Timings> {
        println!("");
        println!("{:5.1}s: {} [tot={}]", self.ct.gettime(), self.groups.as_ref().unwrap(), self.tot);
        
        let mut t = Timings::new();
        t.add("addall", self.tm);
        t.add_other("quadtreetree", OtherData::QuadtreeTree(self.groups.take().unwrap()));
        
        Ok(t)
    }
}

type PrepedBlock = (i64,u64,BTreeMap<Quadtree,u32>);
fn prep_block(qb: QuadtreeBlock, maxdepth: usize) -> PrepedBlock {
    let mut t=BTreeMap::new();
    for (_,q) in qb.nodes { 
        let q=q.round(maxdepth);
        if !t.contains_key(&q) {
            t.insert(q,1);
        } else {
            *t.get_mut(&q).unwrap()+=1;
        }
    }
    for (_,q) in qb.ways { 
        let q=q.round(maxdepth);
        if !t.contains_key(&q) {
            t.insert(q,1);
        } else {
            *t.get_mut(&q).unwrap()+=1;
        }
    }
    for (_,q) in qb.relations { 
        if q.as_int() < 0 {
            //pass
        } else {
            //let q = if q.as_int()<0 { Quadtree::new(0) } else {q.round(self.maxdepth)};
            if !t.contains_key(&q) {
                t.insert(q,1);
            } else {
                *t.get_mut(&q).unwrap()+=1;
            }
        }
    }
    (qb.idx, qb.loc, t)
}

fn make_convertquadtreeblock<T: CallFinish<CallType=PrepedBlock,ReturnType=Timings>>(
    out: Box<T>, maxdepth: usize) -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>> {
    
    let conv = Box::new(move |(i,fb):(usize,FileBlock) | {
        if fb.block_type=="OSMHeader" {
            (0,0,BTreeMap::new())
        } else {
            let qb = QuadtreeBlock::unpack(i as i64, fb.pos, &fb.data()).expect("failed to read block");
            prep_block(qb,maxdepth)
        }
    });
    
    Box::new(CallAll::new(out, "convertquadtreeblock", conv))
}


pub fn find_groups(qtsfn: &str, numchan: usize, maxdepth: usize, target: i64, mintarget: i64) -> io::Result<Box<QuadtreeTree>> {
    
    
    let f = File::open(qtsfn).expect("file not present");
    let mut fbuf = BufReader::new(f);
    
    let mut cc: Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=Timings>> = 
        if numchan > 0 {
            let aa = CallbackSync::new(Box::new(AddAll::new()),numchan);
            let mut bb: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
            for a in aa {
                let a2 = Box::new(ReplaceNoneWithTimings::new(a));
                bb.push(Box::new(Callback::new(make_convertquadtreeblock(a2,maxdepth))));        
            }
            Box::new(CallbackMerge::new(bb, Box::new(MergeTimings::new())))
        } else {
            make_convertquadtreeblock(Box::new(AddAll::new()),maxdepth)
        };
    
    
    for (i,fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
        cc.call((i,fb));            
    }
    
    let mut t = cc.finish()?;
    println!("{}", t);
    
    let mut tree: Option<Box<QuadtreeTree>> = None;
    for (_,b) in std::mem::take(&mut t.others) {
        match b {
            OtherData::QuadtreeTree(t) => { tree=Some(t); },
            _ => {}
        }
    }
    
    
    /*let mut ft: Option<Box<QuadtreeTree>> = None;
    for mut a in bb {
        match a.finish()? {
            Some(g) => ft=Some(g),
            None => {}
        }
    }
    let tree = ft.unwrap();*/
    let tree=tree.unwrap();
    println!("{}", tree);
    
    
    let groups = find_tree_groups(tree, target, mintarget).expect("find_tree_groups failed");
    
    
    
    Ok(groups)
    
    //Err(io::Error::new(ErrorKind::Other,"not impl"))
}
