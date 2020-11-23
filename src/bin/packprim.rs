extern crate osmquadtree;

use std::env;

use osmquadtree::utils::{ConsumeAll,ReplaceNoneWithTimings,MergeTimings};
use osmquadtree::callback::{CallFinish,Callback,CallbackSync,CallbackMerge};
use osmquadtree::read_file_block::{FileBlock,read_all_blocks};
use osmquadtree::elements::{PrimitiveBlock};
use osmquadtree::stringutils::StringUtils;
use osmquadtree::sortblocks::sortblocks::{AddQuadtree,make_packprimblock,make_unpackprimblock,WriteFile};
use osmquadtree::sortblocks::{Timings,OtherData};


fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
    //let mut includeqts=false;
    let mut numchan=4;
    let mut outfn = String::new();
    let mut qtsfn = String::new();
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("outfn=") {
                outfn = args[i].substr(6,args[i].len());
            } else if args[i].starts_with("qtsfn=") {
                qtsfn = args[i].substr(6,args[i].len());
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            }
        }
    }
    if outfn.is_empty() {
        outfn = format!("{}-out.pbf", fname.substr(0,fname.len()-4));
    }
    if qtsfn.is_empty() {
        qtsfn = format!("{}-qts.pbf", fname.substr(0,fname.len()-4));
    }
    
    if outfn == "NONE" {
        
        let cc = Box::new(ConsumeAll::new());
        let aqs = CallbackSync::new(Box::new(AddQuadtree::new(&qtsfn, cc)),numchan);
    
        let mut pp: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
        for aq in aqs {
            let aq2 = Box::new(ReplaceNoneWithTimings::new(aq));
            //pp.push(Box::new(Callback::new(Box::new(UnpackPrimBlock::new(aq2)))));
            pp.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
        }
        
        let ppm = Box::new(CallbackMerge::new(pp,Box::new(MergeTimings::new())));
        
        let (res,d) = read_all_blocks(&fname, ppm);
        println!("\n{:8.3}s Total, {}", d, res);
        
    } else if numchan == 0 {
        let wf = Box::new(WriteFile::new(&outfn, false));
        //let cc = Box::new(PackPrimBlock::new(wf, true));
        let cc = make_packprimblock(wf,true);
        let aq = Box::new(AddQuadtree::new(&qtsfn, cc));
        //let pp = Box::new(UnpackPrimBlock::new(aq));
        let pp = make_unpackprimblock(aq);
        
        let (res,d) = read_all_blocks(&fname, pp);
        
        let mut locs = Vec::new();
        for o in &res.others {
            match o {
                (_,OtherData::FileLocs(l)) => { locs.extend(l.iter()); },
                 _ => {}
            }
        }
        println!("\n{:8.3}s Total, {} [{} locs]", d, res, locs.iter().map(|(_,b)| { b.len() as i64 }).sum::<i64>());
        
        
    } else {
        
        let wws = CallbackSync::new(Box::new(WriteFile::new(&outfn,false)),numchan);
        let mut ccs: Vec<Box<dyn CallFinish<CallType=PrimitiveBlock,ReturnType=Timings>>> = Vec::new();
        for ww in wws {
            let w2 = Box::new(ReplaceNoneWithTimings::new(ww));
            //ccs.push(Box::new(Callback::new(Box::new(PackPrimBlock::new(w2, true)))));
            ccs.push(Box::new(Callback::new(make_packprimblock(w2,true))));
        }
        let cc = Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new())));
        let aqs = CallbackSync::new(Box::new(AddQuadtree::new(&qtsfn, cc)),numchan);
    
        let mut pp: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
        for aq in aqs {
            let aq2 = Box::new(ReplaceNoneWithTimings::new(aq));
            //pp.push(Box::new(Callback::new(Box::new(UnpackPrimBlock::new(aq2)))));
            pp.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
        }
        
        let ppm = Box::new(CallbackMerge::new(pp,Box::new(MergeTimings::new())));
        let (res,d) = read_all_blocks(&fname, ppm);
        
        let mut locs = Vec::new();
        for o in &res.others {
            match o {
                (_,OtherData::FileLocs(l)) => { locs.extend(l.iter()); },
                _ => {}
            }
        }
        println!("\n{:8.3}s Total, {} [{} locs]", d, res, locs.iter().map(|(_,b)| { b.len() as i64 }).sum::<i64>());
        
        //let mut ppm = Box::new(CallbackMerge::new(pp, Box::new(MergeRes())));
        
    };
    //let (writet,unpack,pack,prep,unpack2) = ppm.finish().expect("finish failed");
   
    //println!("unpack first: {:0.3}s, pack: {:0.3}s, prep: {:0.3}s, unpack second: {:0.3}s, write: {:0.3}s", unpack,pack,prep,unpack2,writet);
            
}

