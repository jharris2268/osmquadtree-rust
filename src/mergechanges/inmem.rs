use crate::elements::{PrimitiveBlock,IdSet,Bbox};
use crate::callback::{Callback,CallFinish,CallbackMerge,CallbackSync};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all_idset;
use crate::pbfformat::header_block::HeaderType;
use crate::pbfformat::read_file_block::{ProgBarWrap,read_all_blocks_parallel_prog,FileBlock};
use crate::utils::{ThreadTimer,MergeTimings,ReplaceNoneWithTimings};
use crate::mergechanges::filter_elements::{prep_bbox_filter,Poly};
use crate::sortblocks::{WriteFile};
use crate::sortblocks::writepbf::make_packprimblock;
use crate::update::{get_file_locs,ParallelFileLocs};

use std::sync::Arc;
use std::io::Result;

type Timings = crate::utils::Timings<PrimitiveBlock>;

struct CollectObjs {
    collected: Option<PrimitiveBlock>,
    tm: f64
}

impl CollectObjs {
    pub fn new() -> CollectObjs {
        CollectObjs{collected: Some(PrimitiveBlock::new(0,0)), tm: 0.0}
    }
}

impl CallFinish for CollectObjs {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        let tx=ThreadTimer::new();
        self.collected.as_mut().unwrap().nodes.extend(bl.nodes);
        self.collected.as_mut().unwrap().ways.extend(bl.ways);
        self.collected.as_mut().unwrap().relations.extend(bl.relations);
        self.tm += tx.since();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add("CollectObjs::call", self.tm);
        let tx=ThreadTimer::new();
        let mut bl = std::mem::take(&mut self.collected).unwrap();
        bl.sort();
        
        tm.add("CollectedObjs::finish", tx.since());
        tm.add_other("objs", bl);
        Ok(tm)
    }
}


pub fn collect_blocks_filtered(pfilelocs: &mut ParallelFileLocs, ids: Arc<IdSet>, numchan: usize) -> Result<PrimitiveBlock> {
    
    
    
    let mut pb = ProgBarWrap::new(100);
    pb.set_range(100);
    pb.set_message("merge blocks");
    
    
    
    
    
    let conv: Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>), ReturnType=Timings>> = 
    if numchan == 0 {
        let co = Box::new(CollectObjs::new());
        make_read_primitive_blocks_combine_call_all_idset(co, ids.clone())
    } else {
        
        let mut convs: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>), ReturnType=Timings>>>=Vec::new();
        for _ in 0..numchan {
            let co = Box::new(CollectObjs::new());
            convs.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all_idset(co, ids.clone()))));
        }
        Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
    };
    
    let (tm,_) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, conv, &pb);
    pb.finish();
    
    let mut res = PrimitiveBlock::new(0,0);
    for (_,x) in tm.others {
        res.nodes.extend(x.nodes);
        res.ways.extend(x.ways);
        res.relations.extend(x.relations);
    }
    res.sort();
    
    Ok(res)
}


fn group_blocks<T: CallFinish<CallType=PrimitiveBlock, ReturnType=crate::sortblocks::Timings> + ?Sized>(pb: PrimitiveBlock, size: usize, ids: &IdSet, mut out: Box<T>) -> Result<crate::sortblocks::Timings> {
    
    let mut curr = PrimitiveBlock::new(0,0);
    
    for n in pb.nodes {
        curr.nodes.push(n);
        if curr.len() >= size {
            out.call(std::mem::replace(&mut curr, PrimitiveBlock::new(0,0)));
        }
    }
    
    for w in pb.ways {
        curr.ways.push(w);
        if curr.len() >= size {
            out.call(std::mem::replace(&mut curr, PrimitiveBlock::new(0,0)));
        }
    }
    let mut nr=0;
    for mut r in pb.relations {
        if r.filter_relations(ids) {
            nr+=1;
        }
        curr.relations.push(r);
        if curr.len() >= size {
            out.call(std::mem::replace(&mut curr, PrimitiveBlock::new(0,0)));
        }
    }
    if curr.len() > 0{
        out.call(std::mem::replace(&mut curr, PrimitiveBlock::new(0,0)));
    }
    println!("filtered {} rels", nr);
    out.finish()
}


            
fn read_filter(filter: &str) -> Result<(Bbox, Option<Poly>)> {
    
    match Bbox::from_str(filter) {
        Ok(bbox) => { return Ok((bbox, None)); },
        Err(_) => {}
    }
    
    let poly = Poly::from_file(filter)?;
    let bbox = poly.bounds();
    
    Ok((bbox, Some(poly)))
}


pub fn run_mergechanges_inmem(inprfx: &str, outfn: &str, filter: &str, numchan: usize) -> Result<()> {
    
    let (bbox, poly) = read_filter(filter)?;
    
    let mut pfilelocs = get_file_locs(inprfx, Some(bbox.clone()))?;
    
    let ids = prep_bbox_filter(&mut pfilelocs, bbox.clone(), poly, numchan)?;
    
    
    let ids:Arc<IdSet> = Arc::from(ids);
    
    let pb = collect_blocks_filtered(&mut pfilelocs, ids.clone(), numchan)?;
    
    println!("have {} nodes, {} ways, {} relations", pb.nodes.len(), pb.ways.len(), pb.relations.len());
    
    let wf = Box::new(WriteFile::with_bbox(outfn, HeaderType::NoLocs, Some(&bbox)));
    
    let pack: Box<dyn CallFinish<CallType=PrimitiveBlock,ReturnType=crate::sortblocks::Timings>> = if numchan == 0 {
        make_packprimblock(wf, false)
    } else {
        
        let wff = CallbackSync::new(wf, 4);
        let mut packs: Vec<Box<dyn CallFinish<CallType=PrimitiveBlock,ReturnType=crate::sortblocks::Timings>>> = Vec::new();
        for w in wff {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            packs.push(Box::new(Callback::new(make_packprimblock(w2, false))));
        }
        
        Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new())))
    };
    
    let tm = group_blocks(pb, 8000, &ids, pack)?;
    println!("{}",tm);
        
    
    Ok(())
}  
