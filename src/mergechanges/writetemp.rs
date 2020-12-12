use crate::elements::{PrimitiveBlock,IdSet,IdSetAll,Node,Way,Relation,WithId,Quadtree};
use crate::callback::{Callback,CallFinish,CallbackMerge,CallbackSync};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all_idset;

use crate::pbfformat::read_file_block::{ProgBarWrap,read_all_blocks_parallel_prog,FileBlock};
use crate::utils::{ThreadTimer,MergeTimings,ReplaceNoneWithTimings,LogTimes,parse_timestamp,CallAll};
use crate::mergechanges::inmem::{read_filter,make_write_file};
use crate::mergechanges::filter_elements::prep_bbox_filter;
use crate::sortblocks::{Timings,OtherData};
use crate::sortblocks::writepbf::{make_packprimblock_many};
use crate::update::{ParallelFileLocs,get_file_locs};
use crate::sortblocks::sortblocks::{TempData,WriteTemp,read_temp_data};

use std::sync::Arc;
use std::io::{Result,Error,ErrorKind};
use std::collections::BTreeMap;

struct CollectObj<T: WithId> {
    split: i64,
    pub currs: BTreeMap<i64,Vec<T>>,
    limit: usize
}

impl<T> CollectObj<T>
where T: WithId {
    pub fn new(split: i64, limit: usize) -> CollectObj<T> {
        CollectObj{split:split, currs: BTreeMap::new(), limit: limit}
    }
    
    pub fn add(&mut self, o: T) -> Option<(i64,Vec<T>)>{
        let k = o.get_id() / self.split;
        match self.currs.get_mut(&k) {
            None => {
                let mut v = Vec::with_capacity(self.limit);
                v.push(o);
                self.currs.insert(k, v);
                None
            },
            Some(cc) => { 
                cc.push(o);
                match cc.len() >= self.limit {
                    false => None,
                    true => Some((k,std::mem::replace(&mut *cc, Vec::with_capacity(self.limit))))
                }
            }
        }
    }
    
          
    
}

        

struct CollectTemp<T> {
    out: Box<T>,    
    curr_node: CollectObj<Node>,
    curr_way: CollectObj<Way>,
    curr_relation: CollectObj<Relation>,
    tm: f64
}

impl<T> CollectTemp<T>
where T: CallFinish<CallType=Vec<PrimitiveBlock>, ReturnType=Timings> {
    pub fn new(out: Box<T>, limit: usize, splitat: (i64,i64,i64)) -> CollectTemp<T> {
        CollectTemp{
            out: out,
            curr_node: CollectObj::new(splitat.0, limit),
            curr_way: CollectObj::new(splitat.1, limit),
            curr_relation: CollectObj::new(splitat.2, limit),
            tm: 0.0
        }
    }
    
    fn add_node(&mut self, n: Node) -> Option<PrimitiveBlock> {
        
        match self.curr_node.add(n) {
            None => None,
            Some((q,nn)) => {
                let mut pb = PrimitiveBlock::new(q,0);
                pb.quadtree=Quadtree::new(q);
                pb.nodes.extend(nn);
                Some(pb)
            }
        }
    }
    fn add_way(&mut self, w: Way) -> Option<PrimitiveBlock> {
        
        match self.curr_way.add(w) {
            None => None,
            Some((q,ww)) => {
                let mut pb = PrimitiveBlock::new(q + (1<<25),0);
                pb.quadtree=Quadtree::new(q);
                pb.ways.extend(ww);
                Some(pb)
            }
        }
    }
    fn add_relation(&mut self, r: Relation) -> Option<PrimitiveBlock> {
        
        match self.curr_relation.add(r) {
            None => None,
            Some((q,rr)) => {
                let mut pb = PrimitiveBlock::new(q + (2<<25),0);
                pb.quadtree=Quadtree::new(q);
                pb.relations.extend(rr);
                Some(pb)
            }
        }
    }
    
    fn add_all(&mut self, pb: PrimitiveBlock) -> Vec<PrimitiveBlock> {
        let mut res = Vec::new();
        for n in pb.nodes {
            match self.add_node(n) {
                None => {},
                Some(pb) => { res.push(pb); }
            }
        }
        for w in pb.ways {
            match self.add_way(w) {
                None => {},
                Some(pb) => { res.push(pb); }
            }
        }
        for r in pb.relations {
            match self.add_relation(r) {
                None => {},
                Some(pb) => { res.push(pb); }
            }
        }
        res
    }
    
    fn finish_all(&mut self) -> Vec<PrimitiveBlock> {
        let mut res=Vec::new();
        for (q,nn) in std::mem::take(&mut self.curr_node.currs) {
            if nn.len() > 0 {
                let mut pb = PrimitiveBlock::new(q ,0);
                pb.quadtree=Quadtree::new(q);
                pb.nodes.extend(nn);
                pb.sort();
                res.push(pb);
            }
        }
        for (q,ww) in std::mem::take(&mut self.curr_way.currs) {
            if ww.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + (1<<25),0);
                pb.quadtree=Quadtree::new(q);
                pb.ways.extend(ww);
                pb.sort();
                res.push(pb);
            }
        }
        for (q,rr) in std::mem::take(&mut self.curr_relation.currs) {
            if rr.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + (2<<25),0);
                pb.quadtree=Quadtree::new(q);
                pb.relations.extend(rr);
                pb.sort();
                res.push(pb);
            }
        }
        res
    }
}

impl<T> CallFinish for CollectTemp<T>
where T: CallFinish<CallType=Vec<PrimitiveBlock>, ReturnType=Timings> {
    
    type CallType=PrimitiveBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, pb: PrimitiveBlock) {
        let tx=ThreadTimer::new();
        let res = self.add_all(pb);
        self.tm += tx.since();
        self.out.call(res);
    }
    
    fn finish(&mut self) -> Result<Timings> {
        
        let tx=ThreadTimer::new();
        let res = self.finish_all();
        let ftm = tx.since();
                
        self.out.call(res);
        
        let mut tms = self.out.finish()?;
        tms.add("CollectTemp::call", self.tm);
        tms.add("CollectTemp::finish", ftm);
        
        Ok(tms)
    }
}


pub fn write_temp_blocks(
    pfilelocs: &mut ParallelFileLocs,
    ids: Arc<dyn IdSet>,
    tempfn: &str,
    limit: usize,
    splitat: (i64,i64,i64),
    numchan: usize,
) -> Result<TempData> {
    
    let wt = Box::new(WriteTemp::new(&tempfn));

    let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!(
        "write_temp_blocks to {}, numchan={}",
        tempfn, numchan
    ));
    
    let (mut res, d) = if numchan == 0 {
        let pc = make_packprimblock_many(wt, true);
        let cc = Box::new(CollectTemp::new(pc, limit, splitat));
        let pp = make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true);

        read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog)
    } else {
        let wts = CallbackSync::new(wt, numchan);
        let mut pcs: Vec<Box<dyn CallFinish<CallType = (usize,Vec<FileBlock>), ReturnType = Timings>>> =
            Vec::new();
        for wt in wts {
            let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
            let pc = make_packprimblock_many(wt2, true);
            let cc = Box::new(CollectTemp::new(pc, limit, splitat));
            pcs.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true))));
        }
        let pc = Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())));
        read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pc, &prog)
    };
    prog.finish();
    println!("write_temp_blocks {} {}", res, d);
    
    for (_,b) in std::mem::take(&mut res.others) {
        match b {
            OtherData::FileLocs(fl) => { return Ok(TempData::TempFile((tempfn.to_string(), fl))); },
            OtherData::TempData(td) => { return Ok(TempData::TempBlocks(td)); },
            _ => {}
        }
    }
    
    Err(Error::new(ErrorKind::Other, "no temp data?"))
}


fn collect_blocks((k, fbs): (i64, Vec<FileBlock>)) -> PrimitiveBlock {
    
    let mut pb = PrimitiveBlock::new(k, 0);
    for fb in fbs {
        let t = PrimitiveBlock::read(0, fb.pos, &fb.data(), false, false).expect("?");
        pb.extend(t);
    }
    pb.sort();
    pb
}
    


pub fn run_mergechanges(inprfx: &str, outfn: &str, tempfn: Option<&str>, filter: Option<&str>, timestamp: Option<&str>, numchan: usize) -> Result<()> {
    let mut tx=LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;
    
    println!("bbox={}, poly={:?}", bbox, poly);
    
    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?)
    };
    
    let mut pfilelocs = get_file_locs(inprfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");
    
    let ids:Arc<dyn IdSet> = match filter {
        Some(_) => {
            let ids = prep_bbox_filter(&mut pfilelocs, bbox.clone(), poly, numchan)?;
            tx.add("prep_bbox_filter");
            println!("have: {}", ids);
            Arc::from(ids)
        },
        None => { Arc::new(IdSetAll()) }
    };
    
    let tempfn = match tempfn {
        Some(t) => t.to_string(),
        None => { format!("{}-temp.pbf", &outfn[0..outfn.len()-4]) }
    };
    
    let mut limit=250;
    if tempfn == "NONE" {
        limit=100;
    }
    
    let temps = write_temp_blocks(&mut pfilelocs, ids.clone(), &tempfn, limit, (1i64<<20, 1i64<<17, 1i64<<14), numchan)?;
    tx.add("write_temp_blocks");
    match &temps {
        TempData::TempFile((a,b)) => {
            println!("wrote {} / {} blocks to {}", b.len(), b.iter().map(|(_,p)| { p.len() }).sum::<usize>(), a);
            serde_json::to_writer(std::fs::File::create(&format!("{}-locs.json", a))?,&b)?;
            },
        TempData::TempBlocks(bl) => {println!("have {} / {} temp blocks", bl.len(), bl.iter().map(|(_,p)| { p.len() }).sum::<usize>()); }
    }
    
    let wf = make_write_file(outfn, bbox, 8000, numchan);
    
    let res = if numchan == 0 {
        read_temp_data(temps, Box::new(CallAll::new(wf, "unpack temp", Box::new(collect_blocks))))?
    } else {
        
        let mut ccs: Vec<Box<dyn CallFinish<CallType=(i64,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for wf in CallbackSync::new(wf, numchan) {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            ccs.push(Box::new(Callback::new(Box::new(CallAll::new(wf2, "unpack temp", Box::new(collect_blocks))))));
        }
    
        read_temp_data(temps, Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new()))))?
    };
    println!("{}",res);
    
    tx.add("write final");
    
    println!("{}", tx);
    
    Ok(())
    
}
