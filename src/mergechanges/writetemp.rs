use crate::elements::{PrimitiveBlock,IdSet,IdSetAll,Node,Way,Relation,WithId,Quadtree,Bbox};
use crate::callback::{Callback,CallFinish,CallbackMerge,CallbackSync};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all_idset;

use crate::pbfformat::read_file_block::{ProgBarWrap,read_all_blocks_parallel_prog,FileBlock};
use crate::utils::{ThreadTimer,MergeTimings,ReplaceNoneWithTimings,LogTimes,parse_timestamp,CallAll};
use crate::mergechanges::inmem::{read_filter,make_write_file};
use crate::mergechanges::filter_elements::prep_bbox_filter;
use crate::sortblocks::{Timings,OtherData,TempData};
use crate::sortblocks::writepbf::{make_packprimblock_many};
use crate::update::{ParallelFileLocs,get_file_locs};
use crate::sortblocks::sortblocks::{WriteTempWhich,WriteTempData,WriteTempFile,WriteTempFileSplit,
        read_temp_data, read_tempfile_locs, read_tempfilesplit_locs, write_tempfile_locs,
        write_tempfilesplit_locs};

use std::sync::Arc;
use std::io::{Result,Error,ErrorKind};
use std::collections::BTreeMap;

struct CollectObj<T: WithId> {
    split: i64,
    currs: BTreeMap<i64,Vec<T>>,
    limit: usize,
    max_key:i64
}

impl<T> CollectObj<T>
where T: WithId {
    pub fn new(split: i64, limit: usize, max_key: i64) -> CollectObj<T> {
        CollectObj{split:split, currs: BTreeMap::new(), limit: limit, max_key: max_key}
    }
    
    pub fn add(&mut self, o: T) -> Option<(i64,Vec<T>)>{
        let k = i64::min(o.get_id() / self.split, self.max_key);
        match self.currs.get_mut(&k) {
            None => {
                let mut v = Vec::with_capacity(self.limit);
                v.push(o);
                self.currs.insert(k, v);
                None
            },
            Some(cc) => { 
                cc.push(o);
                if self.limit > 0 {
                    match cc.len() >= self.limit {
                        false => None,
                        true => Some((k,std::mem::replace(&mut *cc, Vec::with_capacity(self.limit))))
                    }
                } else {
                    None
                }
            }
        }
    }
    
    pub fn get_all(&mut self) -> BTreeMap<i64,Vec<T>> {
        std::mem::take(&mut self.currs)
    }
    
}

        

struct CollectTemp<T> {
    out: Box<T>,    
    curr_node: CollectObj<Node>,
    curr_way: CollectObj<Way>,
    curr_relation: CollectObj<Relation>,
    way_off: i64,
    rel_off: i64,
    count: usize,
    write_at: usize,
    tm: f64
}

const MAX_NODE_ID:i64 = 16<<30;
const MAX_WAY_ID:i64 = 2<<30;

impl<T> CollectTemp<T>
where T: CallFinish<CallType=Vec<PrimitiveBlock>, ReturnType=Timings> {
    pub fn new(out: Box<T>, limit: usize, splitat: (i64,i64,i64), write_at: usize) -> CollectTemp<T> {
        let way_off = MAX_NODE_ID/splitat.0;
        let rel_off = way_off + MAX_WAY_ID/splitat.1;
        
        CollectTemp{
            out: out,
            curr_node: CollectObj::new(splitat.0, limit, MAX_NODE_ID/splitat.0 - 1),
            curr_way: CollectObj::new(splitat.1, limit, MAX_WAY_ID/splitat.1 - 1),
            curr_relation: CollectObj::new(splitat.2, limit, 1024),
            way_off: way_off,
            rel_off: rel_off,
            count: 0,
            write_at: write_at,
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
                let mut pb = PrimitiveBlock::new(q + self.way_off,0);
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
                let mut pb = PrimitiveBlock::new(q + self.rel_off,0);
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
            self.count+=1;
        }
        for w in pb.ways {
            match self.add_way(w) {
                None => {},
                Some(pb) => { res.push(pb); }
            }
            self.count+=5;
        }
        for r in pb.relations {
            match self.add_relation(r) {
                None => {},
                Some(pb) => { res.push(pb); }
            }
            self.count+=10;
        }
        
        if self.write_at>0 && self.count > self.write_at {
            res.extend(self.finish_all());
        }       
        res
    }
    
    fn finish_all(&mut self) -> Vec<PrimitiveBlock> {
        let mut res=Vec::new();
        for (q,mut nn) in self.curr_node.get_all() {
            if nn.len() > 0 {
                let mut pb = PrimitiveBlock::new(q ,0);
                pb.quadtree=Quadtree::new(q);
                pb.nodes.append(&mut nn);
                pb.sort();
                res.push(pb);
            }
        }
        for (q,mut ww) in self.curr_way.get_all() {
            if ww.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + self.way_off,0);
                pb.quadtree=Quadtree::new(q);
                pb.ways.append(&mut ww);
                pb.sort();
                res.push(pb);
            }
        }
        for (q,mut rr) in self.curr_relation.get_all() {
            if rr.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + self.rel_off,0);
                pb.quadtree=Quadtree::new(q);
                pb.relations.append(&mut rr);
                pb.sort();
                res.push(pb);
            }
        }
        self.count=0;
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
    write_at: usize,
    splitat: (i64,i64,i64),
    fsplit: i64,
    numchan: usize,
) -> Result<TempData> {
    
    let wt: Box<WriteTempWhich> = if tempfn == "NONE" {
        Box::new(WriteTempWhich::Data(WriteTempData::new()))
    } else {
        if fsplit == 0 {
            Box::new(WriteTempWhich::File(WriteTempFile::new(tempfn.clone())))
        } else {
            Box::new(WriteTempWhich::Split(WriteTempFileSplit::new(tempfn.clone(),fsplit)))
        }
    };

    let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!(
        "write_temp_blocks to {}, numchan={}",
        tempfn, numchan
    ));
    
    let (mut res, d) = if numchan == 0 {
        let pc = make_packprimblock_many(wt, true);
        let cc = Box::new(CollectTemp::new(pc, 0, splitat, write_at));
        let pp = make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true);

        read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog)
    } else {
        let wts = CallbackSync::new(wt, numchan);
        let mut pcs: Vec<Box<dyn CallFinish<CallType = (usize,Vec<FileBlock>), ReturnType = Timings>>> =
            Vec::new();
        for wt in wts {
            let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
            let pc = make_packprimblock_many(wt2, true);
            let cc = Box::new(CollectTemp::new(pc, 0, splitat, write_at/numchan));
            pcs.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true))));
        }
        let pc = Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())));
        read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pc, &prog)
    };
    prog.finish();
    println!("write_temp_blocks {} {}", res, d);
    
    for (_,b) in std::mem::take(&mut res.others) {
        match b {
            OtherData::TempData(td) => { return Ok(td); },
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
    


pub fn run_mergechanges(inprfx: &str, outfn: &str, tempfn: Option<&str>, filter: Option<&str>, timestamp: Option<&str>, keep_temps: bool, numchan: usize) -> Result<()> {
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
    
    let mut limit=1500000;
    if tempfn == "NONE" {
        limit=200;
    }
    let fsplit = if filter.is_none() {
        128
    } else {
        0
    };
        
    let temps = write_temp_blocks(&mut pfilelocs, ids.clone(), &tempfn, limit, (1i64<<21, 1i64<<18, 1i64<<17), fsplit, numchan)?;
    tx.add("write_temp_blocks");
    match &temps {
        TempData::TempFile((a,b)) => {
            println!("wrote {} / {} blocks to {}", b.len(), b.iter().map(|(_,p)| { p.len() }).sum::<usize>(), a);
            if keep_temps {
                write_tempfile_locs(&tempfn, b)?;
            }
        },
        TempData::TempBlocks(bl) => {println!("have {} / {} temp blocks", bl.len(), bl.iter().map(|(_,p)| { p.len() }).sum::<usize>()); },
        TempData::TempFileSplit(parts) => {
            println!("wrote {} files / {} blocks to {}-part-?.pbf", parts.len(), parts.iter().map(|(_,_,p)| p).flatten().map(|(_,p)| { p.len() }).sum::<usize>(), &tempfn);
            if keep_temps {
                write_tempfilesplit_locs(&tempfn, parts)?;
            }
        },
    }
    
    let wf = make_write_file(outfn, bbox, 8000, numchan);
    
    let res = if numchan == 0 {
        read_temp_data(temps, Box::new(CallAll::new(wf, "unpack temp", Box::new(collect_blocks))), !keep_temps)?
    } else {
        
        let mut ccs: Vec<Box<dyn CallFinish<CallType=(i64,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for wf in CallbackSync::new(wf, numchan) {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            ccs.push(Box::new(Callback::new(Box::new(CallAll::new(wf2, "unpack temp", Box::new(collect_blocks))))));
        }
    
        read_temp_data(temps, Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new()))), !keep_temps)?
    };
    println!("{}",res);
    
    tx.add("write final");
    
    println!("{}", tx);
    
    Ok(())
    
}
pub fn run_mergechanges_from_existing(outfn: &str, tempfn: &str, is_split: bool, numchan: usize) -> Result<()> {
    let mut tx=LogTimes::new();
    let bbox = Bbox::planet();
    
    let temps = if is_split {
        read_tempfilesplit_locs(tempfn)?
    } else {
        read_tempfile_locs(tempfn)?
    };
    
    tx.add("load filelocs");
    
    let wf = make_write_file(outfn, bbox, 8000, numchan);
    
    let res = if numchan == 0 {
        read_temp_data(temps, Box::new(CallAll::new(wf, "unpack temp", Box::new(collect_blocks))),false)?
    } else {
        
        let mut ccs: Vec<Box<dyn CallFinish<CallType=(i64,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for wf in CallbackSync::new(wf, numchan) {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            ccs.push(Box::new(Callback::new(Box::new(CallAll::new(wf2, "unpack temp", Box::new(collect_blocks))))));
        }
    
        read_temp_data(temps, Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new()))),false)?
    };
    println!("{}",res);
    
    tx.add("write final");
    
    println!("{}", tx);
    
    Ok(())
    
}