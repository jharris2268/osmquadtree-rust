use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings, ReplaceNoneWithTimings, CallAll};
use crate::elements::{
    Bbox, Block, IdSet, IdSetAll, Node, PrimitiveBlock, Quadtree, Relation, Way, WithId,
};
use crate::mergechanges::filter_elements::prep_bbox_filter;
use crate::mergechanges::{make_write_file, read_filter};
use crate::pbfformat::make_read_primitive_blocks_combine_call_all_idset;
use crate::pbfformat::HeaderType;
use crate::pbfformat::{read_all_blocks_parallel_with_progbar, FileBlock};
use crate::sortblocks::{make_packprimblock_many, make_packprimblock_qtindex};
use crate::sortblocks::{
    read_temp_data, read_tempfile_locs, read_tempfilesplit_locs, write_tempfile_locs,
    write_tempfilesplit_locs, WriteTempData, WriteTempFile, WriteTempFileSplit, WriteTempNull
};
use crate::sortblocks::{OtherData, TempData, Timings, WriteFile};
use crate::update::{get_file_locs, ParallelFileLocs};
use crate::utils::{
    parse_timestamp, LogTimes, ThreadTimer,
};
use crate::message;

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

struct CollectObj<T: WithId> {
    split: i64,
    currs: BTreeMap<i64, Vec<T>>,
    limit: usize,
    max_key: i64,
}

impl<T> CollectObj<T>
where
    T: WithId,
{
    pub fn new(split: i64, limit: usize, max_key: i64) -> CollectObj<T> {
        CollectObj {
            split: split,
            currs: BTreeMap::new(),
            limit: limit,
            max_key: max_key,
        }
    }

    pub fn add(&mut self, o: T) -> Option<(i64, Vec<T>)> {
        let k = i64::min(o.get_id() / self.split, self.max_key);
        match self.currs.get_mut(&k) {
            None => {
                let mut v = Vec::with_capacity(self.limit);
                v.push(o);
                self.currs.insert(k, v);
                None
            }
            Some(cc) => {
                cc.push(o);
                if self.limit > 0 {
                    match cc.len() >= self.limit {
                        false => None,
                        true => Some((
                            k,
                            std::mem::replace(&mut *cc, Vec::with_capacity(self.limit)),
                        )),
                    }
                } else {
                    None
                }
            }
        }
    }

    pub fn get_all(&mut self) -> BTreeMap<i64, Vec<T>> {
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
    tm: f64,
}

const MAX_NODE_ID: i64 = 16 << 30;
const MAX_WAY_ID: i64 = 2 << 30;

impl<T> CollectTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, PrimitiveBlock)>, ReturnType = Timings>,
{
    pub fn new(
        out: Box<T>,
        limit: usize,
        splitat: (i64, i64, i64),
        write_at: usize,
    ) -> CollectTemp<T> {
        let way_off = MAX_NODE_ID / splitat.0;
        let rel_off = way_off + MAX_WAY_ID / splitat.1;

        CollectTemp {
            out: out,
            curr_node: CollectObj::new(splitat.0, limit, MAX_NODE_ID / splitat.0 - 1),
            curr_way: CollectObj::new(splitat.1, limit, MAX_WAY_ID / splitat.1 - 1),
            curr_relation: CollectObj::new(splitat.2, limit, 1024),
            way_off: way_off,
            rel_off: rel_off,
            count: 0,
            write_at: write_at,
            tm: 0.0,
        }
    }

    fn add_node(&mut self, n: Node) -> Option<PrimitiveBlock> {
        match self.curr_node.add(n) {
            None => None,
            Some((q, nn)) => {
                let mut pb = PrimitiveBlock::new(q, 0);
                pb.quadtree = Quadtree::new(q);
                pb.nodes.extend(nn);
                Some(pb)
            }
        }
    }
    fn add_way(&mut self, w: Way) -> Option<PrimitiveBlock> {
        match self.curr_way.add(w) {
            None => None,
            Some((q, ww)) => {
                let mut pb = PrimitiveBlock::new(q + self.way_off, 0);
                pb.quadtree = Quadtree::new(q + self.way_off);
                pb.ways.extend(ww);
                Some(pb)
            }
        }
    }
    fn add_relation(&mut self, r: Relation) -> Option<PrimitiveBlock> {
        match self.curr_relation.add(r) {
            None => None,
            Some((q, rr)) => {
                let mut pb = PrimitiveBlock::new(q + self.rel_off, 0);
                pb.quadtree = Quadtree::new(q + self.rel_off);
                pb.relations.extend(rr);
                Some(pb)
            }
        }
    }

    fn add_all(&mut self, pb: PrimitiveBlock) -> Vec<(i64, PrimitiveBlock)> {
        let mut res = Vec::new();
        for n in pb.nodes {
            match self.add_node(n) {
                None => {}
                Some(pb) => {
                    res.push((pb.index, pb));
                }
            }
            self.count += 1;
        }
        for w in pb.ways {
            match self.add_way(w) {
                None => {}
                Some(pb) => {
                    res.push((pb.index, pb));
                }
            }
            self.count += 5;
        }
        for r in pb.relations {
            match self.add_relation(r) {
                None => {}
                Some(pb) => {
                    res.push((pb.index, pb));
                }
            }
            self.count += 10;
        }

        if self.write_at > 0 && self.count > self.write_at {
            res.extend(self.finish_all());
        }
        res
    }

    fn finish_all(&mut self) -> Vec<(i64, PrimitiveBlock)> {
        let mut res = Vec::new();
        for (q, mut nn) in self.curr_node.get_all() {
            if nn.len() > 0 {
                let mut pb = PrimitiveBlock::new(q, 0);
                pb.quadtree = Quadtree::new(q);
                pb.nodes.append(&mut nn);
                pb.sort();
                res.push((pb.index, pb));
            }
        }
        for (q, mut ww) in self.curr_way.get_all() {
            if ww.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + self.way_off, 0);
                pb.quadtree = Quadtree::new(q + self.way_off);
                pb.ways.append(&mut ww);
                pb.sort();
                res.push((pb.index, pb));
            }
        }
        for (q, mut rr) in self.curr_relation.get_all() {
            if rr.len() > 0 {
                let mut pb = PrimitiveBlock::new(q + self.rel_off, 0);
                pb.quadtree = Quadtree::new(q + self.rel_off);
                pb.relations.append(&mut rr);
                pb.sort();
                res.push((pb.index, pb));
            }
        }
        self.count = 0;
        res
    }
}

impl<T> CallFinish for CollectTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, PrimitiveBlock)>, ReturnType = Timings>,
{
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, pb: PrimitiveBlock) {
        let tx = ThreadTimer::new();
        let res = self.add_all(pb);
        self.tm += tx.since();
        self.out.call(res);
    }

    fn finish(&mut self) -> Result<Timings> {
        let tx = ThreadTimer::new();
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
    splitat: (i64, i64, i64),
    fsplit: i64,
    numchan: usize,
) -> Result<TempData> {
    let wt: Box<dyn CallFinish< CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>> = 
        if tempfn == "NONE" {
            Box::new(WriteTempData::new())
        } else if tempfn == "NULL" {
            Box::new(WriteTempNull::new())
        } else {
            if fsplit == 0 {
                Box::new(WriteTempFile::new(tempfn.clone()))
            } else {
                Box::new(WriteTempFileSplit::new(
                    tempfn.clone(),
                    fsplit,
                ))
            }
        };

    /*let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!(
        "write_temp_blocks to {}, numchan={}",
        tempfn, numchan
    ));*/

    let pp: Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>> =
        if numchan == 0 {
            //let (mut res, d) = if numchan == 0 {
            let pc = make_packprimblock_many(wt, true);
            let cc = Box::new(CollectTemp::new(pc, 0, splitat, write_at));
            /*let pp =*/
            make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true)

        //read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog)
        } else {
            let wts = CallbackSync::new(wt, numchan);
            let mut pcs: Vec<
                Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>,
            > = Vec::new();
            for wt in wts {
                let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
                let pc = make_packprimblock_many(wt2, true);
                let cc = Box::new(CollectTemp::new(pc, 0, splitat, write_at / numchan));
                pcs.push(Box::new(Callback::new(
                    make_read_primitive_blocks_combine_call_all_idset(cc, ids.clone(), true),
                )));
            }
            Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())))
            //read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pc, &prog)
        };
    //prog.finish();
    //message!("write_temp_blocks {} {}", res, d);

    let msg = format!("write_temp_blocks to {}, numchan={}", tempfn, numchan);
    let mut res = read_all_blocks_parallel_with_progbar(
        &mut pfilelocs.0,
        &pfilelocs.1,
        pp,
        &msg,
        pfilelocs.2,
    );

    for (_, b) in std::mem::take(&mut res.others) {
        match b {
            OtherData::TempData(td) => {
                return Ok(td);
            }
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

pub fn run_mergechanges_sort(
    inprfx: &str,
    outfn: &str,
    tempfn: Option<&str>,
    filter: Option<&str>,
    filter_objs: bool,
    timestamp: Option<&str>,
    keep_temps: bool,
    numchan: usize,
    ram_gb: usize,
) -> Result<()> {
    let mut tx = LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;

    message!("bbox={}, poly={:?}", bbox, poly);

    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?),
    };

    let mut pfilelocs = get_file_locs(inprfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");

    let ids: Arc<dyn IdSet> = match filter {
        Some(_) => {
            if filter_objs {
                let ids = prep_bbox_filter(&mut pfilelocs, &bbox, &poly, numchan)?;
                tx.add("prep_bbox_filter");
                message!("have: {}", ids);
                Arc::from(ids)
            } else {
                Arc::new(IdSetAll())
            }
        }
        None => Arc::new(IdSetAll()),
    };

    let tempfn = match tempfn {
        Some(t) => t.to_string(),
        None => {
            format!("{}-temp.pbf", &outfn[0..outfn.len() - 4])
        }
    };

    let mut limit = 200000 * ram_gb;
    if tempfn == "NONE" || tempfn == "NULL" {
        limit = 200;
    }
    let fsplit = if filter.is_none() || pfilelocs.2 > 4 * 1024 * 1024 * 1024 {
        128
    } else {
        0
    };

    call_mergechanges_sort(&mut pfilelocs, outfn, &tempfn, limit, fsplit, ids, &bbox, keep_temps, tx, numchan, ram_gb)
}

pub fn call_mergechanges_sort(
    pfilelocs: &mut ParallelFileLocs,
    outfn: &str,
    tempfn: &str,
    limit: usize,
    fsplit: i64,
    ids: Arc<dyn IdSet>,
    bbox: &Bbox,
    keep_temps: bool,
    mut tx: LogTimes,
    numchan: usize,
    _ram_gb: usize,
) -> Result<()> {


    let temps = write_temp_blocks(
        pfilelocs,
        ids.clone(),
        tempfn,
        limit,
        (1i64 << 21, 1i64 << 18, 1i64 << 17),
        fsplit,
        numchan,
    )?;
    tx.add("write_temp_blocks");
    match &temps {
        TempData::Null => {
            message!("TempData::Null");
            
        },
        TempData::TempFile((a, b)) => {
            message!(
                "wrote {} / {} blocks to {}",
                b.len(),
                b.iter().map(|(_, p)| { p.len() }).sum::<usize>(),
                a
            );
            if keep_temps {
                write_tempfile_locs(&tempfn, b)?;
            }
        }
        TempData::TempBlocks(bl) => {
            message!(
                "have {} / {} temp blocks",
                bl.len(),
                bl.iter().map(|(_, p)| { p.len() }).sum::<usize>()
            );
        }
        TempData::TempFileSplit(parts) => {
            message!(
                "wrote {} files / {} blocks to {}-part-?.pbf",
                parts.len(),
                parts
                    .iter()
                    .map(|(_, _, p)| p)
                    .flatten()
                    .map(|(_, p)| { p.len() })
                    .sum::<usize>(),
                &tempfn
            );
            if keep_temps {
                write_tempfilesplit_locs(&tempfn, parts)?;
            }
        }
    }

    let wf = make_write_file(outfn, bbox, 8000, numchan);

    let res = if numchan == 0 {
        read_temp_data(
            temps,
            Box::new(CallAll::new(wf, "unpack temp", Box::new(collect_blocks))),
            !keep_temps,
        )?
    } else {
        let mut ccs: Vec<
            Box<dyn CallFinish<CallType = (i64, Vec<FileBlock>), ReturnType = Timings>>,
        > = Vec::new();
        for wf in CallbackSync::new(wf, numchan) {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            ccs.push(Box::new(Callback::new(Box::new(CallAll::new(
                wf2,
                "unpack temp",
                Box::new(collect_blocks),
            )))));
        }

        read_temp_data(
            temps,
            Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new()))),
            !keep_temps,
        )?
    };
    message!("{}", res);

    tx.add("write final");

    message!("{}", tx);

    Ok(())
}
pub fn run_mergechanges_sort_from_existing(
    outfn: &str,
    tempfn: &str,
    is_split: bool,
    numchan: usize,
) -> Result<()> {
    let mut tx = LogTimes::new();
    let bbox = Bbox::planet();

    let temps = if is_split {
        read_tempfilesplit_locs(tempfn)?
    } else {
        read_tempfile_locs(tempfn)?
    };

    tx.add("load filelocs");

    let wf = make_write_file(outfn, &bbox, 8000, numchan);

    let res = if numchan == 0 {
        read_temp_data(
            temps,
            Box::new(CallAll::new(wf, "unpack temp", Box::new(collect_blocks))),
            false,
        )?
    } else {
        let mut ccs: Vec<
            Box<dyn CallFinish<CallType = (i64, Vec<FileBlock>), ReturnType = Timings>>,
        > = Vec::new();
        for wf in CallbackSync::new(wf, numchan) {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            ccs.push(Box::new(Callback::new(Box::new(CallAll::new(
                wf2,
                "unpack temp",
                Box::new(collect_blocks),
            )))));
        }

        read_temp_data(
            temps,
            Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new()))),
            false,
        )?
    };
    message!("{}", res);

    tx.add("write final");

    message!("{}", tx);

    Ok(())
}

pub fn run_mergechanges(
    inprfx: &str,
    outfn: &str,
    filter: Option<&str>,
    filter_objs: bool,
    timestamp: Option<&str>,
    numchan: usize,
) -> Result<()> {
    let mut tx = LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;

    message!("bbox={}, poly={:?}", bbox, poly);

    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?),
    };

    let mut pfilelocs = get_file_locs(inprfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");

    let ids: Arc<dyn IdSet> = match (filter_objs, filter) {
        (true, Some(_)) => {
            let ids = prep_bbox_filter(&mut pfilelocs, &bbox, &poly, numchan)?;
            tx.add("prep_bbox_filter");
            message!("have: {}", ids);
            Arc::from(ids)
        }
        _ => Arc::new(IdSetAll()),
    };
    
    call_mergechanges(&mut pfilelocs, outfn, ids, &bbox, tx, numchan)
}

pub fn call_mergechanges(
    pfilelocs: &mut ParallelFileLocs,
    outfn: &str,
    ids: Arc<dyn IdSet>,
    bbox: &Bbox,
    mut tx: LogTimes,
    numchan: usize) -> Result<()> {
    
    let wf = Box::new(WriteFile::with_bbox(&outfn, HeaderType::ExternalLocs, Some(bbox)));

    let pp: Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>> =
        if numchan == 0 {
            let pc = make_packprimblock_qtindex(wf, true);
            make_read_primitive_blocks_combine_call_all_idset(pc, ids.clone(), true)
        } else {
            let wfs = CallbackSync::new(wf, numchan);
            let mut pps: Vec<
                Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>,
            > = Vec::new();
            for w in wfs {
                let w2 = Box::new(ReplaceNoneWithTimings::new(w));
                let pc = make_packprimblock_qtindex(w2, true);
                pps.push(Box::new(Callback::new(
                    make_read_primitive_blocks_combine_call_all_idset(pc, ids.clone(), true),
                )))
            }
            Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
        };

    let msg = format!("write merged blocks, numchan={}", numchan);
    let tm = read_all_blocks_parallel_with_progbar(
        &mut pfilelocs.0,
        &pfilelocs.1,
        pp,
        &msg,
        pfilelocs.2,
    );

    /*let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&);

    let (tm,_) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog);
    prog.finish();*/
    tx.add("write merged blocks");

    message!("{}\n{}", tm, tx);
    Ok(())
}
