use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings, ReplaceNoneWithTimings};
use crate::elements::{Bbox, Block, IdSet, IdSetAll, PrimitiveBlock};
use crate::mergechanges::filter_elements::{prep_bbox_filter, read_filter};
use crate::pbfformat::make_read_primitive_blocks_combine_call_all_idset;
use crate::pbfformat::HeaderType;
use crate::pbfformat::{read_all_blocks_parallel_prog, FileBlock, CompressionType};
use crate::sortblocks::{make_packprimblock_zeroindex, WriteFile};
use crate::pbfformat::{get_file_locs, ParallelFileLocs};
use crate::utils::{parse_timestamp, LogTimes, ThreadTimer};
use crate::{message,progress_percent};
use std::io::{Result,Error,ErrorKind};
use std::sync::Arc;

type Timings = channelled_callbacks::Timings<PrimitiveBlock>;

struct CollectObjs {
    collected: Option<PrimitiveBlock>,
    tm: f64,
}

impl CollectObjs {
    pub fn new() -> CollectObjs {
        CollectObjs {
            collected: Some(PrimitiveBlock::new(0, 0)),
            tm: 0.0,
        }
    }
}

impl CallFinish for CollectObjs {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: PrimitiveBlock) {
        let tx = ThreadTimer::new();
        self.collected.as_mut().unwrap().nodes.extend(bl.nodes);
        self.collected.as_mut().unwrap().ways.extend(bl.ways);
        self.collected
            .as_mut()
            .unwrap()
            .relations
            .extend(bl.relations);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add("CollectObjs::call", self.tm);
        let tx = ThreadTimer::new();
        let mut bl = std::mem::take(&mut self.collected).unwrap();
        bl.sort();

        tm.add("CollectedObjs::finish", tx.since());
        tm.add_other("objs", bl);
        Ok(tm)
    }
}

pub fn collect_blocks_filtered(
    pfilelocs: &mut ParallelFileLocs,
    ids: Arc<dyn IdSet>,
    numchan: usize,
) -> Result<PrimitiveBlock> {
    let pb = progress_percent!("merge blocks");

    let conv: Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>> =
        if numchan == 0 {
            let co = Box::new(CollectObjs::new());
            make_read_primitive_blocks_combine_call_all_idset(co, ids.clone(), true)
        } else {
            let mut convs: Vec<
                Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>,
            > = Vec::new();
            for _ in 0..numchan {
                let co = Box::new(CollectObjs::new());
                convs.push(Box::new(Callback::new(
                    make_read_primitive_blocks_combine_call_all_idset(co, ids.clone(), true),
                )));
            }
            Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
        };

    let (tm, _) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, conv, pb);
    //pb.finish();

    let mut res = PrimitiveBlock::new(0, 0);
    for (_, x) in tm.others {
        res.nodes.extend(x.nodes);
        res.ways.extend(x.ways);
        res.relations.extend(x.relations);
    }
    res.sort();

    Ok(res)
}

struct GroupBlocks<T: ?Sized> {
    out: Box<T>,
    block_size: usize,
    curr: PrimitiveBlock,
    tm: f64,
}

impl<T> GroupBlocks<T>
where
    T: CallFinish<CallType = PrimitiveBlock, ReturnType = crate::sortblocks::Timings> + ?Sized,
{
    pub fn new(out: Box<T>, block_size: usize) -> GroupBlocks<T> {
        GroupBlocks {
            out: out,
            block_size: block_size,
            curr: PrimitiveBlock::new(0, 0),
            tm: 0.0,
        }
    }
}

impl<T> CallFinish for GroupBlocks<T>
where
    T: CallFinish<CallType = PrimitiveBlock, ReturnType = crate::sortblocks::Timings> + ?Sized,
{
    type CallType = PrimitiveBlock;
    type ReturnType = crate::sortblocks::Timings;

    fn call(&mut self, pb: PrimitiveBlock) {
        let tx = ThreadTimer::new();
        for n in pb.nodes {
            self.curr.nodes.push(n);
            if self.curr.len() >= self.block_size {
                self.out
                    .call(std::mem::replace(&mut self.curr, PrimitiveBlock::new(0, 0)));
            }
        }

        for w in pb.ways {
            self.curr.ways.push(w);
            if self.curr.len() >= self.block_size {
                self.out
                    .call(std::mem::replace(&mut self.curr, PrimitiveBlock::new(0, 0)));
            }
        }

        for r in pb.relations {
            self.curr.relations.push(r);
            if self.curr.len() >= self.block_size {
                self.out
                    .call(std::mem::replace(&mut self.curr, PrimitiveBlock::new(0, 0)));
            }
        }
        self.tm += tx.since();
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let tx = ThreadTimer::new();
        if self.curr.len() > 0 {
            self.out
                .call(std::mem::replace(&mut self.curr, PrimitiveBlock::new(0, 0)));
        }
        self.tm += tx.since();
        //message!("filtered {} rels", nr);
        let mut tms = self.out.finish()?;
        tms.add("GroupTiles", self.tm);
        Ok(tms)
    }
}



pub fn make_write_file(
    outfn: &str,
    bbox: &Bbox,
    block_size: usize,
    compression_type: CompressionType,
    numchan: usize,
) -> Box<impl CallFinish<CallType = PrimitiveBlock, ReturnType = crate::sortblocks::Timings>> {
    let wf = Box::new(WriteFile::with_compression_type(
        outfn, HeaderType::NoLocs, Some(bbox), compression_type));

    let pack: Box<
        dyn CallFinish<CallType = PrimitiveBlock, ReturnType = crate::sortblocks::Timings>,
    > = if numchan == 0 {
        make_packprimblock_zeroindex(wf, false, compression_type)
    } else {
        let wff = CallbackSync::new(wf, 4);
        let mut packs: Vec<
            Box<dyn CallFinish<CallType = PrimitiveBlock, ReturnType = crate::sortblocks::Timings>>,
        > = Vec::new();
        for w in wff {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            packs.push(Box::new(Callback::new(make_packprimblock_zeroindex(
                w2, false, compression_type.clone()
            ))));
        }

        Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new())))
    };
    Box::new(GroupBlocks::new(pack, block_size))
}



pub fn run_mergechanges_sort_inmem(
    inprfx: &str,
    outfn: &str,
    filter: Option<&str>,
    filterobjs: bool,
    timestamp: Option<&str>,
    numchan: usize,
    ram_gb: usize,
    compression_type: CompressionType
) -> Result<()> {
    let mut tx = LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;
    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?),
    };

    let mut pfilelocs = get_file_locs(inprfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");
    
    if pfilelocs.2 > (ram_gb as u64)*32*1024*1024 {
        return Err(Error::new(ErrorKind::Other, format!(
            "extract too big to merge in memory ({:0.1}mb > {:0.1}mb)",
            (pfilelocs.2 as f64) / 1024.0 / 1024.0,
            (ram_gb as f64) * 32.0
        )));
    }
            
    
    let ids: Arc<dyn IdSet> = 
        if filterobjs {
            match filter {
                Some(_) => {
                    let ids = prep_bbox_filter(&mut pfilelocs, &bbox, &poly, numchan)?;
                    tx.add("prep_bbox_filter");
                    message!("have: {}", ids);
                    Arc::from(ids)
                }
                None => {
                    panic!("must have a filter");
                }
            }
        } else {
            
            Arc::new(IdSetAll())
        };
    
    call_mergechanges_sort_inmem(&mut pfilelocs, outfn, ids, &bbox, compression_type, tx, numchan)
}

pub fn call_mergechanges_sort_inmem(
    pfilelocs: &mut ParallelFileLocs,
    outfn: &str,
    ids: Arc<dyn IdSet>,
    bbox: &Bbox,
    compression_type: CompressionType,
    mut tx: LogTimes,
    numchan: usize) -> Result<()> {

    let pb = collect_blocks_filtered(pfilelocs, ids.clone(), numchan)?;
    tx.add("collect_blocks_filtered");
    message!(
        "have {} nodes, {} ways, {} relations",
        pb.nodes.len(),
        pb.ways.len(),
        pb.relations.len()
    );

    let mut gb = make_write_file(outfn, &bbox, 8000, compression_type, numchan);
    gb.call(pb);
    let tm = gb.finish()?;
    tx.add("write");
    message!("{}", tm);
    message!("{}", tx);
    Ok(())
}
