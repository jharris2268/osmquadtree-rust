use std::fs::File;
use std::io;
use std::io::Write;
use std::sync::Arc;

use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings,ReplaceNoneWithTimings};
use crate::elements::PrimitiveBlock;

use crate::pbfformat::HeaderType;
use crate::pbfformat::{read_all_blocks_with_progbar, FileBlock, CompressionType};
use crate::sortblocks::addquadtree::{make_unpackprimblock, AddQuadtree};
use crate::sortblocks::writepbf::{make_packprimblock_qtindex, WriteFile};
use crate::sortblocks::{OtherData, QuadtreeTree, Timings};

use crate::utils::{LogTimes, Timer};
use crate::message;
use crate::sortblocks::sortblocks::SortBlocks;

struct CollectBlocks {
    sb: SortBlocks<PrimitiveBlock>,
    tm: f64,
}
impl CollectBlocks {
    pub fn new(groups: Arc<QuadtreeTree>) -> CollectBlocks {
        CollectBlocks {
            sb: SortBlocks::new(groups),
            tm: 0.0,
        }
    }
}
impl CallFinish for CollectBlocks {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: PrimitiveBlock) {
        let tx = Timer::new();
        self.sb.add_all(bl.into_iter()).expect("!");
        self.tm += tx.since();
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut t = Timings::new();
        t.add("find blocks", self.tm);
        let bv = self.sb.finish();

        //t.add_other("groups", OtherData::QuadtreeTree(groups.unwrap()));
        t.add_other("blocks", OtherData::AllBlocks(bv));

        Ok(t)
    }
}

fn get_blocks(
    infn: &str,
    qtsfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
) -> io::Result<Vec<PrimitiveBlock>> {
    let pp: Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>> = if numchan
        == 0
    {
        let cc = Box::new(CollectBlocks::new(groups));
        let aq = Box::new(AddQuadtree::new(qtsfn, cc));
        make_unpackprimblock(aq)
    } else {
        let cc = Box::new(Callback::new(Box::new(CollectBlocks::new(groups))));
        let aqs = CallbackSync::new(Box::new(AddQuadtree::new(qtsfn, cc)), numchan);
        let mut pps: Vec<Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>> =
            Vec::new();
        for aq in aqs {
            let aq2 = Box::new(ReplaceNoneWithTimings::new(aq));
            pps.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
        }

        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
    };
    let (mut res, d) = read_all_blocks_with_progbar(infn, pp, "get_blocks");
    let mut blocks: Option<Vec<PrimitiveBlock>> = None;

    for o in std::mem::take(&mut res.others) {
        match o {
            (_, OtherData::AllBlocks(l)) => {
                blocks = Some(l);
            }
            _ => {}
        }
    }
    message!(
        "\n{:8.3}s Total, {} [{} blocks]",
        d,
        res,
        blocks.as_ref().unwrap().len()
    );

    Ok(blocks.unwrap())
}


pub fn write_blocks(
    outfn: &str,
    blocks: Vec<PrimitiveBlock>,
    numchan: usize,
    timestamp: i64,
    compression_type: CompressionType,
) -> io::Result<()> {
    let wf = Box::new(WriteFile::with_compression_type(&outfn, HeaderType::ExternalLocs, None, compression_type));

    

    let mut wq: Box<dyn CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>> =
        if numchan == 0 {
            make_packprimblock_qtindex(wf, true, compression_type)
        } else {
            let wfs = CallbackSync::new(wf, numchan);
            let mut wqs: Vec<Box<dyn CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>>> =
                Vec::new();
            for w in wfs {
                let w2 = Box::new(ReplaceNoneWithTimings::new(w));
                wqs.push(Box::new(Callback::new(make_packprimblock_qtindex(
                    w2, true, compression_type
                ))));
            }
            Box::new(CallbackMerge::new(wqs, Box::new(MergeTimings::new())))
        };

    for mut b in blocks {
        b.end_date = timestamp;
        wq.call(b);
    }
    message!("{}", wq.finish()?);
    Ok(())
}

pub fn sort_blocks_inmem(
    infn: &str,
    qtsfn: &str,
    outfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
    timestamp: i64,
    compression_type: CompressionType,
    lt: &mut LogTimes,
) -> io::Result<()> {
    let groupsfn = format!("{}-groups.txt", outfn);
    let outf = File::create(&groupsfn)?;
    for (_, g) in groups.iter() {
        writeln!(&outf, "{};{};{}", g.qt, g.weight, g.total)?;
    }

    message!(
        "call get_blocks({}, {}, {}, {})",
        infn, qtsfn, groups, numchan
    );
    let blocks = get_blocks(infn, qtsfn, groups, numchan)?;
    lt.add("read data");
    message!(
        "call write_blocks({}, {}, {}, {})",
        outfn,
        blocks.len(),
        numchan,
        timestamp
    );
    //Err(io::Error::new(io::ErrorKind::Other,"not impl"))
    write_blocks(outfn, blocks, numchan, timestamp, compression_type)?;
    lt.add("write blocks");
    Ok(())
}
