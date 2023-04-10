use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, CallAll,MergeTimings, ReplaceNoneWithTimings};
use crate::elements::{ElementType, IdSet, MinimalBlock, Quadtree};
use crate::pbfformat::{
    file_length, pack_file_block, read_all_blocks, read_all_blocks_prog,
    read_all_blocks_with_progbar, FileBlock,CompressionType
};
use crate::logging::ProgressPercent;
use simple_protocolbuffers::{
    pack_data, pack_delta_int, pack_value, un_zig_zag, zig_zag, DeltaPackedInt, IterTags, PbfTag,
};

use crate::utils::ThreadTimer;

pub enum ResultType {
    NumTiles(usize),
    CheckIndexResult(Vec<Quadtree>),
}

type Timings = channelled_callbacks::Timings<ResultType>;
type CallFinishFileBlocks =
    Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>;

use std::fs::File;
use std::io::{BufReader /*Error,ErrorKind*/, Result, Write};
use std::sync::Arc;

fn prep_index_block(mb: &MinimalBlock) -> Vec<u8> {
    let mut res = Vec::with_capacity(20 + 5 * mb.len());

    pack_value(&mut res, 1, zig_zag(mb.quadtree.as_int()));
    if !mb.nodes.is_empty() {
        pack_data(&mut res, 2, &pack_delta_int(mb.nodes.iter().map(|n| n.id)));
    }
    if !mb.ways.is_empty() {
        pack_data(&mut res, 3, &pack_delta_int(mb.ways.iter().map(|w| w.id)));
    }
    if !mb.relations.is_empty() {
        pack_data(
            &mut res,
            4,
            &pack_delta_int(mb.relations.iter().map(|r| r.id)),
        );
    }

    res.shrink_to_fit();

    res
}

fn check_index_block(bl: Vec<u8>, idset: &dyn IdSet) -> Option<Quadtree> {
    let mut qt = Quadtree::new(-2);
    for tg in IterTags::new(&bl) {
        match tg {
            PbfTag::Value(1, q) => qt = Quadtree::new(un_zig_zag(q)),
            PbfTag::Data(2, nn) => {
                for n in DeltaPackedInt::new(&nn) {
                    if idset.contains(ElementType::Node, n) {
                        return Some(qt);
                    }
                }
            }
            PbfTag::Data(3, ww) => {
                for w in DeltaPackedInt::new(&ww) {
                    if idset.contains(ElementType::Way, w) {
                        return Some(qt);
                    }
                }
            }
            PbfTag::Data(4, rr) => {
                for r in DeltaPackedInt::new(&rr) {
                    if idset.contains(ElementType::Relation, r) {
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
    tm: f64,
}

impl WF {
    pub fn new(outfn: &str) -> WF {
        WF {
            f: File::create(outfn).expect("failed to create file"),
            nt: 0,
            tm: 0.0,
        }
    }
}

impl CallFinish for WF {
    type CallType = Vec<u8>;
    type ReturnType = Timings;

    fn call(&mut self, d: Vec<u8>) {
        if d.is_empty() {
            return;
        }
        let tx = ThreadTimer::new();
        self.f.write_all(&d).expect("failed to write data");
        self.tm += tx.since();
        self.nt += 1;
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tms = Timings::new();
        tms.add("write", self.tm);
        tms.add_other("num_tiles", ResultType::NumTiles(self.nt));
        Ok(tms)
    }
}

fn convert_indexblock(i_fb: (usize, FileBlock)) -> Vec<u8> {
    if i_fb.1.block_type != "OSMData" {
        return Vec::new();
    }

    let mb = MinimalBlock::read(i_fb.0 as i64, i_fb.1.pos, &i_fb.1.data(), false)
        .expect("MinimalBlock::read failed");
    let d = prep_index_block(&mb);
    pack_file_block("IndexBlock", &d, &CompressionType::Zlib).expect("pack_file_block failed")
}

pub fn write_index_file(infn: &str, outfn: &str, numchan: usize) -> usize {
    let pack: CallFinishFileBlocks = if numchan == 0 {
        let wf = Box::new(WF::new(outfn));
        Box::new(CallAll::new(wf, "convert", Box::new(convert_indexblock)))
    } else {
        let wfs = CallbackSync::new(Box::new(WF::new(outfn)), numchan);

        let mut packs: Vec<
            Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>,
        > = Vec::new();

        for wf in wfs {
            let wf2 = Box::new(ReplaceNoneWithTimings::new(wf));
            packs.push(Box::new(Callback::new(Box::new(CallAll::new(
                wf2,
                "convert",
                Box::new(convert_indexblock),
            )))));
        }
        Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new())))
    };
    let (mut tm, _) =
        read_all_blocks_with_progbar(infn, pack, &format!("write_index_file for {}", infn));

    match tm.others.pop().unwrap().1 {
        ResultType::NumTiles(nt) => {
            return nt;
        }
        _ => {
            panic!("??");
        }
    }
}

struct CheckIndexFile {
    idset: Arc<dyn IdSet>,
    quadtrees: Vec<Quadtree>,
    tm: f64,
}

impl CheckIndexFile {
    pub fn new(idset: Arc<dyn IdSet>) -> CheckIndexFile {
        CheckIndexFile {
            idset: idset,
            quadtrees: Vec::new(),
            tm: 0.0,
        }
    }
}

impl CallFinish for CheckIndexFile {
    type CallType = Vec<u8>;
    type ReturnType = Timings;

    fn call(&mut self, bl: Vec<u8>) {
        let tx = ThreadTimer::new();
        match check_index_block(bl, self.idset.as_ref()) {
            Some(q) => {
                self.quadtrees.push(q);
            }
            None => {}
        }
        self.tm += tx.since();
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut t = Timings::new();
        t.add("check index", self.tm);

        let qts = std::mem::take(&mut self.quadtrees);
        t.add_other("result", ResultType::CheckIndexResult(qts));
        Ok(t)
    }
}

fn unpack_fb(i_fb: (usize, FileBlock)) -> Vec<u8> {
    if i_fb.1.block_type != "IndexBlock" {
        return Vec::new();
    }
    i_fb.1.data()
}

pub fn check_index_file(
    indexfn: &str,
    idset: Arc<dyn IdSet>,
    numchan: usize,
    pb: Option<(&Box<dyn ProgressPercent>,f64,f64)>
) -> Result<(Vec<Quadtree>, f64)> {
    let ca: CallFinishFileBlocks = if numchan == 0 {
        let ci = Box::new(CheckIndexFile::new(idset));

        Box::new(CallAll::new(ci, "unpack", Box::new(unpack_fb)))
    } else {
        let mut cas: Vec<CallFinishFileBlocks> = Vec::new();
        for _ in 0..numchan {
            let ci = Box::new(CheckIndexFile::new(idset.clone()));
            let ca = Box::new(CallAll::new(ci, "unpack", Box::new(unpack_fb)));
            cas.push(Box::new(Callback::new(ca)));
        }
        Box::new(CallbackMerge::new(cas, Box::new(MergeTimings::new())))
    };

    let (tm, x) = match pb {
        None => read_all_blocks(indexfn, ca),
        Some((pb,start_pc,end_pc)) => {
            let flen = file_length(indexfn);
            let f = File::open(indexfn).expect("fail");
            let mut fbuf = BufReader::new(f);
            read_all_blocks_prog(&mut fbuf, flen, ca, pb, start_pc,end_pc)
        }
    };

    let mut qq = Vec::new();
    for (_, x) in tm.others {
        match x {
            ResultType::CheckIndexResult(q) => {
                qq.extend(q);
            }
            _ => {}
        }
    }
    Ok((qq, x))
}
