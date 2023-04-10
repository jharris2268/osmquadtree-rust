use simple_protocolbuffers::{read_delta_packed_int, DeltaPackedInt, PackedInt};

use channelled_callbacks::{CallFinish, Callback, CallbackMerge, MergeTimings};
use crate::elements::{Bbox, ElementType, MinimalBlock, Quadtree, PrimitiveBlock, WithId, WithTimestamp, SetCommon};
use crate::pbfformat::{make_convert_minimal_block_parts, make_convert_primitive_block};
use crate::pbfformat::{file_length, read_all_blocks_with_progbar};
use crate::utils::LogTimes;


use crate::calcqts::quadtree_store::{QuadtreeGetSet, QuadtreeSimple};
use crate::calcqts::write_quadtrees::{PackQuadtrees, WriteQuadTree};
use crate::calcqts::{CallFinishFileBlocks, OtherData, Timings};

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use crate::message;
pub struct CollectedData {
    pub nodes: BTreeMap<i64, (i32, i32)>,
    pub ways: BTreeMap<i64, Vec<i64>>,
    pub relations: BTreeMap<i64, Vec<(ElementType, i64)>>,
}
impl CollectedData {
    pub fn new() -> CollectedData {
        CollectedData {
            nodes: BTreeMap::new(),
            ways: BTreeMap::new(),
            relations: BTreeMap::new(),
        }
    }
    pub fn extend(&mut self, other: CollectedData) {
        self.nodes.extend(other.nodes);
        self.ways.extend(other.ways);
        self.relations.extend(other.relations);
    }
}

struct CollectTiles {
    data: Option<CollectedData>,
    max_timestamp: i64
}

impl CollectTiles {
    pub fn new() -> CollectTiles {
        CollectTiles {
            data: Some(CollectedData::new()),
            max_timestamp: 0,
        }
    }
}

impl CallFinish for CollectTiles {
    type CallType = MinimalBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: MinimalBlock) {
        let dx = self.data.as_mut().unwrap();
        for n in bl.nodes {
            dx.nodes.insert(n.id, (n.lon, n.lat));
            if n.timestamp > self.max_timestamp {
                self.max_timestamp = n.timestamp;
            }
        }
        for w in bl.ways {
            dx.ways.insert(w.id, read_delta_packed_int(&w.refs_data));
            if w.timestamp > self.max_timestamp {
                self.max_timestamp = w.timestamp;
            }
        }
        for r in bl.relations {
            let mut p = Vec::new();
            for (t, r) in PackedInt::new(&r.types_data).zip(DeltaPackedInt::new(&r.refs_data)) {
                let c = ElementType::from_int(t);
                p.push((c, r));
            }
            dx.relations.insert(r.id, p);
            if r.timestamp > self.max_timestamp {
                self.max_timestamp = r.timestamp;
            }
        }
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add_other("data", OtherData::CollectedData(self.data.take().unwrap()));
        tm.add_other("max_timestamp", OtherData::MaxTimestamp(self.max_timestamp));
        Ok(tm)
    }
}


struct CollectTilesPrimitive {
    data: Option<CollectedData>,
    blocks: Vec<PrimitiveBlock>,
    max_timestamp: i64
}

impl CollectTilesPrimitive {
    pub fn new() -> CollectTilesPrimitive {
        CollectTilesPrimitive {
            data: Some(CollectedData::new()),
            blocks: Vec::new(),
            max_timestamp: 0,
        }
    }
}

impl CallFinish for CollectTilesPrimitive {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: PrimitiveBlock) {
        let dx = self.data.as_mut().unwrap();
        for n in &bl.nodes {
            dx.nodes.insert(n.get_id(), (n.lon, n.lat));
            if n.get_timestamp() > self.max_timestamp {
                self.max_timestamp = n.get_timestamp();
            }
        }
        for w in &bl.ways {
            dx.ways.insert(w.get_id(), w.refs.clone());
            if w.get_timestamp() > self.max_timestamp {
                self.max_timestamp = w.get_timestamp();
            }
        }
        for r in &bl.relations {
            let mut p = Vec::new();
            for m in &r.members {
                p.push((m.mem_type.clone(), m.mem_ref));
            }
            dx.relations.insert(r.get_id(), p);
            if r.get_timestamp() > self.max_timestamp {
                self.max_timestamp = r.get_timestamp();
            }
        }
        self.blocks.push(bl);
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add_other("data", OtherData::CollectedData(self.data.take().unwrap()));
                
        tm.add_other("blocks", OtherData::OriginalData(std::mem::take(&mut self.blocks)));
        tm.add_other("max_timestamp", OtherData::MaxTimestamp(self.max_timestamp));
        Ok(tm)
    }
}

fn calc_collected_data_quadtrees(
    data: &CollectedData,
    lt: &mut LogTimes,
    qt_level: usize,
    qt_buffer: f64)
        -> Result<(QuadtreeSimple, QuadtreeSimple, QuadtreeSimple)>
{
    
    let mut wayqts = QuadtreeSimple::new();
    for (w, rr) in &data.ways {
        let mut bx = Bbox::empty();
        for r in rr {
            match data.nodes.get(&r) {
                Some((ln, lt)) => {
                    bx.expand(*ln, *lt);
                }
                None => {
                    return Err(Error::new(ErrorKind::Other, format!("missing node {}", r)));
                }
            }
        }

        let q = Quadtree::calculate(&bx, qt_level, qt_buffer);
        wayqts.set(*w, q);
    }
    message!("calculated {} way qts", wayqts.len());
    lt.add("calculate way qts");
    let mut nodeqts = QuadtreeSimple::new();

    for (w, rr) in &data.ways {
        let q = wayqts.get(*w).unwrap();
        for r in rr {
            nodeqts.expand(*r, q);
        }
    }
    message!("calculated {} node qts from way qts", nodeqts.len());
    
    for (n, (ln, lt)) in &data.nodes {
        if !nodeqts.has_value(*n) {
            let q = Quadtree::calculate_point(*ln, *lt, qt_level, qt_buffer);
            nodeqts.set(*n, q);
        }
    }

    message!("have {} node qts", nodeqts.len());
    lt.add("calculate node qts");
    let mut relrels = Vec::new();

    let mut relqts = QuadtreeSimple::new();

    for (r, mems) in &data.relations {
        if mems.is_empty() {
            relqts.set(*r, Quadtree::new(0));
        } else {
            for (ty, rf) in mems {
                match ty {
                    ElementType::Node => match nodeqts.get(*rf) {
                        Some(nq) => {
                            relqts.expand(*r, nq);
                        }
                        None => {}
                    },
                    ElementType::Way => match wayqts.get(*rf) {
                        Some(wq) => {
                            relqts.expand(*r, wq);
                        }
                        None => {}
                    },
                    ElementType::Relation => {
                        relrels.push((*r, *rf));
                    },
                    _ => {}
                }
            }
        }
    }
    for _ in 0..5 {
        for (a, b) in &relrels {
            match relqts.get(*b) {
                None => {}
                Some(q) => {
                    relqts.expand(*a, q);
                }
            }
        }
    }

    message!("have {} relqts", relqts.len());
    lt.add("calculate relation qts");
    
    Ok((nodeqts, wayqts, relqts))
}

fn read_minimal_data(fname: &str, numchan: usize) -> (CollectedData, i64) {
    
    let conv: CallFinishFileBlocks = if numchan == 0 {
        let cc = Box::new(CollectTiles::new());
        make_convert_minimal_block_parts(false, true, true, true, cc)
    } else {
        let mut convs: Vec<CallFinishFileBlocks> = Vec::new();
        for _ in 0..numchan {
            let c2 = Box::new(CollectTiles::new());
            convs.push(Box::new(Callback::new(make_convert_minimal_block_parts(
                false, true, true, true, c2,
            ))));
        }
        Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
    };

    let (tm, _) = read_all_blocks_with_progbar(fname, conv, "read data");

    let mut rd = CollectedData::new();
    let mut max_timestamp = 0;
    for t in tm.others {
        match t {
            (_, OtherData::CollectedData(dd)) => {
                rd.extend(dd);
            },
            (_, OtherData::MaxTimestamp(ts)) => {
                if ts > max_timestamp {
                    max_timestamp = ts;
                }
            },
            _ => {}
        }
    }
    (rd, max_timestamp)
}

fn read_primitive_data(fname: &str, numchan: usize) -> (CollectedData, Vec<PrimitiveBlock>, i64) {
    
    let conv: CallFinishFileBlocks = if numchan == 0 {
        let cc = Box::new(CollectTilesPrimitive::new());
        make_convert_primitive_block(false, cc)
    } else {
        let mut convs: Vec<CallFinishFileBlocks> = Vec::new();
        for _ in 0..numchan {
            let c2 = Box::new(CollectTilesPrimitive::new());
            convs.push(make_convert_primitive_block(false, c2));
        }
        Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
    };

    let (tm, _) = read_all_blocks_with_progbar(fname, conv, "read data");

    let mut rd = CollectedData::new();
    let mut max_timestamp = 0;
    let mut blocks = Vec::new();
    for t in tm.others {
        match t {
            (_, OtherData::CollectedData(dd)) => {
                rd.extend(dd);
            },
            (_, OtherData::OriginalData(bls)) => {
                blocks.extend(bls);
            },
            (_, OtherData::MaxTimestamp(ts)) => {
                if ts > max_timestamp {
                    max_timestamp = ts;
                }
            },
            _ => {}
        }
    }
    blocks.sort_by_key(|b| b.index);
    (rd, blocks, max_timestamp)
}

pub fn run_calcqts_inmem(
    fname: &str,
    outfn: Option<&str>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Result<(String, LogTimes,i64)> {
    
    let mut lt = LogTimes::new();
    
    let outfn_ = match outfn {
        Some(o) => String::from(o),
        None => format!("{}-qts.pbf", &fname[0..fname.len() - 4]),
    };
    let outfn = &outfn_;

    if file_length(fname) > 1024 * 1024 * 1024 {
        return Err(Error::new(
            ErrorKind::Other,
            "run_calcqts_inmem only suitable for pbf files smaller than 1gb",
        ));
    }

    let (data,max_timestamp) = read_minimal_data(fname, numchan);
    
    lt.add("read data");
    message!(
        "have {} nodes, {} ways, {} relations",
        data.nodes.len(),
        data.ways.len(),
        data.relations.len()
    );
    
    let (nodeqts,wayqts, relqts) = calc_collected_data_quadtrees(&data, &mut lt, qt_level, qt_buffer)?;
    
    let writeqts = Box::new(WriteQuadTree::new(outfn));

    let mut allqts = PackQuadtrees::new(writeqts, 8000);
    for (n, q) in nodeqts.items() {
        allqts.add_node(n, q);
    }
    for (w, q) in wayqts.items() {
        allqts.add_way(w, q);
    }
    for (r, q) in relqts.items() {
        if q.as_int() < 0 {
            allqts.add_relation(r, Quadtree::new(0));
        } else {
            allqts.add_relation(r, q);
        }
    }
    allqts.finish();
    lt.add("write file");
    Ok((outfn_, lt, max_timestamp))
    //Err(Error::new(ErrorKind::Other,"not implemented"))
}

pub fn run_calcqts_addto_objs(
    fname: &str,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Result<(Vec<PrimitiveBlock>,LogTimes,i64)> {
  
    let mut lt = LogTimes::new();
    
    

    if file_length(fname) > 128 * 1024 * 1024 {
        return Err(Error::new(
            ErrorKind::Other,
            "run_calcqts_addto_objs only suitable for pbf files smaller than 128mb",
        ));
    }

    let (data,mut blocks,max_timestamp) = read_primitive_data(fname, numchan);
    
    lt.add("read data");
    message!(
        "have {} nodes, {} ways, {} relations",
        data.nodes.len(),
        data.ways.len(),
        data.relations.len()
    );
    
    let (nodeqts,wayqts, relqts) = calc_collected_data_quadtrees(&data, &mut lt, qt_level, qt_buffer)?;
    
    for bl in &mut blocks {
        for n in &mut bl.nodes {
            n.set_quadtree(nodeqts.get(n.get_id()).unwrap());
        }
        for w in &mut bl.ways {
            w.set_quadtree(wayqts.get(w.get_id()).unwrap());
        }
        for r in &mut bl.relations {
            r.set_quadtree(relqts.get(r.get_id()).unwrap());
        }
    }
    
    
    lt.add("added objs");
    Ok((blocks, lt, max_timestamp))
    //Err(Error::new(ErrorKind::Other,"not implemented"))read_primitve_data
    
}
