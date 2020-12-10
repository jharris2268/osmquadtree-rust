use crate::callback::{CallFinish, Callback, CallbackMerge};
use crate::elements::{make_elementtype, Bbox, ElementType, MinimalBlock, Quadtree};
use crate::pbfformat::convertblocks::make_convert_minimal_block_parts;
use crate::pbfformat::read_file_block::{file_length, read_all_blocks_with_progbar};
use crate::pbfformat::read_pbf;
use crate::utils::MergeTimings;

use crate::calcqts::quadtree_store::{QuadtreeGetSet, QuadtreeSimple};
use crate::calcqts::write_quadtrees::{PackQuadtrees, WriteQuadTree};
use crate::calcqts::{CallFinishFileBlocks, OtherData, Timings};

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};

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
}

impl CollectTiles {
    pub fn new() -> CollectTiles {
        CollectTiles {
            data: Some(CollectedData::new()),
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
        }
        for w in bl.ways {
            dx.ways
                .insert(w.id, read_pbf::read_delta_packed_int(&w.refs_data));
        }
        for r in bl.relations {
            let mut p = Vec::new();
            for (t, r) in read_pbf::PackedInt::new(&r.types_data)
                .zip(read_pbf::DeltaPackedInt::new(&r.refs_data))
            {
                let c = make_elementtype(t);
                p.push((c, r));
            }
            dx.relations.insert(r.id, p);
        }
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add_other("data", OtherData::CollectedData(self.data.take().unwrap()));
        Ok(tm)
    }
}

pub fn run_calcqts_inmem(
    fname: &str,
    outfn: Option<&str>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Result<()> {
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

    let data = {
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
        for t in tm.others {
            match t {
                (_, OtherData::CollectedData(dd)) => {
                    rd.extend(dd);
                }
                _ => {}
            }
        }
        rd
    };

    println!(
        "have {} nodes, {} ways, {} relations",
        data.nodes.len(),
        data.ways.len(),
        data.relations.len()
    );

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
    println!("calculated {} way qts", wayqts.len());

    let mut nodeqts = QuadtreeSimple::new();

    for (w, rr) in &data.ways {
        let q = wayqts.get(*w).unwrap();
        for r in rr {
            nodeqts.expand(*r, q);
        }
    }
    println!("calculated {} node qts from way qts", nodeqts.len());

    for (n, (ln, lt)) in &data.nodes {
        if !nodeqts.has_value(*n) {
            let q = Quadtree::calculate_point(*ln, *lt, qt_level, qt_buffer);
            nodeqts.set(*n, q);
        }
    }

    println!("have {} node qts", nodeqts.len());

    let mut relrels = Vec::new();

    let mut relqts = QuadtreeSimple::new();

    for (r, mems) in data.relations {
        if mems.is_empty() {
            relqts.set(r, Quadtree::new(0));
        } else {
            for (ty, rf) in mems {
                match ty {
                    ElementType::Node => match nodeqts.get(rf) {
                        Some(nq) => {
                            relqts.expand(r, nq);
                        }
                        None => {}
                    },
                    ElementType::Way => match wayqts.get(rf) {
                        Some(wq) => {
                            relqts.expand(r, wq);
                        }
                        None => {}
                    },
                    ElementType::Relation => {
                        relrels.push((r, rf));
                    }
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

    println!("have {} relqts", relqts.len());

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

    Ok(())
    //Err(Error::new(ErrorKind::Other,"not implemented"))
}
