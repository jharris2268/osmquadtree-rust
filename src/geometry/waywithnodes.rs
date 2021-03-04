use crate::callback::CallFinish;
use crate::elements::{Block, Element, Node, PrimitiveBlock, Quadtree};
use crate::geometry::{GeometryStyle, LonLat, OtherData, Timings, WorkingBlock};
use crate::utils::ThreadTimer;

use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};
use std::sync::Arc;

type LocTile = BTreeMap<i64, LonLat>;

struct Locations {
    tiles: BTreeMap<Quadtree, LocTile>,
    max_len: usize,

    num_locs: i64,
    max_locs: i64,
    style: Arc<GeometryStyle>,
}

impl Locations {
    pub fn new(style: Arc<GeometryStyle>) -> Locations {
        Locations {
            tiles: BTreeMap::new(),
            max_len: 0,
            max_locs: 0,
            num_locs: 0,
            style: style,
        }
    }

    pub fn get_loc(&self, i: &i64) -> Option<LonLat> {
        for (_, t) in self.tiles.iter() {
            match t.get(i) {
                None => {}
                Some(p) => {
                    return Some(p.clone());
                }
            }
        }
        None
    }

    pub fn get_locs(&self, refs: &Vec<i64>) -> Result<Vec<LonLat>> {
        let mut res = Vec::with_capacity(refs.len());
        for i in refs {
            match self.get_loc(i) {
                Some(l) => res.push(l),
                None => {
                    return Err(Error::new(ErrorKind::Other, format!("missing node {}", i)));
                }
            }
        }
        Ok(res)
    }

    pub fn remove_finished_tiles(&mut self, tl: &Quadtree) {
        let mut removes = Vec::with_capacity(self.tiles.len());
        for (t, p) in self.tiles.iter() {
            if !t.is_parent(tl) {
                removes.push(t.clone());
                self.num_locs -= p.len() as i64;
            }
        }
        for t in removes {
            self.tiles.remove(&t);
        }
    }
    pub fn remove_all(&mut self) {
        for (_, p) in self.tiles.iter() {
            self.num_locs -= p.len() as i64;
        }
        self.tiles.clear();
    }
    pub fn add_tile(&mut self, qt: Quadtree, nodes: Vec<Node>) -> Vec<Node> {
        let mut res = Vec::with_capacity(nodes.len());
        let mut t = BTreeMap::new();
        for n in nodes {
            t.insert(n.id, LonLat::new(n.lon, n.lat));
            if node_has_tag(&self.style, &n) {
                res.push(n);
            }
        }
        self.num_locs += t.len() as i64;
        self.tiles.insert(qt, t);

        self.max_len = usize::max(self.max_len, self.tiles.len());
        self.max_locs = i64::max(self.max_locs, self.num_locs);
        res
    }
}
impl std::fmt::Display for Locations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Locations[{} tiles [{} max], {} locations [{} max]",
            self.tiles.len(),
            self.max_len,
            self.num_locs,
            self.max_locs
        )
    }
}

fn node_has_tag(style: &GeometryStyle, n: &Node) -> bool {
    for t in &n.tags {
        if style.feature_keys.contains(&t.key) {
            return true;
        }
    }
    false
}

pub struct CollectWayNodes<T: ?Sized> {
    out: Box<T>,
    locs: Locations,
    errs: Vec<(Element, String)>,

    tm: f64,
}

impl<T> CollectWayNodes<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings> + ?Sized,
{
    pub fn new(out: Box<T>, style: Arc<GeometryStyle>) -> CollectWayNodes<T> {
        CollectWayNodes {
            out: out,
            locs: Locations::new(style),
            errs: Vec::new(),
            tm: 0.0,
        }
    }

    pub fn process_tile(&mut self, pb: PrimitiveBlock) -> Result<WorkingBlock> {
        let mut res =
            WorkingBlock::new(pb.get_index(), pb.get_quadtree().clone(), pb.get_end_date());

        self.locs.remove_finished_tiles(&pb.quadtree);
        res.pending_nodes = self.locs.add_tile(pb.quadtree, pb.nodes);

        for w in pb.ways {
            match self.locs.get_locs(&w.refs) {
                Ok(rr) => {
                    res.pending_ways.push((w, rr));
                }
                Err(e) => {
                    self.errs.push((Element::Way(w), e.to_string()));
                }
            }
        }

        res.pending_relations = pb.relations;

        Ok(res)
    }
}

impl<T> CallFinish for CollectWayNodes<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings> + ?Sized,
{
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, pb: PrimitiveBlock) {
        let tx = ThreadTimer::new();
        let r = self.process_tile(pb).expect("!!");
        self.tm += tx.since();
        self.out.call(r);
    }

    fn finish(&mut self) -> Result<Timings> {
        self.locs.remove_all();
        let mut tms = self.out.finish()?;
        tms.add("CollectWayNodes", self.tm);
        tms.add_other(
            "CollectWayNodes",
            OtherData::Messages(vec![self.locs.to_string()]),
        );
        if !self.errs.is_empty() {
            tms.add_other(
                "CollectWayNodes",
                OtherData::Errors(std::mem::take(&mut self.errs)),
            );
        }
        Ok(tms)
    }
}
