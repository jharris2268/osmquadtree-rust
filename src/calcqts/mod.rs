mod calcinmem;
mod calculate;
mod expand_wayboxes;
mod node_waynodes;
mod packwaynodes;
mod quadtree_store;
mod write_quadtrees;

pub use calcinmem::run_calcqts_inmem;
pub use calculate::{run_calcqts, run_calcqts_load_existing, run_calcqts_prelim};

use crate::pbfformat::{FileBlock, FileLocs};

use std::collections::BTreeMap;
use std::sync::Arc;

//pub type WayNodeVals = Arc<Vec<(i64, Vec<Vec<u8>>)>>;
pub enum WayNodeVals {
    PackedInMem(Vec<(i64, Vec<u8>)>),
    TempFile(String, FileLocs),
}

#[derive(Clone)]
pub enum NodeWayNodes {
    //Combined(String),
    InMem(String, Arc<WayNodeVals>, u64),
    Seperate(String, String, FileLocs, u64),
}

pub enum OtherData {
    PackedWayNodes(WayNodeVals),
    RelMems(packwaynodes::RelMems),
    QuadtreeSimple(Box<quadtree_store::QuadtreeSimple>),
    QuadtreeGetSet(Box<dyn quadtree_store::QuadtreeGetSet>),
    QuadtreeTiles(Vec<Box<quadtree_store::QuadtreeTileInt>>),
    WayBoxTiles(BTreeMap<i64, Box<expand_wayboxes::WayBoxesVec>>),
    NumTiles(usize),
    WriteQuadTree(Box<write_quadtrees::WriteQuadTree>),
    FileLen(u64),
    CollectedData(calcinmem::CollectedData),
    FirstWayTile(u64),
}

pub type Timings = channelled_callbacks::Timings<OtherData>;

pub type CallFinishFileBlocks =
    Box<dyn channelled_callbacks::CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>;
