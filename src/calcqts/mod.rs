pub mod calcinmem;
pub mod calculate;
pub mod expand_wayboxes;
pub mod node_waynodes;
pub mod packwaynodes;
pub mod quadtree_store;
pub mod write_quadtrees;

pub use calcinmem::run_calcqts_inmem;
pub use calculate::{run_calcqts, run_calcqts_load_existing, run_calcqts_prelim};

use crate::pbfformat::read_file_block::FileBlock;
use crate::pbfformat::writefile::FileLocs;

use std::collections::BTreeMap;
use std::sync::Arc;

//pub type WayNodeVals = Arc<Vec<(i64, Vec<Vec<u8>>)>>;
pub enum WayNodeVals {
    PackedInMem(Vec<(i64,Vec<u8>)>),
    TempFile(String,FileLocs)
}

#[derive(Clone)]
pub enum NodeWayNodes {
    Combined(String),
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
    FirstWayTile(u64)
}

pub type Timings = crate::utils::Timings<OtherData>;

pub type CallFinishFileBlocks =
    Box<dyn crate::callback::CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>;
