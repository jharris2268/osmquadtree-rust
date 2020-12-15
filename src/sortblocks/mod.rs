pub mod addquadtree;
pub mod prepgraph;
pub mod quadtreetree;
pub mod sortblocks;
pub mod writepbf;

use crate::elements::PrimitiveBlock;

type FileLocs = Vec<(i64, Vec<(u64, u64)>)>;

pub enum TempData {
    TempBlocks(Vec<(i64, Vec<Vec<u8>>)>),
    TempFile((String, FileLocs)),
    TempFileSplit(Vec<(i64, String, FileLocs)>)
}

pub enum OtherData {
    FileLocs(FileLocs),
    TempData(TempData),
    QuadtreeTree(Box<quadtreetree::QuadtreeTree>),
    AllBlocks(Vec<PrimitiveBlock>),
}

pub type Timings = crate::utils::Timings<OtherData>;

pub use prepgraph::find_groups;
pub use quadtreetree::{QuadtreeTree, QuadtreeTreeItem};
pub use sortblocks::{sort_blocks, sort_blocks_inmem};
pub use writepbf::{WriteFile, WriteFileInternalLocs};
