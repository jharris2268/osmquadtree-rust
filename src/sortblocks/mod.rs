pub mod prepgraph;
pub mod sortblocks;
pub mod quadtreetree;
pub mod writepbf;
pub mod addquadtree;

use super::elements::PrimitiveBlock;


pub enum OtherData {
    FileLocs(Vec<(i64,Vec<(u64,u64)>)>),
    TempData(Vec<(i64,Vec<Vec<u8>>)>),
    QuadtreeTree(Box<quadtreetree::QuadtreeTree>),
    AllBlocks(Vec<PrimitiveBlock>),
}

pub type Timings = super::utils::Timings<OtherData>;

pub use prepgraph::find_groups;
pub use quadtreetree::{QuadtreeTree,QuadtreeTreeItem};
pub use sortblocks::{sort_blocks,sort_blocks_inmem};
