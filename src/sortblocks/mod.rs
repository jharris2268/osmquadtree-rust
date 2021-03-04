mod addquadtree;
mod inmem;
mod prepgraph;
mod quadtreetree;
mod sortblocks;
mod tempfile;
mod writepbf;

use crate::elements::PrimitiveBlock;

type FileLocs = Vec<(i64, Vec<(u64, u64)>)>;

pub enum TempData {
    TempBlocks(Vec<(i64, Vec<Vec<u8>>)>),
    TempFile((String, FileLocs)),
    TempFileSplit(Vec<(i64, String, FileLocs)>),
}

pub enum OtherData {
    FileLocs(FileLocs),
    TempData(TempData),
    QuadtreeTree(Box<quadtreetree::QuadtreeTree>),
    AllBlocks(Vec<PrimitiveBlock>),
}

pub type Timings = crate::utils::Timings<OtherData>;

pub use inmem::sort_blocks_inmem;
pub use prepgraph::find_groups;
pub use quadtreetree::{find_tree_groups, QuadtreeTree, QuadtreeTreeItem};
pub use tempfile::{
    read_temp_data, read_tempfile_locs, read_tempfilesplit_locs, sort_blocks, write_tempfile_locs,
    write_tempfilesplit_locs, WriteTempData, WriteTempFile, WriteTempFileSplit, WriteTempWhich,
};
pub use writepbf::{
    make_packprimblock_many, make_packprimblock_qtindex, make_packprimblock_zeroindex, WriteFile,
    WriteFileInternalLocs,
};
