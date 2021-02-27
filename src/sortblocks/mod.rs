mod addquadtree;
mod prepgraph;
mod quadtreetree;
mod sortblocks;
mod writepbf;
mod inmem;
mod tempfile;

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
pub use quadtreetree::{find_tree_groups, QuadtreeTree, QuadtreeTreeItem};
pub use tempfile::{sort_blocks,WriteTempWhich,WriteTempData,WriteTempFile,WriteTempFileSplit,
        read_temp_data, read_tempfile_locs, read_tempfilesplit_locs, write_tempfile_locs,
        write_tempfilesplit_locs};
pub use inmem::sort_blocks_inmem;
pub use writepbf::{WriteFile, WriteFileInternalLocs,make_packprimblock_many,make_packprimblock_qtindex,make_packprimblock_zeroindex};


