

use std::collections::BTreeMap;
use std::io::Result;

use crate::callback::{CallFinish, Callback, CallbackMerge, CallbackSync};
use crate::elements::Quadtree;
use crate::elements::QuadtreeBlock;
use crate::pbfformat::read_file_block::{FileBlock, read_all_blocks_with_progbar};
use crate::sortblocks::quadtreetree::{find_tree_groups, QuadtreeTree};
use crate::utils::{CallAll, MergeTimings, ReplaceNoneWithTimings, Timer};

use crate::sortblocks::{OtherData, Timings};

struct AddAll {
    groups: Option<Box<QuadtreeTree>>,
    
    tm: f64,
}

impl AddAll {
    pub fn new() -> AddAll {
        AddAll {
            groups: Some(Box::new(QuadtreeTree::new())),
            tm: 0.0,
        }
    }
}

impl CallFinish for AddAll {
    type CallType = PrepedBlock;
    type ReturnType = Timings;

    fn call(&mut self, mb: Self::CallType) {
        let tx = Timer::new();
        let groups = self.groups.as_mut().unwrap();
        for (q, w) in mb.2 {
            groups.add(&q, w);
        
        }

        
        self.tm += tx.since();
    }
    fn finish(&mut self) -> Result<Timings> {
        
        let mut t = Timings::new();
        t.add("addall", self.tm);
        t.add_other(
            "quadtreetree",
            OtherData::QuadtreeTree(self.groups.take().unwrap()),
        );

        Ok(t)
    }
}

type PrepedBlock = (i64, u64, BTreeMap<Quadtree, u32>);
fn prep_block(qb: QuadtreeBlock, maxdepth: usize) -> PrepedBlock {
    let mut t = BTreeMap::new();
    for (_, q) in qb.nodes {
        let q = q.round(maxdepth);
        if !t.contains_key(&q) {
            t.insert(q, 1);
        } else {
            *t.get_mut(&q).unwrap() += 1;
        }
    }
    for (_, q) in qb.ways {
        let q = q.round(maxdepth);
        if !t.contains_key(&q) {
            t.insert(q, 1);
        } else {
            *t.get_mut(&q).unwrap() += 1;
        }
    }
    for (_, q) in qb.relations {
        if q.as_int() < 0 {
            //pass
        } else {
            //let q = if q.as_int()<0 { Quadtree::new(0) } else {q.round(self.maxdepth)};
            if !t.contains_key(&q) {
                t.insert(q, 1);
            } else {
                *t.get_mut(&q).unwrap() += 1;
            }
        }
    }
    (qb.idx, qb.loc, t)
}

fn make_convertquadtreeblock<T: CallFinish<CallType = PrepedBlock, ReturnType = Timings>>(
    out: Box<T>,
    maxdepth: usize,
) -> Box<impl CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>> {
    let conv = Box::new(move |(i, fb): (usize, FileBlock)| {
        if fb.block_type == "OSMHeader" {
            (0, 0, BTreeMap::new())
        } else {
            let qb =
                QuadtreeBlock::unpack(i as i64, fb.pos, &fb.data()).expect("failed to read block");
            prep_block(qb, maxdepth)
        }
    });

    Box::new(CallAll::new(out, "convertquadtreeblock", conv))
}

pub fn find_groups(
    qtsfn: &str,
    numchan: usize,
    maxdepth: usize,
    target: i64,
    mintarget: i64,
) -> Result<Box<QuadtreeTree>> {
    
    let cc: Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>> =
        if numchan > 0 {
            let aa = CallbackSync::new(Box::new(AddAll::new()), numchan);
            let mut bb: Vec<
                Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>,
            > = Vec::new();
            for a in aa {
                let a2 = Box::new(ReplaceNoneWithTimings::new(a));
                bb.push(Box::new(Callback::new(make_convertquadtreeblock(
                    a2, maxdepth,
                ))));
            }
            Box::new(CallbackMerge::new(bb, Box::new(MergeTimings::new())))
        } else {
            make_convertquadtreeblock(Box::new(AddAll::new()), maxdepth)
        };
    
    let (mut t,_) = read_all_blocks_with_progbar(qtsfn, cc, "prepare quadtreetree");
    
    println!("{}", t);

    let mut tree: Option<Box<QuadtreeTree>> = None;
    for (_, b) in std::mem::take(&mut t.others) {
        match b {
            OtherData::QuadtreeTree(t) => {
                tree = Some(t);
            }
            _ => {}
        }
    }

    let tree = tree.unwrap();
    println!("{}", tree);

    find_tree_groups(tree, target, mintarget)

}
