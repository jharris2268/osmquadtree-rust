use std::collections::BTreeMap;
use std::io::Result;
use std::sync::Arc;

use crate::elements::{Block, Quadtree, WithQuadtree};
use crate::sortblocks::QuadtreeTree;

fn get_block<'a, B: Block>(
    blocks: &'a mut BTreeMap<i64, B>,
    groups: &'a QuadtreeTree,
    q: &Quadtree,
) -> &'a mut B {
    let (_, b) = groups.find(q);
    let q = b.qt.as_int();
    if !blocks.contains_key(&q) {
        let t = B::with_quadtree(b.qt.clone());
        //let mut t = PrimitiveBlock::new(0,0);
        //t.quadtree = b.qt;
        blocks.insert(q.clone(), t);
    }
    blocks.get_mut(&q).unwrap()
}

pub struct SortBlocks<BlockType: Block> {
    groups: Arc<QuadtreeTree>,
    blocks: BTreeMap<i64, BlockType>,
}

impl<'a, BlockType> SortBlocks<BlockType>
where
    BlockType: Block,
{
    pub fn new(groups: Arc<QuadtreeTree>) -> SortBlocks<BlockType> {
        SortBlocks {
            groups: groups,
            blocks: BTreeMap::new(),
        }
    }

    fn get_block(&'a mut self, q: &Quadtree) -> &'a mut BlockType {
        get_block(&mut self.blocks, &self.groups, q)
    }

    fn add_object(&mut self, e: BlockType::Element) -> Result<()> {
        let t = self.get_block(e.get_quadtree());
        t.add_object(e)
    }

    pub fn add_all<Iter: Iterator<Item = BlockType::Element>>(&mut self, bl: Iter) -> Result<()> {
        for o in bl {
            self.add_object(o)?;
        }
        Ok(())
        
    }

    pub fn finish(&mut self) -> Vec<BlockType> {
        let mut bv = Vec::new();
        for (_, mut b) in std::mem::take(&mut self.blocks) {
            b.sort();
            bv.push(b);
        }
        //bv.sort_by_key(|b| b.get_quadtree().as_int());
        bv
    }
}


pub struct CollectTemp<B: Block> {
    
    limit: usize,
    splitat: i64,
    groups: Arc<QuadtreeTree>,
    pending: BTreeMap<i64, B>,
    qttoidx: BTreeMap<Quadtree, i64>,
    
}

impl<'a, BlockType> CollectTemp<BlockType>
where
    BlockType: Block
{
    pub fn new(
        limit: usize,
        splitat: i64,
        groups: Arc<QuadtreeTree>,
    ) -> CollectTemp<BlockType> {
        let mut qttoidx = BTreeMap::new();
        let mut i = 0;
        for (_, t) in groups.iter() {
            qttoidx.insert(t.qt, i);
            i += 1;
        }
        CollectTemp {
            limit: limit,
            splitat: splitat,
            groups: groups,
            qttoidx: qttoidx,
            pending: BTreeMap::new(),
        }
    }

    pub fn add_all<Iter: Iterator<Item = BlockType::Element>>(&mut self, bl: Iter) -> Result<Vec<(i64, BlockType)>> {
        let mut mm = Vec::new();
        for e in bl {
            match self.add_object(e)? {
                None => {},
                Some(m) => mm.push(m)
            }
        }
        Ok(mm)
    }

    fn get_block(&'a mut self, q: &Quadtree) -> (i64,&'a mut BlockType) {
        let tq = self.groups.find(q).1.qt;
        let i = self.qttoidx.get(&tq).unwrap();
        let k = i / self.splitat;
        if !self.pending.contains_key(&k) {
            let t = BlockType::with_quadtree(Quadtree::empty());
            self.pending.insert(k.clone(), t);
        }
        (k,self.pending.get_mut(&k).unwrap())
    }

    fn add_object(&mut self, n: BlockType::Element) -> Result<Option<(i64,BlockType)>> {
        let l=self.limit;
        let (i,t) = self.get_block(n.get_quadtree());
        t.add_object(n)?;
        if t.weight() >= l {
            return Ok(Some((i,std::mem::replace(t, BlockType::with_quadtree(Quadtree::empty())))));
        }
        Ok(None)
    }

    
    pub fn finish(&mut self) -> Vec<(i64,BlockType)> {
        let mut bv = Vec::new();
        for (i, mut b) in std::mem::take(&mut self.pending) {
            b.sort();
            bv.push((i,b));
        }
        //bv.sort_by_key(|b| b.0);
        bv
    }
    
}


