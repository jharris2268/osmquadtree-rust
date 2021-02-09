use std::collections::HashMap;
use std::sync::Arc;
use std::io::Result;

use crate::elements::{Quadtree, Block,Element, WithQuadtree};
use crate::sortblocks::QuadtreeTree;



fn get_block<'a, B: Block>(
    blocks: &'a mut HashMap<i64, B>,
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
    blocks: HashMap<i64, BlockType>,
}

impl<'a, BlockType> SortBlocks<BlockType>
    where BlockType : Block
{
    pub fn new(groups: Arc<QuadtreeTree>) -> SortBlocks<BlockType> {
        SortBlocks {
            groups: groups,
            blocks: HashMap::new(),
        }
    }

    fn get_block(&'a mut self, q: &Quadtree) -> &'a mut BlockType {
        get_block(&mut self.blocks, &self.groups, q)
    }

    fn add_object(&mut self, e: Element) -> Result<()> {
        let t = self.get_block(e.get_quadtree());
        t.add_object(e)
    }
     
    /*

    fn add_node(&mut self, n: Node) {
        let t = self.get_block(n.quadtree);
        t.nodes.push(n);
    }

    fn add_way(&mut self, w: Way) {
        let t = self.get_block(w.quadtree);
        t.ways.push(w);
    }

    fn add_relation(&mut self, r: Relation) {
        let t = self.get_block(r.quadtree);
        t.relations.push(r);
    }*/

    pub fn add_all<Iter: Iterator<Item = Element>>(&mut self, bl: Iter)  -> Result<()>{
        
        for o in bl {
            self.add_object(o)?;
        }
        Ok(())
        /*
        
        for n in bl.nodes {
            self.add_node(n);
        }
        for w in bl.ways {
            self.add_way(w);
        }
        for r in bl.relations {
            self.add_relation(r);
        }*/
    }

    pub fn finish(&mut self) -> Vec<BlockType> {
        let mut bv = Vec::new();
        for (_, mut b) in std::mem::take(&mut self.blocks) {
            b.sort();
            bv.push(b);
        }
        bv.sort_by_key(|b| b.get_quadtree().as_int());
        bv
    }
}
