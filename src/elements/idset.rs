use crate::elements::traits::ElementType;

use std::collections::{BTreeSet,BTreeMap};
use std::fmt;




pub trait IdSet: Sync+Send+'static + fmt::Display {
    fn contains(&self, t: ElementType, id: i64) -> bool;
    
}

pub struct IdSetAll();

impl IdSet for IdSetAll {
    fn contains(&self, _t: ElementType, _id: i64) -> bool {
        true
    }
}

impl fmt::Display for IdSetAll  {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdSetAll")
    }
}


#[derive(Clone)]
pub struct IdSetSet {
    pub nodes: BTreeSet<i64>,
    pub exnodes: BTreeSet<i64>,
    pub ways: BTreeSet<i64>,
    pub relations: BTreeSet<i64>,
}

impl IdSetSet {
    pub fn new() -> IdSetSet {
        IdSetSet {
            nodes: BTreeSet::new(),
            ways: BTreeSet::new(),
            relations: BTreeSet::new(),
            exnodes: BTreeSet::new(),
        }
    }

    
    pub fn is_exnode(&self, id: i64) -> bool {
        self.exnodes.contains(&id)
    }
}

impl IdSet for IdSetSet {
    fn contains(&self, t: ElementType, id: i64) -> bool {
        match t {
            ElementType::Node => self.nodes.contains(&id),
            ElementType::Way => self.ways.contains(&id),
            ElementType::Relation => self.relations.contains(&id),
        }
    }
}

impl fmt::Display for IdSetSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IdSetSet: {} nodes, {} ways, {} relations, {} exnodes",
            self.nodes.len(),
            self.ways.len(),
            self.relations.len(),
            self.exnodes.len()
        )
    }
}

const BITVECSET_SPLIT:i64 =1<<22;
pub struct BitVecSet {
    tiles: BTreeMap<i64, Vec<u8>>,
    count: usize
}

impl BitVecSet {
    pub fn new() -> BitVecSet {
        BitVecSet{tiles: BTreeMap::new(), count: 0 }
    }
    pub fn len(&self) -> usize {
        self.count
    }
    
    pub fn insert(&mut self, v: i64) {
        let k = v / BITVECSET_SPLIT;
        let vi = (v % BITVECSET_SPLIT) as usize;
        
        let vik = vi / 8;
        let vii = (vi % 8) as u8;
        
        match self.tiles.get_mut(&k) {
            None => {
                let mut nn = vec![0u8; (BITVECSET_SPLIT/8) as usize];
                nn[vik] = 1<<vii as u8;
                self.tiles.insert(k, nn);
                self.count+=1;
            },
            Some(nn) => {
                if ((nn[vik] >> vii) & 1)==0 {
                    nn[vik] |= 1 << vii;
                    self.count+=1;
                }
            }
        }
    }
    
    pub fn contains(&self, v: &i64) -> bool {
        let k = v / BITVECSET_SPLIT;
        let vi = (v % BITVECSET_SPLIT) as usize;
        
        let vik = vi / 8;
        let vii = (vi % 8) as u8;
        match self.tiles.get(&k) {
            None => {
                false
            },
            Some(nn)=>{
                ((nn[vik] >> vii) & 1)==1
            }
        }
    }
}




pub struct IdSetBool {
    pub nodes: BitVecSet,
    pub exnodes: BTreeSet<i64>,
    pub ways: BitVecSet,
    pub relations:BitVecSet,
}

impl IdSetBool {
    pub fn new() -> IdSetBool {
        IdSetBool {
            nodes: BitVecSet::new(),
            ways: BitVecSet::new(),
            relations: BitVecSet::new(),
            exnodes: BTreeSet::new(),
        }
    }

    
    pub fn is_exnode(&self, id: i64) -> bool {
        self.exnodes.contains(&id)
    }
}

impl IdSet for IdSetBool {
    fn contains(&self, t: ElementType, id: i64) -> bool {
        match t {
            ElementType::Node => self.nodes.contains(&id),
            ElementType::Way => self.ways.contains(&id),
            ElementType::Relation => self.relations.contains(&id),
        }
    }
}

impl fmt::Display for IdSetBool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "IdSetBool: {} nodes, {} ways, {} relations, {} exnodes, [{} tiles, {} mb]",
            self.nodes.len(),
            self.ways.len(),
            self.relations.len(),
            self.exnodes.len(),
            self.nodes.tiles.len()+self.ways.tiles.len()+self.relations.tiles.len(),
            ((self.nodes.tiles.len()+self.ways.tiles.len()+self.relations.tiles.len()) as f64)*(BITVECSET_SPLIT as f64)/8.0/1024.0/1024.0
        )
    }
}
