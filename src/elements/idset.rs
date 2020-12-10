
use crate::elements::primitive_block::{ElementType};


use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone)]
pub struct IdSet {
    pub nodes: BTreeSet<i64>,
    pub exnodes: BTreeSet<i64>,
    pub ways: BTreeSet<i64>,
    pub relations: BTreeSet<i64>,
}

impl IdSet {
    pub fn new() -> IdSet {
        IdSet{nodes: BTreeSet::new(), ways: BTreeSet::new(), relations: BTreeSet::new(), exnodes: BTreeSet::new()}
    }
    
    pub fn contains(&self, t: ElementType, id: i64) -> bool {
        match t {
            ElementType::Node => self.nodes.contains(&id),
            ElementType::Way => self.ways.contains(&id),
            ElementType::Relation => self.relations.contains(&id),
        }
    }
    pub fn is_exnode(&self, id: i64) -> bool {
        self.exnodes.contains(&id)
    }
    
    
    
}

impl fmt::Display for IdSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdSet: {} nodes, {} ways, {} relations, {} exnodes", self.nodes.len(), self.ways.len(), self.relations.len(), self.exnodes.len())
    }
}
