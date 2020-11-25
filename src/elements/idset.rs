
use super::primitive_block::{Node,Way,Relation,ElementType};
//use super::minimal_block::{MinimalNode,MinimalWay,MinimalRelation,MinimalBlock};

use std::collections::BTreeSet;
use std::fmt;

#[derive(Clone)]
pub struct IdSet {
    nodes: BTreeSet<i64>,
    exnodes: BTreeSet<i64>,
    ways: BTreeSet<i64>,
    relations: BTreeSet<i64>,
}

impl IdSet {
    pub fn new() -> IdSet {
        IdSet{nodes: BTreeSet::new(), ways: BTreeSet::new(), relations: BTreeSet::new(), exnodes: BTreeSet::new()}
    }
    
    pub fn contains(&self, t: ElementType, id: i64) -> bool {
        match t {
            ElementType::Node => self.nodes.contains(&id) || self.exnodes.contains(&id),
            ElementType::Way => self.ways.contains(&id),
            ElementType::Relation => self.relations.contains(&id),
        }
    }
    
    pub fn add_node(&mut self, n: &Node) {
        self.nodes.insert(n.id);
    }
    
    pub fn add_way(&mut self, w: &Way) {
        self.ways.insert(w.id);
        self.exnodes.extend(w.refs.iter());
        /*for r in w.refs.iter() {
            self.exnodes.insert(*r);
        }*/
    }
    
    pub fn add_relation(&mut self, r: &Relation) {
        self.relations.insert(r.id);
    }

    pub fn clip_exnodes(&mut self) {
        self.exnodes = self.exnodes.difference(&self.nodes).map(|n| {*n}).collect();
    }
}

impl fmt::Display for IdSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IdSet: {} nodes, {} ways, {} relations, {} exnodes", self.nodes.len(), self.ways.len(), self.relations.len(), self.exnodes.len())
    }
}
