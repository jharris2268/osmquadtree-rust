use super::primitive_block::{PrimitiveBlock,Node,Way,Relation,Changetype};
use super::minimal_block::{MinimalBlock,MinimalNode,MinimalWay,MinimalRelation};

use std::cmp::{Ordering};

trait WithIdAndChangetype {
    fn get_id(&self) -> i64;
    fn get_changetype(&mut self) -> &mut Changetype;
}

impl WithIdAndChangetype for Node {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}
impl WithIdAndChangetype for Way {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}

impl WithIdAndChangetype for Relation {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}

impl WithIdAndChangetype for MinimalNode {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}
impl WithIdAndChangetype for MinimalWay {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}

impl WithIdAndChangetype for MinimalRelation {
    fn get_id(&self) -> i64 {
        self.id
    }
    
    fn get_changetype(&mut self) -> &mut Changetype {
        &mut self.changetype
    }
}

fn combine<T: WithIdAndChangetype + Ord>(mut left: Vec<T>, mut right: Vec<T>) -> Vec<T> {
    let mut res = Vec::new();
    
    left.reverse();
    right.reverse();
    
    
    while !left.is_empty() && !right.is_empty() {
        if left.is_empty() {
            res.push(right.pop().unwrap());
            
        } else if right.is_empty() {
            res.push(left.pop().unwrap());
        } else {
            match right.last().unwrap().get_id().cmp(&left.last().unwrap().get_id()) {
                Ordering::Less => {
                    res.push(left.pop().unwrap());
                    
                },
                Ordering::Equal => {
                    left.pop();
                    
                    res.push(right.pop().unwrap());
                },
                Ordering::Greater => {
                    res.push(right.pop().unwrap());
                },
            }
        }
    }
    res.reverse();
    res
}

fn check_changetype<T: WithIdAndChangetype>(o: &mut T) -> bool {
    match o.get_changetype() {
        Changetype::Normal => { return true; },
        Changetype::Delete => { return false; }
        Changetype::Remove => { return false; }
        _ => {}
    }
    *o.get_changetype() = Changetype::Normal;
    true
}

fn apply_change<T: WithIdAndChangetype + Ord>(mut left: Vec<T>, mut right: Vec<T>) -> Vec<T> {
    let mut res = Vec::new();
    
    left.reverse();
    right.reverse();
    
    
    while !left.is_empty() && !right.is_empty() {
        if left.is_empty() {
            let mut r = right.pop().unwrap();
            if check_changetype(&mut r) {
                res.push(r);
            }
            
        } else if right.is_empty() {
            let mut l = right.pop().unwrap();
            if check_changetype(&mut l) {
                res.push(l);
            }
        } else {
            match right.last().unwrap().get_id().cmp(&left.last().unwrap().get_id()) {
                Ordering::Less => {
                    let mut l = right.pop().unwrap();
                    if check_changetype(&mut l) {
                        res.push(l);
                    }
                    
                },
                Ordering::Equal => {
                    left.pop();
                    
                    let mut r = right.pop().unwrap();
                    if check_changetype(&mut r) {
                        res.push(r);
                    }
                },
                Ordering::Greater => {
                    let mut r = right.pop().unwrap();
                    if check_changetype(&mut r) {
                        res.push(r);
                    }
                },
            }
        }
    }
    res.reverse();
    res
}



pub fn combine_block_primitive(mut left: PrimitiveBlock, mut right: PrimitiveBlock) -> PrimitiveBlock {
    left.nodes = combine(std::mem::take(&mut left.nodes), std::mem::take(&mut right.nodes));
    left.ways = combine(std::mem::take(&mut left.ways), std::mem::take(&mut right.ways));
    left.relations = combine(std::mem::take(&mut left.relations), std::mem::take(&mut right.relations));
    
    
    left
}

pub fn combine_block_minimal(mut left: MinimalBlock, mut right: MinimalBlock) -> MinimalBlock {
    left.nodes = combine(std::mem::take(&mut left.nodes), std::mem::take(&mut right.nodes));
    left.ways = combine(std::mem::take(&mut left.ways), std::mem::take(&mut right.ways));
    left.relations = combine(std::mem::take(&mut left.relations), std::mem::take(&mut right.relations));
    
    
    left
}

pub fn apply_change_primitive(mut left: PrimitiveBlock, mut right: PrimitiveBlock) -> PrimitiveBlock {
    left.nodes = apply_change(std::mem::take(&mut left.nodes), std::mem::take(&mut right.nodes));
    left.ways = apply_change(std::mem::take(&mut left.ways), std::mem::take(&mut right.ways));
    left.relations = apply_change(std::mem::take(&mut left.relations), std::mem::take(&mut right.relations));
    
    
    left
}

pub fn apply_change_minimal(mut left: MinimalBlock, mut right: MinimalBlock) -> MinimalBlock {
    left.nodes = apply_change(std::mem::take(&mut left.nodes), std::mem::take(&mut right.nodes));
    left.ways = apply_change(std::mem::take(&mut left.ways), std::mem::take(&mut right.ways));
    left.relations = apply_change(std::mem::take(&mut left.relations), std::mem::take(&mut right.relations));
    
    
    left
}

pub fn merge_changes_primitive(orig: PrimitiveBlock, mut changes: Vec<PrimitiveBlock>) -> PrimitiveBlock {
    if changes.is_empty() {
        return orig;
    }
    let mut merged_change = changes.pop().unwrap();
    
    while !changes.is_empty() {
        merged_change = combine_block_primitive(changes.pop().unwrap(), merged_change);
    }
    
    apply_change_primitive(orig, merged_change)
}
    
pub fn merge_changes_minimal(orig: MinimalBlock, mut changes: Vec<MinimalBlock>) -> MinimalBlock {
    if changes.is_empty() {
        return orig;
    }
    let mut merged_change = changes.pop().unwrap();
    
    while !changes.is_empty() {
        merged_change = combine_block_minimal(changes.pop().unwrap(), merged_change);
    }
    
    apply_change_minimal(orig, merged_change)
}
