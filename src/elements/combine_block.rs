use crate::elements::{Changetype, Node, PrimitiveBlock, Relation, Way};
use crate::elements::{MinimalBlock, MinimalNode, MinimalRelation, MinimalWay};

use std::cmp::Ordering;

trait WithIdAndChangetype {
    fn get_id(&self) -> i64;
    fn get_changetype(&self) -> Changetype;
    fn set_changetype(&mut self, c: Changetype);
}

impl WithIdAndChangetype for Node {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}
impl WithIdAndChangetype for Way {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}

impl WithIdAndChangetype for Relation {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}

impl WithIdAndChangetype for MinimalNode {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}
impl WithIdAndChangetype for MinimalWay {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}

impl WithIdAndChangetype for MinimalRelation {
    fn get_id(&self) -> i64 {
        self.id
    }

    fn get_changetype(&self) -> Changetype {
        self.changetype
    }
    
    fn set_changetype(&mut self, c: Changetype) {
        self.changetype = c;
    }
}

fn combine<T: WithIdAndChangetype + Ord>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut res = Vec::new();

    let mut left_iter = left.into_iter();
    let mut right_iter = right.into_iter();

    let mut left_curr = left_iter.next();
    let mut right_curr = right_iter.next();

    while !(left_curr == None && right_curr == None) {
        if left_curr == None {
            res.push(right_curr.take().unwrap());
            right_curr = right_iter.next();
        } else if right_curr == None {
            res.push(left_curr.take().unwrap());
            left_curr = left_iter.next();
        } else {
            match left_curr
                .as_ref()
                .unwrap()
                .get_id()
                .cmp(&right_curr.as_ref().unwrap().get_id())
            {
                Ordering::Less => {
                    res.push(left_curr.take().unwrap());
                    left_curr = left_iter.next();
                }
                Ordering::Equal => {
                    left_curr = left_iter.next();
                    res.push(right_curr.take().unwrap());
                    right_curr = right_iter.next();
                }
                Ordering::Greater => {
                    res.push(right_curr.take().unwrap());
                    right_curr = right_iter.next();
                }
            }
        }
    }

    res
}


fn combine_clone<T: WithIdAndChangetype + Ord + Clone>(left: &[T], right: &[T]) -> Vec<T> {
    let mut res = Vec::new();

    let mut left_iter = left.into_iter();
    let mut right_iter = right.into_iter();

    let mut left_curr = left_iter.next();
    let mut right_curr = right_iter.next();

    while !(left_curr == None && right_curr == None) {
        if left_curr == None {
            res.push(right_curr.unwrap().clone());
            right_curr = right_iter.next();
        } else if right_curr == None {
            res.push(left_curr.unwrap().clone());
            left_curr = left_iter.next();
        } else {
            match left_curr
                .as_ref()
                .unwrap()
                .get_id()
                .cmp(&right_curr.as_ref().unwrap().get_id())
            {
                Ordering::Less => {
                    res.push(left_curr.unwrap().clone());
                    left_curr = left_iter.next();
                }
                Ordering::Equal => {
                    left_curr = left_iter.next();
                    res.push(right_curr.unwrap().clone());
                    right_curr = right_iter.next();
                }
                Ordering::Greater => {
                    res.push(right_curr.unwrap().clone());
                    right_curr = right_iter.next();
                }
            }
        }
    }

    res
}


fn check_changetype<T: WithIdAndChangetype>(o: &mut T) -> bool {
    match o.get_changetype() {
        Changetype::Normal => {
            return true;
        }
        Changetype::Delete => {
            return false;
        }
        Changetype::Remove => {
            return false;
        }
        _ => {}
    }
    o.set_changetype(Changetype::Normal);
    true
}

fn check_changetype_clone<T: WithIdAndChangetype + Clone>(o: &T) -> Option<T> {
    match o.get_changetype() {
        Changetype::Normal => {
            Some(o.clone())
        }
        Changetype::Delete => {
            None
        }
        Changetype::Remove => {
            None
        }
        _ => {
            let mut n = o.clone();
            n.set_changetype(Changetype::Normal);
            Some(n)
        }
    }
    
}

fn check_changetype_add<T: WithIdAndChangetype>(res: &mut Vec<T>, obj: &mut Option<T>) {
    let mut r = obj.take().unwrap();
    if check_changetype(&mut r) {
        res.push(r);
    }
}

fn check_changetype_clone_add<T: WithIdAndChangetype+Clone>(res: &mut Vec<T>, obj: &Option<&T>) {
    
    match check_changetype_clone(obj.unwrap()) {
        Some(r) => { res.push(r); },
        None => {}
        
    }
}



fn apply_change<T: WithIdAndChangetype + Ord>(left: Vec<T>, right: Vec<T>) -> Vec<T> {
    let mut res = Vec::new();

    let mut left_iter = left.into_iter();
    let mut right_iter = right.into_iter();

    let mut left_curr = left_iter.next();
    let mut right_curr = right_iter.next();

    while !(left_curr == None && right_curr == None) {
        if left_curr == None {
            check_changetype_add(&mut res, &mut right_curr);
            right_curr = right_iter.next();
        } else if right_curr == None {
            check_changetype_add(&mut res, &mut left_curr);
            left_curr = left_iter.next();
        } else {
            match left_curr
                .as_ref()
                .unwrap()
                .get_id()
                .cmp(&right_curr.as_ref().unwrap().get_id())
            {
                Ordering::Less => {
                    check_changetype_add(&mut res, &mut left_curr);
                    left_curr = left_iter.next();
                }
                Ordering::Equal => {
                    left_curr = left_iter.next();
                    check_changetype_add(&mut res, &mut right_curr);
                    right_curr = right_iter.next();
                }
                Ordering::Greater => {
                    check_changetype_add(&mut res, &mut right_curr);
                    right_curr = right_iter.next();
                }
            }
        }
    }

    res
}

fn apply_change_clone<T: WithIdAndChangetype + Ord + Clone>(left: &[T], right: &[T]) -> Vec<T> {
    let mut res = Vec::new();

    let mut left_iter = left.into_iter();
    let mut right_iter = right.into_iter();

    let mut left_curr = left_iter.next();
    let mut right_curr = right_iter.next();

    while !(left_curr == None && right_curr == None) {
        if left_curr == None {
            check_changetype_clone_add(&mut res, &right_curr);
            right_curr = right_iter.next();
        } else if right_curr == None {
            check_changetype_clone_add(&mut res, &left_curr);
            left_curr = left_iter.next();
        } else {
            match left_curr
                .as_ref()
                .unwrap()
                .get_id()
                .cmp(&right_curr.as_ref().unwrap().get_id())
            {
                Ordering::Less => {
                    check_changetype_clone_add(&mut res, &left_curr);
                    left_curr = left_iter.next();
                }
                Ordering::Equal => {
                    left_curr = left_iter.next();
                    check_changetype_clone_add(&mut res, &right_curr);
                    right_curr = right_iter.next();
                }
                Ordering::Greater => {
                    check_changetype_clone_add(&mut res, &right_curr);
                    right_curr = right_iter.next();
                }
            }
        }
    }

    res
}

pub fn combine_block_primitive(
    mut left: PrimitiveBlock,
    mut right: PrimitiveBlock,
) -> PrimitiveBlock {
    left.nodes = combine(
        std::mem::take(&mut left.nodes),
        std::mem::take(&mut right.nodes),
    );
    left.ways = combine(
        std::mem::take(&mut left.ways),
        std::mem::take(&mut right.ways),
    );
    left.relations = combine(
        std::mem::take(&mut left.relations),
        std::mem::take(&mut right.relations),
    );
    
    left.end_date = right.end_date;
    
    left
}

pub fn combine_block_primitive_clone(
    left: &PrimitiveBlock,
    right: &PrimitiveBlock,
) -> PrimitiveBlock {
    
    let mut res = PrimitiveBlock::new(left.index, left.location);
    res.quadtree = left.quadtree.clone();
    res.start_date = left.start_date;
    res.end_date = right.end_date;
    
    
    res.nodes = combine_clone(&left.nodes, &right.nodes);
    res.ways = combine_clone(&left.ways, &right.ways);
    res.relations = combine_clone(&left.relations, &right.relations);
    res
}


pub fn combine_block_minimal(mut left: MinimalBlock, mut right: MinimalBlock) -> MinimalBlock {
    left.nodes = combine(
        std::mem::take(&mut left.nodes),
        std::mem::take(&mut right.nodes),
    );
    left.ways = combine(
        std::mem::take(&mut left.ways),
        std::mem::take(&mut right.ways),
    );
    left.relations = combine(
        std::mem::take(&mut left.relations),
        std::mem::take(&mut right.relations),
    );
    left.end_date = right.end_date;
    left
}

pub fn apply_change_primitive(
    mut left: PrimitiveBlock,
    mut right: PrimitiveBlock,
) -> PrimitiveBlock {
    left.nodes = apply_change(
        std::mem::take(&mut left.nodes),
        std::mem::take(&mut right.nodes),
    );
    left.ways = apply_change(
        std::mem::take(&mut left.ways),
        std::mem::take(&mut right.ways),
    );
    left.relations = apply_change(
        std::mem::take(&mut left.relations),
        std::mem::take(&mut right.relations),
    );
    left.end_date = right.end_date;
    left
}

pub fn apply_change_primitive_clone(
    left: &PrimitiveBlock,
    right: &PrimitiveBlock,
) -> PrimitiveBlock {
    
    let mut res = PrimitiveBlock::new(left.index, left.location);
    res.quadtree = left.quadtree.clone();
    res.start_date = left.start_date;
    res.end_date = right.end_date;
    
    
    res.nodes = apply_change_clone(&left.nodes, &right.nodes);
    res.ways = apply_change_clone(&left.ways, &right.ways);
    res.relations = apply_change_clone(&left.relations, &right.relations);
    res
}
    
    
    
pub fn apply_change_minimal(mut left: MinimalBlock, mut right: MinimalBlock) -> MinimalBlock {
    left.nodes = apply_change(
        std::mem::take(&mut left.nodes),
        std::mem::take(&mut right.nodes),
    );
    left.ways = apply_change(
        std::mem::take(&mut left.ways),
        std::mem::take(&mut right.ways),
    );
    left.relations = apply_change(
        std::mem::take(&mut left.relations),
        std::mem::take(&mut right.relations),
    );
    left.end_date = right.end_date;
    left
}

pub fn merge_changes_primitive(
    orig: PrimitiveBlock,
    mut changes: Vec<PrimitiveBlock>,
) -> PrimitiveBlock {
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
