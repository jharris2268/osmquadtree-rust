use std::sync::Arc;
use std::collections::{BTreeMap,BTreeSet};

use crate::elements::{Node,Way,Tag,Quadtree};
use crate::geometry::{GeometryStyle,Timings,WorkingBlock,OtherData};
use crate::geometry::style::ParentTagSpec;
use crate::callback::CallFinish;
use crate::utils::ThreadTimer;

fn find_tag<'a>(tgs: &'a Vec<Tag>, k: &String) -> Option<&'a String> {
    for t in tgs {
        if &t.key==k {
            return Some(&t.val);
        }
    }
    None
}

pub struct AddParentTag<T> {
    out: Box<T>,
    style: Arc<GeometryStyle>,
    pending: BTreeMap<i64,(Node,BTreeMap<String,(String,i64)>)>,
    pending_tiles: BTreeMap<Quadtree,Vec<i64>>,
    node_keys:BTreeSet<String>,
    total_added: usize,
    tags_added: usize,
    finished: usize,
    tm: f64
}
fn get_prio(sp: &ParentTagSpec, v: &String) -> Option<i64> {
    if sp.way_priority.is_empty() {
        return Some(0);
    }
    match sp.way_priority.get(v) {
        None => None,
        Some(p) => Some(*p)
    }
}
fn has_tag(tags: &Vec<Tag>, keys: &Vec<String>) -> bool {
    for t in tags {
        if keys.contains(&t.key) {
            return true;
        }
    }
    return false;
}

impl<T> AddParentTag<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
{
    pub fn new(out: Box<T>, style: Arc<GeometryStyle>) -> AddParentTag<T> {
        
        let mut nk = BTreeSet::new();
        for (_,sp) in &style.parent_tags {
            for k in &sp.node_keys {
                nk.insert(k.clone());
            }
        }
        
        AddParentTag{out,style,pending: BTreeMap::new(), pending_tiles: BTreeMap::new(), node_keys: nk, total_added:0, tags_added:0, finished:0, tm:0.0}
    }
    
    fn node_has_tag(&self, n: &Node) -> bool {
        for t in &n.tags {
            if self.node_keys.contains(&t.key) {
                return true;
            }
        }
        return false;
    }
        
        
    
    fn has_node(&self, w: &Way) -> bool {
        for r in &w.refs {
            if self.pending.contains_key(r) {
                return true;
            }
        }
        return false;
    }
    
      
        
    
    fn process_way(&mut self, w: &Way) {
        for (k, sp) in &self.style.parent_tags {
            
            match find_tag(&w.tags, &sp.way_key) {
                None => {},
                Some(v) => {
                    match get_prio(sp, v) {
                    
                        None => {},
                        Some(p) => {
                            
                            for r in &w.refs {
                                match self.pending.get_mut(r) {
                                    None => {},
                                    Some((n,tx)) => {
                                        if has_tag(&n.tags, &sp.node_keys) {
                                            match tx.get_mut(k) {
                                                None => { tx.insert(k.clone(), (v.clone(),p)); },
                                                Some(vp) => {
                                                    if p > vp.1 {
                                                        vp.0 = v.clone();
                                                        vp.1 = p;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    fn process_block(&mut self, bl: &mut WorkingBlock) -> Option<WorkingBlock> {
        if bl.geometry_block.quadtree.as_int() < 0 {
            return None;
        }
            
        let mut nn = Vec::new();
        let mut tt = Vec::new();
        for n in std::mem::take(&mut bl.pending_nodes) {
            
            if self.node_has_tag(&n) {
                tt.push(n.id);
                self.pending.insert(n.id,(n,BTreeMap::new()));
                self.total_added+=1;
            } else {
                nn.push(n);
            }
        }
        if !tt.is_empty() {
            self.pending_tiles.insert(bl.geometry_block.quadtree.clone(), tt);
        }
        bl.pending_nodes = nn;
        
        
        for (w,_,_) in bl.pending_ways.iter_mut() {
            if self.has_node(w) {
                self.process_way(w);
            }
        }
        
        let mut ff = WorkingBlock::new(0,Quadtree::empty(),0);
        
        let mut removes = Vec::with_capacity(self.pending_tiles.len());
        for (t,_) in self.pending_tiles.iter() {
            if !t.is_parent(&bl.geometry_block.quadtree) {
                removes.push(t.clone());
                
            }
        }
        for t in removes {
            for i in self.pending_tiles.remove(&t).unwrap() {
                match self.pending.remove(&i) {
                    None => {},
                    Some((mut n,t)) => {
                        if !t.is_empty() {
                            self.tags_added+=1;
                        }
                        for (x,(y,_z)) in t {
                            n.tags.push(Tag::new(x,y));
                        }
                        self.finished+=1;
                        ff.pending_nodes.push(n);
                    }
                }
            }
        }
        if !ff.pending_nodes.is_empty() {
            return Some(ff);
        }
        None
        
    }
    
    fn finish_all(&mut self) -> Option<WorkingBlock> {
        
        let mut ff = WorkingBlock::new(0,Quadtree::empty(),0);
        
        for (_,(mut n, t)) in std::mem::take(&mut self.pending) {
            if !t.is_empty() {
                self.tags_added+=1;
            }
            for (x,(y,_z)) in t {
                n.tags.push(Tag::new(x,y));
            }
            self.finished+=1;
            ff.pending_nodes.push(n);
            
        }
        
        if !ff.pending_nodes.is_empty() {
            return Some(ff);
        }
        None
    }
}

impl<T> CallFinish for AddParentTag<T>
    where T: CallFinish<CallType=WorkingBlock, ReturnType=Timings>
{
    type CallType=WorkingBlock;
    type ReturnType=Timings;

    fn call(&mut self, mut bl: WorkingBlock) {
        let tx=ThreadTimer::new();
        let x = self.process_block(&mut bl);
        self.tm+=tx.since();
        self.out.call(bl);
        match x {
            None => {},
            Some(b) =>  {self.out.call(b); },
        }
    }
    
    fn finish(&mut self) -> std::io::Result<Timings> {
        let tx=ThreadTimer::new();
        let ff=self.finish_all();
        let fx=tx.since();
        match ff {
            None => {},
            Some(b) => {self.out.call(b); },
        }
        let mut tms = self.out.finish()?;
        tms.add("AddParent::call", self.tm);
        tms.add("AddParent::finish", fx);
        tms.add_other("AddParent", OtherData::Messages(vec![format!("{} nodes kept, {} with tags added, {} finished", self.total_added,self.tags_added, self.finished)]));
        Ok(tms)
    }
}
        
    
        
