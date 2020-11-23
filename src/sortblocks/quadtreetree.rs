use std::fmt;
use std::iter::Iterator;
use std::io;
use std::io::Write;

mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::elements::Quadtree;
use osmquadtree::utils::Checktime;

#[derive(Copy, Clone)]
pub struct QuadtreeTreeItem { 
    pub qt: Quadtree,
    pub parent: u32,
    pub weight: u32,
    pub total: i64,
    pub children: [u32; 4]
}
impl QuadtreeTreeItem {
    pub fn root() -> QuadtreeTreeItem {
        QuadtreeTreeItem { qt: Quadtree::new(0), parent: u32::MAX, total: 0, weight: 0, children: [u32::MAX,u32::MAX,u32::MAX,u32::MAX] }
    }
    
    pub fn new(qt: Quadtree, parent: u32) -> QuadtreeTreeItem {
        QuadtreeTreeItem { qt: qt, parent: parent, total: 0, weight: 0, children: [u32::MAX,u32::MAX,u32::MAX,u32::MAX] }
    }
    
    
}

impl fmt::Display for QuadtreeTreeItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut nc = 0;
        for c in &self.children {
            if *c!=u32::MAX {
                nc+=1
            }
        }
        write!(f, "{:19}: {:10} total {:10} weight {} children", self.qt.as_string(), self.total, self.weight, nc)
        //write!(f, "{:19}: {:10} total {:10} weight [{} {:?}]", self.qt.as_string(), self.total, self.weight, self.parent,self.children)
    }
}



pub struct QuadtreeTree {
    items: Vec<QuadtreeTreeItem>,
    count: usize
}
impl fmt::Display for QuadtreeTree { 
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f,"QuadtreeTree [{} total, {} // {} items]", self.total_weight(), self.count, self.items.len())
    }
}

impl<'a> QuadtreeTree {
    pub fn new() -> QuadtreeTree { 
        let mut items = Vec::with_capacity(1000000);
        items.push(QuadtreeTreeItem::root());
        QuadtreeTree{items: items,count: 0}
    }
    
    pub fn iter(&self) -> QuadtreeTreeIter {
        
        QuadtreeTreeIter::new(&self, 0)
    }
    
    pub fn clone(&self) -> QuadtreeTree {
        QuadtreeTree{items: self.items.iter().map(|x| {*x}).collect(), count: self.count}
    }
    
    
    pub fn total_weight(&self) -> i64 {
        self.items[0].total
    }
    
    pub fn len(&self) -> usize {
        self.count
    }
    
    fn find_int(&'a self, qt: Quadtree) -> (usize, &'a QuadtreeTreeItem) {
        if qt.as_int()<0 {
            panic!("can't find neg qt");
        }
        
        let mut i=0;
        let mut t = self.items.get(i).unwrap();
        
        for j in 0..qt.depth() {
            let v = qt.quad(j);
            if t.children[v]==u32::MAX {
                return (i,t);
            }
            i=t.children[v] as usize;
            t = self.items.get(i).unwrap();
        }
        (i,t)
    }
    
    pub fn find(&'a self, qt: Quadtree) -> (usize, &'a QuadtreeTreeItem) {
        let (mut i,mut t) = self.find_int(qt);
        
        loop {
            if t.weight>0 {
                return (i,t)
            }
            if t.parent == u32::MAX {
                return (i,t)
            }
            i=t.parent as usize;
            t=self.items.get(i).unwrap();
        }
    }
        
         
    pub fn remove(&mut self, qt: Quadtree) -> i64 {
        let (i,tx) = self.find_int(qt);
        let w = tx.total;
        
        let mut t = self.items.get_mut(i).unwrap();
        t.weight=0;
        t.total=0;
        t.children=[u32::MAX,u32::MAX,u32::MAX,u32::MAX];
        
        if t.parent != u32::MAX {
            let mut tp = t.parent as usize;
            t = self.items.get_mut(tp).unwrap();
                
            for j in 0..4 {
                if t.children[j]==i as u32 {
                    t.children[j] = u32::MAX;
                }
            }
            
            t.total -= w;

            while t.parent!=u32::MAX {
                tp=t.parent as usize;
                t = self.items.get_mut(tp).unwrap();
                t.total -= w;
                
            }
            
            if t.qt.as_int()!=0 {
                panic!("??");
            }
        }
        w
    }
        
        
    
    pub fn add(&'a mut self, qt: Quadtree, w: u32) -> &'a QuadtreeTreeItem {
        if qt.as_int()<0 {
            panic!("can't find neg qt");
        }
        let w2 = w as i64;
        let mut ti=0;
        for i in 0..qt.depth() {
            self.items[ti].total+=w2;
            
            let v=qt.quad(i);
            if self.items[ti].children[v] == u32::MAX {
                let n = self.items.len() as u32;
                
                if self.items.capacity() == self.items.len() {
                    self.items.reserve(self.items.len()+1000000);
                }

                self.items.push(QuadtreeTreeItem::new(qt.round(i+1), ti as u32));
                self.items[ti].children[v] = n;
            }
            
            ti = self.items[ti].children[v] as usize;
        }
        let t = self.items.get_mut(ti).unwrap();
        if w>0 && t.weight==0 {
            self.count+=1;
        }
        t.weight += w;
        t.total += w2;
        t
    }
    
    pub fn at(&'a self, i: u32) -> &'a QuadtreeTreeItem {
        self.items.get(i as usize).unwrap()
    }
    
    pub fn next(&self, i: u32) -> u32 {
        
        next_item(&self.items, self.at(i), i as usize, 0) as u32
    }
    
    pub fn next_sibling(&self, i: u32) -> u32 {
        
        next_sibling(&self.items, self.at(i), i as usize) as u32
    }
    
}

pub struct QuadtreeTreeIter<'a> {
    tree: &'a QuadtreeTree,
    curr: usize,
    first:bool
    
    
}

impl<'a> QuadtreeTreeIter<'a> {
    pub fn new(tree: &'a QuadtreeTree, curr: usize) -> QuadtreeTreeIter<'a> {
        QuadtreeTreeIter{tree:tree,curr:curr,first:true}
    }

}

pub fn next_item(items: &Vec<QuadtreeTreeItem>, t: &QuadtreeTreeItem, ti: usize, li: usize) -> usize {
    
    if li < 4 {
        for i in li..4 {
            if t.children[i] != u32::MAX {
                return t.children[i] as usize;
            }
        }
    }
    next_sibling(items, t, ti)
}

fn next_sibling(items: &Vec<QuadtreeTreeItem>, t: &QuadtreeTreeItem, ti: usize) -> usize {
    
    if t.parent == u32::MAX {
        return u32::MAX as usize;
    }
    
    let p = items.get((t.parent) as usize).unwrap();
    let ni = ||->usize {
        for i in 0..4 {
            if p.children[i] == ti as u32{
                return i+1;
            }
        }
        panic!("should have found child");
    }();
    if ni == 4 {
        return next_sibling(items, p, t.parent as usize);
    }
    next_item(items, p, t.parent as usize, ni)
}


impl<'a> Iterator for QuadtreeTreeIter<'a> {
    type Item = (usize,&'a QuadtreeTreeItem);
    
    fn next(&mut self) -> Option<(usize,&'a QuadtreeTreeItem)> {
        if self.curr==u32::MAX as usize {
            return None;
        }
        
        let mut t = self.tree.items.get(self.curr).unwrap();
        
        self.curr = if self.first {
            self.first=false;
            0
        } else {
            next_item(&self.tree.items, t, self.curr, 0)
        };
        
        if self.curr==u32::MAX as usize {
            return None;
        }
        
        t = self.tree.items.get(self.curr).unwrap();
        if t.weight == 0 {
            return self.next();
        }
                
        Some((self.curr,t))
    }
}
        

fn all_children_small(tree: &QuadtreeTree, tile: &QuadtreeTreeItem, mintarget: i64) -> bool {
    for c in &tile.children {
        if *c != u32::MAX {
            if tree.at(*c).total as i64 > mintarget {
                return false;
            }
        }
    }
    true
}

fn find_within(tree: &QuadtreeTree, mintarget: i64, maxtarget: i64, absmintarget: i64) -> Vec<(Quadtree, i64)> {
    let mut res = Vec::new();
    if (tree.total_weight()) < mintarget {
        res.push((Quadtree::new(0),tree.total_weight()));
        return res;
    }
    
    let mut t = 0;
    
    loop {
        match t {
            u32::MAX => { return res; },
            i => {
                let tx = tree.at(i);
                if (tx.total) < mintarget {
                    t = tree.next_sibling(i);
                } else if tx.weight>0 && (tx.total) <= maxtarget {
                    res.push((tx.qt.clone(), tx.total));
                    t = tree.next_sibling(i);
                } else if tx.weight>0 && tx.total==(tx.weight as i64) {
                    res.push((tx.qt.clone(), tx.total));
                    t = tree.next_sibling(i);
                } else if tx.weight>0 && all_children_small(tree, tx, absmintarget) {
                    res.push((tx.qt.clone(), tx.total));
                    t = tree.next_sibling(i);
                } else {
                    t = tree.next(i);
                }
            }
        }
    }
}

    

    
pub fn find_tree_groups(mut tree: Box<QuadtreeTree>, target: i64, absmintarget: i64) -> io::Result<Box<QuadtreeTree>> {
    
    let mut res = Box::new(QuadtreeTree::new());
    
    let mut mintarget = target-50;
    let mut maxtarget = target+50;
    let mut ct=Checktime::new();
    let mut all = Vec::new();
    while tree.total_weight() > 0 {
        let vv = find_within(&tree, mintarget, maxtarget,absmintarget);
            
        if vv.is_empty() {
            
            mintarget = i64::max(absmintarget, mintarget-50);
            maxtarget += 50;
            
        } else {
            let vvl=vv.len();
            for (a,_) in vv { 
                let b = tree.remove(a.clone());
                
                all.push((a,b));
                //res.add(a,b);
            }
            match ct.checktime() {
                Some(d) => {
                    print!("\r{:5.1}s: add {} [{}/{}], tree: {}, result: {}", d, vvl, mintarget,maxtarget,tree, res);
                    io::stdout().flush().expect("");
                },
                None=>{}
            }
        }
    }
    all.sort();
    for (a,b) in all {
        if b >= (u32::MAX as i64) {
            println!("add {} {}??", a, b);
        }
        res.add(a,b as u32);
    }
    
    println!("");
    println!("{:5.1}s: [{}/{}], tree: {}, result: {}", ct.gettime(), mintarget,maxtarget,tree, res);
    Ok(res)
}
    
