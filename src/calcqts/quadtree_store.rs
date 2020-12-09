
use crate::elements::Quadtree;
use std::collections::BTreeMap;
use std::fmt;


pub const WAY_SPLIT_SHIFT:i64 = 20;

pub const WAY_SPLIT_VAL:usize = (1<<WAY_SPLIT_SHIFT) as usize;
pub const WAY_SPLIT_MASK:i64 = (1<<WAY_SPLIT_SHIFT) - 1;

               
pub trait QuadtreeGetSet: Sync + Send + 'static
{
   
    fn has_value(&self, i: i64) -> bool;
    fn get(&self, i: i64) -> Option<Quadtree>;
    fn set(&mut self, i: i64, q: Quadtree);
    fn items(&self) -> Box<dyn Iterator<Item=(i64,Quadtree)> + '_>;
    fn len(&self) -> usize;
}


pub struct QuadtreeSimple(BTreeMap<i64,Quadtree>);

impl QuadtreeSimple {
    
    
    pub fn new() -> QuadtreeSimple {
        QuadtreeSimple(BTreeMap::new())
    }
    
    pub fn from_values(vals: BTreeMap<i64,Quadtree>) -> QuadtreeSimple {
        QuadtreeSimple(vals)
    }
    
    pub fn len(&self) -> usize {
        self.0.len()
    }
    
    
   
    pub fn expand(&mut self, i: i64, q: Quadtree) {
        
        match self.0.get_mut(&i) {
            None => { self.0.insert(i,q); },
            Some(qx) => { *qx = q.common(&qx); }
        }
    }
    pub fn expand_if_present(&mut self, i: i64, q: &Quadtree) {
        
        match self.0.get_mut(&i) {
            None => { },
            Some(qx) => { *qx = q.common(&qx); }
        }
    }
    
    
}

impl<'a> QuadtreeGetSet for QuadtreeSimple {
    
    
    fn get(&self, r: i64) -> Option<Quadtree> {
        match self.0.get(&r) {
            Some(q) => Some(q.clone()),
            None => None
        }
        
    }
    fn len(&self) -> usize {
        self.0.len()
    }
    fn has_value(&self, i: i64) -> bool {
        self.0.contains_key(&i)
    }
    
    fn set(&mut self, i: i64, q: Quadtree) {
        self.0.insert(i, q);
    }
    fn items(&self) -> Box<dyn Iterator<Item=(i64,Quadtree)> + '_> {
        Box::new(self.0.iter().map(|(a,b)| { (*a,b.clone()) }))
    }
    
}
pub struct QuadtreeTileInt {
    pub off: i64,
    pub count: usize,
    valsa: Vec<u32>,
    valsb: Vec<u16>,
}

fn unpack_val(a: u32, b: u16) -> i64 {
    if (b&1)==0 { return -1; }
    let mut v = a as i64;
    v <<= 8;
    v += (b >> 8) as i64;
    v <<= 23;
    v += ((b >> 1) & 127) as i64;
    return v;
}

fn pack_val(v: i64) -> (u32, u16) {
    if v<0 {
        return (0,0);
    }
    let a = ((v>>31) & 0xffffffff) as u32;
    let mut b:u16 = ((((v>>23) & 0xffff))<<8) as u16;
    b += ((v & 127) << 1) as u16;
    b += 1;
    return (a,b);
}

impl QuadtreeTileInt {
    pub fn new(off: i64) -> QuadtreeTileInt {
        QuadtreeTileInt{off: off, valsa: vec![0u32; WAY_SPLIT_VAL], valsb: vec![0u16; WAY_SPLIT_VAL], count: 0}
    }


    pub fn has_value(&self, i: usize) -> bool {
        
        (self.valsb[i]&1)==1
    }
        
        
    pub fn get(&self, i: usize) -> i64 {
        unpack_val(self.valsa[i], self.valsb[i])
        
    }
    
    
    pub fn set(&mut self, i:usize, v: i64) -> bool {
        let newv = !self.has_value(i);
        let (a,b)=pack_val(v);
        self.valsa[i]=a;
        self.valsb[i]=b;
        
        if newv {
            self.count+=1;
        }
        newv
    }
    
    pub fn iter(&self) -> impl Iterator<Item=(i64,Quadtree)> + '_{
        let o = self.off;
        self.valsa.iter().zip(&self.valsb).enumerate().map(move |(i,(a,b))| ( o+(i as i64), Quadtree::new(unpack_val(*a,*b)))).filter(|(_,v)| { v.as_int()>=0 })
    }
         
        
        
    
}


pub struct QuadtreeSplit {
    tiles: BTreeMap<i64, Box<QuadtreeTileInt>>,
    count: usize
}

impl QuadtreeSplit {
    pub fn new() -> QuadtreeSplit {
        QuadtreeSplit{tiles: BTreeMap::new(),count: 0}
    }
    pub fn add_tile(&mut self, t: Box<QuadtreeTileInt>) {
        self.count+=t.count;
        let ti = t.off>>WAY_SPLIT_SHIFT;
        
        self.tiles.insert(ti, t);
        
    }
}
impl fmt::Display for QuadtreeSplit {
    fn fmt(&self, f:&mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QuadtreeSplit: {} objs in {} tiles", self.len(), self.tiles.len())
    }
}

impl QuadtreeGetSet for QuadtreeSplit {
    
    fn has_value(&self, id: i64) -> bool {
        let idt = id>>WAY_SPLIT_SHIFT;
        let idi = (id & WAY_SPLIT_MASK) as usize;
        
        if !self.tiles.contains_key(&idt) {
            return false;
        }
        self.tiles[&idt].has_value(idi)
    }
    
    fn set(&mut self, id: i64, qt: Quadtree) {
        
        let idt = id>>WAY_SPLIT_SHIFT;
        let idi = (id & WAY_SPLIT_MASK) as usize;
        
        
        if !self.tiles.contains_key(&idt) {
            self.tiles.insert(idt,Box::new(QuadtreeTileInt::new(idt<<WAY_SPLIT_SHIFT)));
        }
        if self.tiles.get_mut(&idt).unwrap().set(idi, qt.as_int()) { //x, y, z) {
            self.count+=1;
        }
        
        
    }
    
    fn get(&self, id: i64) -> Option<Quadtree> {
        let idt = id>>WAY_SPLIT_SHIFT;
        let idi = (id & WAY_SPLIT_MASK) as usize;
        
        if !self.tiles.contains_key(&idt) {
            return None;
        }
        let t = self.tiles.get(&idt).unwrap();
        if !t.has_value(idi) {
            return None;
        }
        Some(Quadtree::new(t.get(idi)))
        
    }
    
    fn len(&self) -> usize {
        self.count
    }
    
    fn items(&self) -> Box<dyn Iterator<Item=(i64,Quadtree)> + '_> {
        Box::new(self.tiles.iter().flat_map(|(_,x)| { x.iter() }))//.map(|(a,b)| { (a as i64, Quadtree::new(b)) }))
    }
    
}

