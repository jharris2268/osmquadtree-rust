extern crate osmquadtree;

use std::fs::File;
use std::io;
use std::io::{Write,Seek,SeekFrom,Read,BufReader,ErrorKind};
use std::fmt;
use std::collections::BTreeMap;
use std::thread;
use std::sync::{mpsc,Arc, Mutex};
use std::env;


use osmquadtree::read_file_block;
use osmquadtree::read_file_block::{FileBlock};
use osmquadtree::read_pbf;
use osmquadtree::write_pbf;
//use osmquadtree::header_block;


use osmquadtree::elements::{MinimalNode,MinimalBlock,QuadtreeBlock};
use osmquadtree::callback::{CallFinish, Callback,CallbackSync,CallbackMerge};
use osmquadtree::elements::{Bbox,Quadtree};

use osmquadtree::stringutils::{StringUtils};
use osmquadtree::utils::{Checktime,Timer,MergeTimings,ReplaceNoneWithTimings,CallAll};

pub enum OtherData {
    PackedWayNodes(Vec<(i64,Vec<Box<Vec<u8>>>)>),
    RelMems(RelMems),
    QuadtreeSimple(Box<QuadtreeSimple>),
    QuadtreeGetSet(Box<dyn QuadtreeGetSet>),
    NumTiles(usize),
    WriteQuadTree(Box<WriteQuadTree>),
    FileLen(u64),
}

pub type Timings = osmquadtree::utils::Timings<OtherData>;


struct WayNodeTile {
    key: i64,
    pub vals: Vec<(i64,i64)>
}
    
impl WayNodeTile {
    pub fn new(key: i64, capacity: usize) -> WayNodeTile {
        WayNodeTile{key: key, vals: Vec::with_capacity(capacity)}
        
    }
    pub fn tile_key(&self) -> i64 { self.key }
    
    pub fn add(&mut self, n: i64, w: i64) {
        self.vals.push((n,w));
    }
    
    pub fn sort(&mut self) {
        self.vals.sort();
    }
    pub fn clear(&mut self) {
        self.vals.clear();
    }
    pub fn len(&self) -> i64 {
        self.vals.len() as i64
    }
    
    pub fn at(&self, mut idx: i64) -> (i64, i64) {
        if idx < 0 {
            idx+=self.len() as i64;
        }
        self.vals[idx as usize]
    }
    /*pub fn iter(&self) -> impl Iterator<Item=&(i64,i64)> {
        self.vals.iter()
    }*/
    
    
    pub fn pack(&self) -> Vec<u8> {
        let nn = write_pbf::pack_delta_int_ref(self.vals.iter().map(|(n,_w)| { n }));
        let ww = write_pbf::pack_delta_int_ref(self.vals.iter().map(|(_n,w)| { w }));
        
        let mut l = 0;
        l+=write_pbf::value_length(1, write_pbf::zig_zag(self.key));
        l+=write_pbf::value_length(2, self.vals.len() as u64);
        l+=write_pbf::data_length(3, nn.len());
        l+=write_pbf::data_length(4, ww.len());
        
        let mut res = Vec::with_capacity(l);
        
        write_pbf::pack_value(&mut res, 1, write_pbf::zig_zag(self.key));
        write_pbf::pack_value(&mut res, 2, self.vals.len() as u64);
        write_pbf::pack_data(&mut res, 3, &nn[..]);
        write_pbf::pack_data(&mut res, 4, &ww[..]);
        
        return res;
    }
    
    pub fn unpack(&mut self, data: &Vec<u8>, minw: i64, maxw: i64) -> Result<usize, io::Error> {
        
        let ti = self.vals.len();
        let mut nv=Vec::new();
        let mut wv=Vec::new();
        for tg in read_pbf::IterTags::new(&data[..], 0) {
            match tg {
                read_pbf::PbfTag::Value(1, k) => {
                    if read_pbf::un_zig_zag(k) != self.key {
                        return Err(io::Error::new(ErrorKind::Other,"wrong key"));
                    }
                },
                read_pbf::PbfTag::Value(2, l) => {
                    nv.reserve(l as usize);
                    wv.reserve(l as usize);
                    self.vals.reserve(l as usize + ti);
                }
                    
                read_pbf::PbfTag::Data(3, nn) => {
                    nv.extend(read_pbf::DeltaPackedInt::new(&nn));
                    
                },
                
                read_pbf::PbfTag::Data(4, ww) => {
                    wv.extend(read_pbf::DeltaPackedInt::new(&ww));
                    
                },
                _ => { return Err(io::Error::new(ErrorKind::Other,"unexpected tag")); }
            };
        }
        
        if minw>0 || maxw > 0 {
            self.vals.extend(nv.iter().zip(wv).filter(|(_,b)| { b>&minw && (maxw==0 || b<=&maxw) }).map(|(a,b)| { (*a,b) }));
        } else {
            self.vals.extend(nv.iter().zip(wv).map(|(a,b)| {(*a,b)}));
        }
        
        Ok(self.vals.len()-ti)
    }
        
}

impl IntoIterator for WayNodeTile {
    type Item = (i64,i64);
    type IntoIter = std::vec::IntoIter<Self::Item>;
    
    fn into_iter(self) -> Self::IntoIter {
        self.vals.into_iter()
    }
}

impl fmt::Display for WayNodeTile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.len() == 0 {
            return write!(f, "Tile {}: empty", self.tile_key());
        } else {
            return write!(f, "Tile {}: {} way nodes ({}, {}) to ({}, {})", self.tile_key(), self.len(), self.at(0).0, self.at(0).1, self.at(-1).0, self.at(-1).1)
        }
    }
}

/*
struct WayNodeTile32 {
    np: i64,
    key: i64,
    pub vals: Vec<(u32,u32)>
}
    
impl WayNodeTile32 {
    pub fn new(np: i64, key: i64, capacity: usize) -> WayNodeTile32 {
        WayNodeTile32{np: np, key: key, vals: Vec::with_capacity(capacity)}
        
    }
    pub fn tile_key(&self) -> i64 { self.key }
    
    pub fn add(&mut self, mut n: i64, w: i64) {
        if w<0 || w>=(u32::MAX as i64) {
            panic!("way out of range");
        }
        n -= self.key*self.np;
        if n<0 || n>=(u32::MAX as i64) {
            panic!("node out of range");
        }
        self.vals.push((n as u32,w as u32));
    }
    
    pub fn sort(&mut self) {
        self.vals.sort();
    }
    pub fn clear(&mut self) {
        self.vals.clear();
    }
    pub fn len(&self) -> i64 {
        self.vals.len() as i64
    }
    
    pub fn at(&self, mut idx: i64) -> (i64, i64) {
        if idx < 0 {
            idx+=self.len() as i64;
        }
        let (n,w) = self.vals[idx as usize];
        ((n as i64)+self.key*self.np, w as i64)
        
    }
    /*pub fn iter(&self) -> impl Iterator<Item=&(i64,i64)> {
        self.vals.iter()
    }*/
    
    
    pub fn pack(&self) -> Vec<u8> {
        let nn = write_pbf::pack_delta_int(self.vals.iter().map(|(n,_w)| { (*n as i64) +  self.key*self.np}));
        let ww = write_pbf::pack_delta_int(self.vals.iter().map(|(_n,w)| { *w as i64 }));
        
        let mut l = 0;
        l+=write_pbf::value_length(1, write_pbf::zig_zag(self.key));
        l+=write_pbf::value_length(2, self.vals.len() as u64);
        l+=write_pbf::data_length(3, nn.len());
        l+=write_pbf::data_length(4, ww.len());
        
        let mut res = Vec::with_capacity(l);
        
        write_pbf::pack_value(&mut res, 1, write_pbf::zig_zag(self.key));
        write_pbf::pack_value(&mut res, 2, self.vals.len() as u64);
        write_pbf::pack_data(&mut res, 3, &nn[..]);
        write_pbf::pack_data(&mut res, 4, &ww[..]);
        
        return res;
    }
    
    
        
}


impl fmt::Display for WayNodeTile32 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.len() == 0 {
            return write!(f, "Tile {}: empty", self.tile_key());
        } else {
            return write!(f, "Tile {}: {} way nodes ({}, {}) to ({}, {})", self.tile_key(), self.len(), self.at(0).0, self.at(0).1, self.at(-1).0, self.at(-1).1)
        }
    }
} 
*/
struct CollectTilesStore {
    //filename: String,
    vals :Vec<(i64, Vec<Box<Vec<u8>>>)>,
    tm: f64,
}
    
    
impl CollectTilesStore {
    pub fn new() -> CollectTilesStore {
        CollectTilesStore{vals: Vec::new(), tm: 0.0}
    }
    
}

impl CallFinish for CollectTilesStore/*<'_>*/ {
    type CallType=Vec<(i64,Box<Vec<u8>>)>;
    type ReturnType=Timings;
    
    
    fn call(&mut self, p: Self::CallType) {
        //let vals = self.vals.as_mut().unwrap();
        let tt = Timer::new();
        for (qi, qd) in p {
            let qv = qi as usize;
            
            if qv >= self.vals.len()  {
                for i in self.vals.len() .. qv+1 {
                    self.vals.push((i as i64, Vec::new()));
                }
                
            }
            self.vals[qv].1.push(qd);
        }
        self.tm += tt.since();
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        
        let mut tt = Timings::new();
        tt.add("collecttilestore", self.tm);
        tt.add_other("waynodes", OtherData::PackedWayNodes(std::mem::take(&mut self.vals)));
        Ok(tt)
        
    }
}

    
    
pub struct RelMems {
    pub nodes: Vec<(i64,i64)>,
    pub ways: Vec<(i64,i64)>,
    pub relations: Vec<(i64,i64)>,
    pub empty_rels: Vec<i64>
}
impl RelMems {
    pub fn new() -> RelMems {
        RelMems{nodes: Vec::new(),ways: Vec::new(),relations: Vec::new(),empty_rels: Vec::new()}
    }
    pub fn len(&self) -> usize {
        self.nodes.len()+self.ways.len()+self.relations.len()
    }
    pub fn extend(&mut self, other: &RelMems) {
        self.nodes.extend(&other.nodes);
        self.ways.extend(&other.ways);
        self.relations.extend(&other.relations);
        self.empty_rels.extend(&other.empty_rels);
    }
    
    pub fn pack(&self) -> Vec<u8> {
        
        let mut res = Vec::new();
        write_pbf::pack_data(&mut res, 1, &write_pbf::pack_delta_int_ref(self.nodes.iter().map(|(x,_)| {x})));
        write_pbf::pack_data(&mut res, 2, &write_pbf::pack_delta_int_ref(self.nodes.iter().map(|(_,y)| {y})));
        
        write_pbf::pack_data(&mut res, 3, &write_pbf::pack_delta_int_ref(self.ways.iter().map(|(x,_)| {x})));
        write_pbf::pack_data(&mut res, 4, &write_pbf::pack_delta_int_ref(self.ways.iter().map(|(_,y)| {y})));
        
        write_pbf::pack_data(&mut res, 5, &write_pbf::pack_delta_int_ref(self.relations.iter().map(|(x,_)| {x})));
        write_pbf::pack_data(&mut res, 6, &write_pbf::pack_delta_int_ref(self.relations.iter().map(|(_,y)| {y})));
        
        write_pbf::pack_data(&mut res, 7, &write_pbf::pack_delta_int_ref(self.empty_rels.iter().map(|x| { x })));
        
        res
    }
    
    pub fn unpack(&mut self, data: &[u8], load_nodes: bool, load_others: bool) {
        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        let mut d = Vec::new();
        let mut e = Vec::new();
        let mut f = Vec::new();
        
        for t in read_pbf::IterTags::new(data,0) {
            match t {
                read_pbf::PbfTag::Data(1, x) => { if load_nodes { a=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(2, x) => { if load_nodes { b=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(3, x) => { if load_others { c=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(4, x) => { if load_others {d=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(5, x) => { if load_others {e=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(6, x) => { if load_others {f=read_pbf::DeltaPackedInt::new(x).collect(); }},
                read_pbf::PbfTag::Data(7, x) => { if load_others {self.empty_rels.extend(read_pbf::DeltaPackedInt::new(x)); }},
                _ => {}
            }
        }
        self.nodes.extend(a.iter().zip(b).map(|(x,y)|{ (*x,y)}));
        self.ways.extend(c.iter().zip(d).map(|(x,y)|{(*x,y)}));
        self.relations.extend(e.iter().zip(f).map(|(x,y)|{(*x,y)}));
    }
}

impl fmt::Display for RelMems {
    fn fmt(&self, f:&mut fmt::Formatter<'_>)-> fmt::Result {
        write!(f, "RelMems: {} nodes, {} ways, {} rels, {} empty", self.nodes.len(), self.ways.len(), self.relations.len(), self.empty_rels.len())
    }
}

struct PackWayNodes<T> {
    pending: Vec<Box<WayNodeTile>>,
    split: i64,
    limit: usize,
    outcall: Box<T>,
    ct: Option<Checktime>,
    relmems: Option<RelMems>,
    tm: f64,
    a: usize,
    b: u64,
    
}

impl<T> PackWayNodes<T>
    where T: CallFinish<CallType=Vec<(i64,Box<Vec<u8>>)>,ReturnType=Timings>
{
    pub fn new(split: i64, limit: usize, outcall: Box<T>, firsth: bool) -> PackWayNodes<T> {
        let ct = {
            match firsth {
                true => Some(Checktime::new()),
                false => None
            }
        };
        PackWayNodes{pending: Vec::new(), split: split, limit: limit, outcall: outcall, ct: ct, relmems: Some(RelMems::new()),tm:0.0,a:0,b:0}
    }
    
    fn check_tile(&mut self, t: i64) {
        let ts = t as usize;
        if ts >= self.pending.len() {
            for i in self.pending.len()..ts+1 {
                //self.pending.push(Box::new(WayNodeTile32::new(self.split, i as i64, self.limit)));
                self.pending.push(Box::new(WayNodeTile::new(i as i64, self.limit)));
            }
        }
       
    }
    
    fn add(&mut self, n: i64, w: i64) -> Option<(i64,Box<Vec<u8>>)> {
        let t = n/self.split;
        self.check_tile(t);
        
        let tt = self.pending.get_mut(t as usize).unwrap();
        
        tt.add(n,w);
        if tt.len() as usize == self.limit {
            tt.sort();
            let p = tt.pack();
            let mut p2 =Box::new(read_file_block::pack_file_block("WayNodes",&p,true).unwrap());
            p2.shrink_to_fit();
            tt.clear();
            return Some((t, p2));
            
        }
        
        None
    } 
    
    fn add_all(&mut self, idx: usize, fb: FileBlock) -> Vec<(i64,Box<Vec<u8>>)> {
        let mut res=Vec::new();
        
        let fbd = fb.data();
                
        if fb.block_type == "OSMHeader" {
            //let hh = header_block::HeaderBlock::read(fb.pos+fb.len, &fbd, self.fname).unwrap();
            //println!("header_block(bbox: {:?}, writer: {}, features: {:?}, {} index entries)", hh.bbox, hh.writer, hh.features, hh.index.len());
        } else {
            let mb = MinimalBlock::read_parts(idx as i64, fb.pos+fb.len, &fbd, false,false,true,true).expect("failed to read block");
            
            match &mut self.ct {
                Some(ct) => match ct.checktime() {
                    Some(d) => {
                        print!("\rtime {:6.1}s minimal_block(index: {}, pos: {:0.1}mb, quadtree: {:?}, start_date: {}, end_date: {}): {} nodes {} ways {} relations; {} tiles [{} mb], {} written [{} mb]",
                            d, mb.index, (mb.location as f64)/1024.0/1024.0, mb.quadtree, mb.start_date, mb.end_date, mb.nodes.len(), mb.ways.len(), mb.relations.len(),
                            self.pending.len(), (self.pending.len() as f64)*(self.limit as f64)*16.0/1024.0/1024.0, self.a, (self.b as f64)/1024.0/1024.0 );
                        io::stdout().flush().expect("");
                        },
                    None => {}
                },
                None => {}
            }


            for w in mb.ways {
                for n in read_pbf::DeltaPackedInt::new(&w.refs_data) {
                    
                    match self.add(n,w.id) {
                        Some(bl) => {
                            self.a+=1;
                            self.b+=bl.1.len() as u64;
                            res.push(bl);
                        },
                        None => {}
                    };
                }
            }
            let rm = self.relmems.as_mut().unwrap();
            for r in mb.relations {
                if r.refs_data.is_empty() {
                    rm.empty_rels.push(r.id);
                } else {
                    
                    for (rf,ty) in read_pbf::DeltaPackedInt::new(&r.refs_data).zip(read_pbf::PackedInt::new(&r.types_data)) {
                        //let m = ((ty as i64) << 60) | rf;
                        
                        
                        
                        match ty {
                            0 => { rm.nodes.push((r.id,rf)); }
                            1 => { rm.ways.push((r.id,rf)); }
                            2 => { rm.relations.push((r.id,rf)); }
                            _ => {}
                        }
                        //self.relmems.push((r.id,ty as u8,rf));
                    }
                }
            }
                    
            
        }
        res        
        
    }
    
    fn add_remaining(&mut self) -> Vec<(i64,Box<Vec<u8>>)> {
        let mut res=Vec::new();
        let p = std::mem::take(&mut self.pending);
        for mut t in p {
            if t.len()>0 {
                t.sort();
                let p = t.pack();
                let mut p2 = Box::new(read_file_block::pack_file_block("WayNodes",&p,true).unwrap());
                p2.shrink_to_fit();
                self.a+=1; self.b+=p2.len() as u64;
                res.push((t.tile_key(),p2));
            }
        }
        match &self.ct {
            Some(ct) => {
                println!("\ntime {:6.1}s {} tiles [{} mb], {} written [{} mb]",
                    ct.gettime(), self.pending.len(), (self.pending.len() as f64)*(self.limit as f64)*16.0/1024.0/1024.0, self.a, (self.b as f64)/1024.0/1024.0 );
            },
            None => {},
            
        }
        
        res
    }
        
    
}

impl<T> CallFinish for PackWayNodes<T>
    where T: CallFinish<CallType=Vec<(i64,Box<Vec<u8>>)>,ReturnType=Timings>
{
    type CallType=(usize,FileBlock);
    //type ReturnType=(RelMems,T::ReturnType);
    type ReturnType=Timings;
     
    fn call(&mut self, fb: Self::CallType) {
        let tt=Timer::new();
        let pp = self.add_all(fb.0,fb.1);
        self.tm+=tt.since();
        self.outcall.call(pp);
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        
        let tt=Timer::new();
        let pp = self.add_remaining();
        let x=tt.since();
        
        self.outcall.call(pp);
        
        let mut timings = self.outcall.finish()?;
        timings.add("packwaynodes", self.tm);
        timings.add("packwaynodes final", x);
        
        let r=self.relmems.take().unwrap();
        timings.add_other("relmems", OtherData::RelMems(r));
        Ok(timings)
        
        
            
    }
}

fn read_way_node_tiles_vals(pos: &mut u64, tile: i64, vals: Vec<Box<Vec<u8>>>, minw: i64, maxw: i64) -> io::Result<WayNodeTile> {
    
    let mut res = WayNodeTile::new(tile,0);
    
    if vals.is_empty() {
        return Ok(res);
        
    }
    
    for v in vals {
        let fb = read_file_block::unpack_file_block(*pos,&v)?;
        if fb.block_type != "WayNodes" {
            return Err(io::Error::new(ErrorKind::Other, format!("wrong block type {}", fb.block_type)));
        }
        
        res.unpack(&fb.data(), minw, maxw)?;
        *pos += v.len() as u64;
        drop(v);
    }
    res.sort();
    Ok(res)
    
}


fn read_way_node_tiles_vals_send(vals: Vec<(i64, Vec<Box<Vec<u8>>>)>, send: mpsc::SyncSender<WayNodeTile>, minw: i64, maxw: i64) {
    let mut pos: u64=0;
    for (k,vv) in vals {
        let wnt = read_way_node_tiles_vals(&mut pos, k, vv, minw, maxw).unwrap();
        
        send.send(wnt).expect("send failed");
    }
    drop(send);
    
}   

struct ChannelReadWayNodeFlatIter {
    //jh: thread::JoinHandle<()>,
    recv: Arc<Mutex<mpsc::Receiver<WayNodeTile>>>,
    hadfirst:bool,
    curr: Option<WayNodeTile>,
    idx: i64,
    
}

impl ChannelReadWayNodeFlatIter {
    
    
    pub fn filter_vals(waynodevals: Vec<(i64,Vec<Box<Vec<u8>>>)>, minw: i64, maxw: i64) -> ChannelReadWayNodeFlatIter {
        let (s,r) = mpsc::sync_channel(1);
        let rx=Arc::new(Mutex::new(r));
        
        /*let jh =*/ thread::spawn( move || read_way_node_tiles_vals_send(waynodevals, s,minw, maxw));
        ChannelReadWayNodeFlatIter{/*jh:jh,*/recv:rx.clone(),hadfirst:false,curr:None,idx:0}
    }
    
    fn next_wnt(&mut self) {
        match self.recv.lock().unwrap().recv() {
            Ok(wnt) => { 
                self.curr=Some(wnt);
                self.idx=0;
            },
            Err(_) => {
                
                self.curr=None;
            }
        }
        
        
    }
    
}

impl Iterator for ChannelReadWayNodeFlatIter {
    type Item = (i64,i64);
    
    fn next(&mut self) -> Option<(i64,i64)> {
        if !self.hadfirst {
            self.next_wnt();
            self.hadfirst=true;
        }
        
        match &self.curr {
            None => { return None; }
            Some(wnt) => {
                if self.idx == wnt.len() {
                    self.next_wnt();
                    return self.next();
                }
                let r = wnt.at(self.idx);
                self.idx+=1;
                return Some(r);
            }
        }
    }
}
                


pub struct NodeWayNodeComb {
    pub id: i64,
    pub lon: i32,
    pub lat: i32,
    pub ways: Vec<i64>
}


impl NodeWayNodeComb {
    pub fn new(nd: MinimalNode, ways: Vec<i64>) -> NodeWayNodeComb {
        NodeWayNodeComb{id: nd.id, lon: nd.lon, lat: nd.lat, ways: ways}
    }
    pub fn from_id(id: i64) -> NodeWayNodeComb {
        NodeWayNodeComb{id:id,lon:0,lat:0,ways: Vec::new()}
    }
    
}

impl fmt::Display for NodeWayNodeComb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:12} {:10} {:10} {:3} ways", self.id, self.lon, self.lat, self.ways.len())
    }
}



pub struct NodeWayNodeCombTile {
    pub vals: Vec<NodeWayNodeComb>
}

impl NodeWayNodeCombTile {
    pub fn new(vals: Vec<NodeWayNodeComb>) -> NodeWayNodeCombTile {
        NodeWayNodeCombTile{vals}
    }
    
    pub fn pack(&self) -> Vec<u8> {
        let mut res = Vec::new();
        write_pbf::pack_value(&mut res, 1, self.vals.len() as u64);
        write_pbf::pack_data(&mut res, 2, &write_pbf::pack_delta_int(
            self.vals.iter().map(|x| { x.id })));
        write_pbf::pack_data(&mut res, 3, &write_pbf::pack_delta_int(
            self.vals.iter().map(|x| { x.lon as i64 })));
        write_pbf::pack_data(&mut res, 4, &write_pbf::pack_delta_int(
            self.vals.iter().map(|x| { x.lat as i64 })));
        write_pbf::pack_data(&mut res, 5, &write_pbf::pack_int(
            self.vals.iter().map(|x| { x.ways.len() as u64 })));
        write_pbf::pack_data(&mut res, 6, &write_pbf::pack_delta_int_ref(
            self.vals.iter().flat_map(|x| { x.ways.iter() })));
        
        res
    }
    
    pub fn unpack(data: &[u8], minw: i64, maxw: i64) -> NodeWayNodeCombTile {
        let mut res = NodeWayNodeCombTile{vals:Vec::new()};
        
        let mut numw = Vec::new();
        let mut ww = Vec::new();
        for t in read_pbf::IterTags::new(&data,0) {
            match t {
                read_pbf::PbfTag::Value(1, c) => { res.vals.reserve(c as usize); },
                read_pbf::PbfTag::Data(2, x) => { 
                    //read_pbf::DeltaPackedInt::new(x).enumerate().map( |(_,x) | { res.vals.push(NodeWayNodeComb::from_id(x));}).collect(); },
                    for i in read_pbf::DeltaPackedInt::new(x) {
                        res.vals.push(NodeWayNodeComb::from_id(i));
                    }},
                read_pbf::PbfTag::Data(3, x) => {
                    for (i,ln) in  read_pbf::DeltaPackedInt::new(x).enumerate() {
                        res.vals[i].lon = ln as i32;
                    }},
                    
                read_pbf::PbfTag::Data(4, x) => {
                    for (i,lt) in  read_pbf::DeltaPackedInt::new(x).enumerate() {
                        res.vals[i].lat = lt as i32;
                    }},
                read_pbf::PbfTag::Data(5, x) => { numw = read_pbf::read_packed_int(x); },
                read_pbf::PbfTag::Data(6, x) => { ww = read_pbf::read_delta_packed_int(x); },
                _ => {}
            }
        }
        
        let mut s=0;
        if minw==0 && maxw == 0 {
             for (i,r) in res.vals.iter_mut().enumerate() {
                let n = numw[i] as usize;
                r.ways.extend(ww[s .. s+n].iter());
                s+=n;
            }
        } else {
            for (i,r) in res.vals.iter_mut().enumerate() {
                let n = numw[i] as usize;
                r.ways.extend(ww[s .. s+n].iter().filter( |w:&&i64| { **w>= minw && (maxw==0 || **w < maxw) } ));
                s+=n;
            }
        }
        if s!=ww.len() { panic!("gone wrong"); }
        res
    }
            
}

struct CombineNodeWayNodeCB<T,U> {
    waynode: U,
    hadfirst: bool,
    waynode_curr: Option<(i64,i64)>,
    tm: f64,
    combined_cb: Box<T>
}

impl<T,U> CombineNodeWayNodeCB<T,U>
    where
        T: CallFinish<CallType=NodeWayNodeCombTile>,
        U: Iterator<Item=(i64,i64)>
{
    pub fn new(
        waynode: U,
        combined_cb: Box<T>
    ) -> CombineNodeWayNodeCB<T,U> {
        
        let waynode_curr=None;
        let hadfirst=false;
        let tm=0.0;
        CombineNodeWayNodeCB{waynode,hadfirst,waynode_curr, tm,combined_cb}
    }
    
    
}

fn combine_nodes_waynodes<'a, T>(
    waynode_iter: &'a mut T, waynode_curr: &'a mut Option<(i64,i64)>, mb: MinimalBlock) -> NodeWayNodeCombTile
    
    where T: Iterator<Item=(i64,i64)>
    
{
        
    
    let mut res = Vec::with_capacity(mb.nodes.len());
    
    for n in &mb.nodes {
        
        let ways = || -> Vec<i64> {
        
            let mut v = Vec::new();
                
            loop {
                match waynode_curr {
                    None => { return v; }
                    Some((a,b)) => {
                        if *a < n.id {
                            //self.next_waynode();
                            *waynode_curr = waynode_iter.next();
                        } else if *a==n.id {
                            v.push(*b);
                            //self.next_waynode();
                            *waynode_curr = waynode_iter.next();
                        } else {
                            return v;
                        }
                    }
                }
            }
        }();
        res.push(NodeWayNodeComb{id: n.id,lon: n.lon,lat: n.lat,ways: ways});
    }
    NodeWayNodeCombTile::new(res)
}


impl<T,U> CallFinish for CombineNodeWayNodeCB<T,U>
    where
        T: CallFinish<CallType=NodeWayNodeCombTile,ReturnType=Timings>,
        U: Iterator<Item=(i64,i64)> + Sync + Send + 'static
{
        
    type CallType = MinimalBlock;
    type ReturnType = Timings;

    fn call(&mut self, mb: MinimalBlock) {
        let t=Timer::new();
        if !self.hadfirst {
            
            self.waynode_curr = self.waynode.next();
            self.hadfirst=true;
        }
        
        let res = combine_nodes_waynodes(&mut self.waynode, &mut self.waynode_curr, mb);
        self.tm+=t.since();
        if res.vals.len()>0 {
            self.combined_cb.call(res);
        }
        return;
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        let mut t = self.combined_cb.finish()?;
        t.add("combinenodewaynode", self.tm);
        Ok(t)
    }
}


fn make_packwaynodescomb<T: CallFinish<CallType=Vec<u8>,ReturnType=Timings>>(out: Box<T>) -> Box<impl CallFinish<CallType=NodeWayNodeCombTile,ReturnType=Timings>> {
    let conv = Box::new( |n: NodeWayNodeCombTile| { read_file_block::pack_file_block("NodeWayNodes", &n.pack(), true).expect("failed to pack") });
    
    Box::new(CallAll::new(out, "packnodewaycomb", conv))
}
        
        
    
        

pub struct WayBoxesSimple {
    boxes: BTreeMap<i64,Bbox>,
    tm: f64,
    qt_level: usize,
    qt_buffer: f64
}

impl WayBoxesSimple {
    pub fn new(qt_level: usize, qt_buffer: f64) -> WayBoxesSimple {
        WayBoxesSimple{boxes: BTreeMap::new(), tm: 0.0, qt_level: qt_level, qt_buffer:qt_buffer}
    }
    
    pub fn expand(&mut self, w: i64, lon: i32, lat: i32) {
        match self.boxes.get_mut(&w) {
            None => { self.boxes.insert(w, Bbox::new(lon,lat,lon,lat)); }
            Some(nb) => {
                nb.expand(lon,lat);
            }
        }
    }
    
    pub fn calculate(&mut self, maxlevel: usize, buffer: f64) -> Box<QuadtreeSimple> {
        let mut qts = BTreeMap::new();
        for (w,b) in std::mem::take(&mut self.boxes) {
            qts.insert(w, Quadtree::calculate(&b, maxlevel, buffer));
        }
        Box::new(QuadtreeSimple(qts))
    }
    
    pub fn iter(&self) -> impl Iterator<Item=(&i64,&Bbox)> +'_ {
        self.boxes.iter()
    }
}

impl CallFinish for WayBoxesSimple {
    type CallType = NodeWayNodeCombTile;
    type ReturnType = Timings;
    
    fn call(&mut self, v: NodeWayNodeCombTile) {
        let t=Timer::new();
        for n in &v.vals {
            for w in &n.ways {
                self.expand(*w,n.lon,n.lat);
            }
        }
        self.tm+=t.since();
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        let mut t = Timings::new();
        t.add("wayboxessimple", self.tm);
        let tx=Timer::new();
        let r = self.calculate(self.qt_level,self.qt_buffer);
        t.add("calc quadtrees", tx.since());
        t.add_other("quadtrees", OtherData::QuadtreeSimple(r));
        Ok(t)
    }
}

pub struct WayBoxesVec {
    wb: Vec<i32>,
    //wb: Box<[i32;4*1024*1024]>,
    off: usize,
    c: usize
}

impl WayBoxesVec {
    pub fn new(off: usize) -> WayBoxesVec {
        let mut wb: Vec<i32> = Vec::with_capacity(4*1024*1024);        
        for _ in 0..1024*1024 {
            wb.extend(&[2000000000,2000000000,-2000000000,-2000000000]);
        }
        WayBoxesVec{wb: wb, off: off, c: 0}
    }
    
    pub fn expand(&mut self, i: usize, lon: i32, lat: i32) {
        
        if self.wb[4*i] > 1800000000 {
            self.c+=1;
        }
        if lon < self.wb[4*i + 0] { self.wb[4*i + 0] = lon; }
        if lat < self.wb[4*i + 1] { self.wb[4*i + 1] = lat; }
        if lon > self.wb[4*i + 2] { self.wb[4*i + 2] = lon; }
        if lat > self.wb[4*i + 3] { self.wb[4*i + 3] = lat; }
        
    }
    
    pub fn calculate(&self, maxlevel: usize, buffer: f64) -> (usize, Box<QuadtreeBlock>) { 
        
        let mut t = Box::new(QuadtreeBlock::with_capacity(self.c));
        let mut c=0;
        for i in 0..1024*1024 {
            
            if self.wb[4*i + 0] <= 1800000000 {
                let q = Quadtree::calculate_vals(
                    self.wb[4*i + 0],self.wb[4*i + 1],
                    self.wb[4*i + 2],self.wb[4*i + 3],
                    maxlevel, buffer);
                    
                //t.set(i,q.as_int());
                t.add_way((i+self.off) as i64, q);
                c+=1;
            }
        }
        if c!=self.c {
            println!("?? tile {} {} != {}", self.off>>20, self.c, c);
        }
        
        (c,t)
    }
    
    
}

pub struct WayBoxesSplit {
    tiles: BTreeMap<i64, Box<WayBoxesVec>>,
    writeqts: Option<Box<WriteQuadTree>>,
    tm: f64,
    qt_level: usize,
    qt_buffer: f64
}

impl WayBoxesSplit{
    pub fn new(writeqts: Box<WriteQuadTree>, qt_level: usize, qt_buffer: f64) -> WayBoxesSplit {
        WayBoxesSplit{tiles: BTreeMap::new(), writeqts:Some(writeqts), tm: 0.0, qt_level: qt_level, qt_buffer: qt_buffer}
    }
        
        
        
    pub fn expand(&mut self, w: i64, lon: i32, lat: i32) {
        
        let wt = w >> 20;
        let wi = (w & 0xfffff) as usize;
        
        if !self.tiles.contains_key(&wt) {
            
            self.tiles.insert(wt, Box::new(WayBoxesVec::new((wt<<20) as usize)));
            if self.tiles.len()>512 {
                panic!("too many tiles");
            }
        }
        self.tiles.get_mut(&wt).unwrap().expand(wi, lon, lat);
    }
    
    pub fn calculate(&mut self) -> usize {
        let mut r = 0;
        let tt = std::mem::take(&mut self.tiles);
        
        for (_,b) in tt {
            let (c,t) = b.calculate(self.qt_level, self.qt_buffer);
            self.writeqts.as_mut().unwrap().call(t);
            r += c
        }
        r
    }
    
    
}
    
impl CallFinish for WayBoxesSplit {
    type CallType = NodeWayNodeCombTile;
    type ReturnType = Timings;
    
    fn call(&mut self, v: NodeWayNodeCombTile) {
        let tx = Timer::new();
        for n in &v.vals {
            for w in &n.ways {
                self.expand(*w,n.lon,n.lat);
            }
        }
        self.tm+=tx.since();
    
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        let mut t = Timings::new();
        t.add("expand boxes", self.tm);
        
        let tx=Timer::new();
        let nt = self.calculate();
        t.add("write qts", tx.since());
        t.add_other("tiles written", OtherData::NumTiles(nt));
        t.add_other("writeqts", OtherData::WriteQuadTree(self.writeqts.take().unwrap()));
        Ok(t)
    }
}

               
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
    off: usize,
    
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
    pub fn new(off: usize) -> QuadtreeTileInt {
        QuadtreeTileInt{off: off, valsa: vec![0u32; 1024*1024], valsb: vec![0u16; 1024*1024]}
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
        
        newv
    }
    
    pub fn iter(&self) -> impl Iterator<Item=(usize,i64)> + '_{
        let o = self.off;
        self.valsa.iter().zip(&self.valsb).enumerate().map(move |(i,(a,b))| ( o+i, unpack_val(*a,*b))).filter(|(_,v)| { *v>=0 })
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
    pub fn add_tile(&mut self, i: i64, t: Box<QuadtreeTileInt>, c: usize) {
        self.tiles.insert(i, t);
        self.count+=c;
    }
}
impl fmt::Display for QuadtreeSplit {
    fn fmt(&self, f:&mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "QuadtreeSplit: {} objs in {} tiles", self.len(), self.tiles.len())
    }
}

impl QuadtreeGetSet for QuadtreeSplit {
    
    fn has_value(&self, id: i64) -> bool {
        let idt = id>>20;
        let idi = (id & 0xfffff) as usize;
        
        if !self.tiles.contains_key(&idt) {
            return false;
        }
        self.tiles[&idt].has_value(idi)
    }
    
    fn set(&mut self, id: i64, qt: Quadtree) {
        
        let idt = id>>20;
        let idi = (id & 0xfffff) as usize;
        
        if !self.tiles.contains_key(&idt) {
            self.tiles.insert(idt,Box::new(QuadtreeTileInt::new((idt<<20) as usize)));
        }
        if self.tiles.get_mut(&idt).unwrap().set(idi, qt.as_int()) { //x, y, z) {
            self.count+=1;
        }
        
        
    }
    
    fn get(&self, id: i64) -> Option<Quadtree> {
        let idt = id>>20;
        let idi = (id & 0xfffff) as usize;
        
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
        Box::new(self.tiles.iter().flat_map(|(_,x)| { x.iter() }).map(|(a,b)| { (a as i64, Quadtree::new(b)) }))
    }
    
}



fn make_convert_minimal_block<T: CallFinish<CallType=MinimalBlock, ReturnType=Timings>>
    (readnodes: bool, readways: bool, readrelations: bool, t: Box<T>)
        -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>
{
    let cmb = Box::new(move |(i,fb): (usize, FileBlock)| {
        if fb.block_type=="OSMHeader" {
            MinimalBlock::new()
        } else {
            MinimalBlock::read_parts(i as i64, fb.pos, &fb.data(), false, readnodes, readways, readrelations).expect("failed to read block")
        }
    });
    
    Box::new(CallAll::new(t, "convertminimalblock", cmb))
}
        

fn read_all_blocks<T: Sync+Send+'static>(fname: &str, mut out: Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=T>>) -> io::Result<T> {
    
    let file = File::open(fname)?;
    let mut fbuf = BufReader::new(file);
    
    let mut idx=0;
    loop {
        match read_file_block::read_file_block(&mut fbuf) {
            Ok(fb) => { out.call((idx,fb)); },
            Err(_) => { return out.finish(); }
        }
        idx+=1;
    }
}    

fn get_relmems_waynodes(mut tt: Timings) -> (RelMems, Vec<(i64,Vec<Box<Vec<u8>>>)>) {
    let mut r = RelMems::new();
    let mut w = Vec::new();
    
    for (_,b) in std::mem::take(&mut tt.others) {
        match b {
            OtherData::RelMems(rx) => r.extend(&rx),
            OtherData::PackedWayNodes(wx) => w.extend(wx),
            _ => {}
        }
    }
    return (r, w)
}

fn prep_way_nodes(infn: &str, numchan: usize) -> io::Result<(RelMems,Vec<(i64,Vec<Box<Vec<u8>>>)>)>{
    println!("prep_way_nodes({},{})", infn, numchan);
    
    let (split, limit) = (1<<20, 1<<12);

    let tt = match numchan {
        0 =>  {
        
            let ct=Box::new(CollectTilesStore::new());
            
            let pwn = Box::new(PackWayNodes::new(split, limit, ct,true));
            
            read_all_blocks(&infn, pwn)
            
        },
        
        numchan => {
            let ct = Box::new(CollectTilesStore::new());
            let ct_par = CallbackSync::new(ct, numchan);
        
            let mut pwn_par: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
           
            let mut j=0;
            for ctx in ct_par {
                let ctm = Box::new(ReplaceNoneWithTimings::new(ctx));
                let pwn = Box::new(PackWayNodes::new(split, limit, ctm,j==0));
                pwn_par.push(Box::new(Callback::new(pwn)));
                j+=1;
            }
            
            let mm = Box::new(CallbackMerge::new(pwn_par, Box::new(MergeTimings::new())));
            
            read_all_blocks(&infn, mm)
            
        }
    }?;
    
    println!("{}",tt);
    Ok(get_relmems_waynodes(tt))
}


fn write_nodewaynodevals(infn: &str, waynodevals: Vec<(i64,Vec<Box<Vec<u8>>>)>, waynodesfn: &str) -> u64 {
    
    
    
    let wvs = CallbackSync::new(Box::new(WriteFile::new(waynodesfn)),4);
    let mut wvps: Vec<Box<dyn CallFinish<CallType=NodeWayNodeCombTile, ReturnType=Timings>>> = Vec::new();
    for w in wvs {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));
        //wvps.push(Box::new(Callback::new(Box::new(PackWayNodesComb::new(w2)))));
        wvps.push(Box::new(Callback::new(make_packwaynodescomb(w2))));
    }
    
    //let wvpm = Box::new(SplitNodeWayNodes::new(wvps));
    let wvpm = Box::new(CallbackMerge::new(wvps, Box::new(MergeTimings::new())));
    
    let wvp_prog = Box::new(Callback::new(Box::new(WayNodeCombProg::new(wvpm))));
    
    let wn_iter = ChannelReadWayNodeFlatIter::filter_vals(waynodevals, 0, 0);

    
    let combines = CallbackSync::new(Box::new(CombineNodeWayNodeCB::new(wn_iter, wvp_prog)), 4);
    
    //let mut converts: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=Option<u64>>>>= Vec::new();
    let mut converts: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=Timings>>>= Vec::new();
    for c in combines {
        let c2 = Box::new(ReplaceNoneWithTimings::new(c));
        //converts.push(Box::new(Callback::new(Box::new(ConvertMinimalblock::new(true,false,false,c2)))));
        converts.push(Box::new(Callback::new(make_convert_minimal_block(true,false,false,c2))));
    }
    
    //let converts_merge = Box::new(CallbackMerge::new(converts, Box::new(TakeAny::new())));
    let converts_merge = Box::new(CallbackMerge::new(converts, Box::new(MergeTimings::new())));
    
    let t = read_all_blocks(&infn, converts_merge).expect("failed");
    println!("write_nodewaynodevals: {}", t);
    let mut nt=0;
    for (_,b) in t.others {
        match b {
            OtherData::FileLen(n) => nt += n,
            _ => {}
        }
    }
    nt
    
}
    

fn calc_way_quadtrees_simple(infn: &str,  waynodevals: &Vec<(i64,Vec<Box<Vec<u8>>>)>, qt_level: usize, qt_buffer: f64) -> Box<QuadtreeSimple> {
   
    let wn_iter = ChannelReadWayNodeFlatIter::filter_vals(waynodevals.clone(), 0, 0);

    let wb = Box::new(Callback::new(Box::new(WayBoxesSimple::new(qt_level, qt_buffer))));
    let combines = CallbackSync::new(Box::new(CombineNodeWayNodeCB::new(wn_iter, wb)), 4);
    
    let mut converts: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock), ReturnType=Timings>>>= Vec::new();
    for c in combines {
        let c2 = Box::new(ReplaceNoneWithTimings::new(c));
        //converts.push(Box::new(Callback::new(Box::new(ConvertMinimalblock::new(true,false,false,c2)))));
        converts.push(Box::new(Callback::new(make_convert_minimal_block(true,false,false,c2))));
    }
    
    let converts_merge = Box::new(CallbackMerge::new(converts, Box::new(MergeTimings::new())));
    
    let t = read_all_blocks(&infn, converts_merge).expect("failed");
    println!("calc_way_quadtrees_simple {}",t);
    let mut o: Option<Box<QuadtreeSimple>> = None;
    for (_,b) in t.others {
        match b {
            OtherData::QuadtreeSimple(q) => o=Some(q),
            _ => {}
        }
    }
    o.unwrap()
    
}
struct UnpackNodeWayNodeCombTile<T> {
    out: Box<T>,
    minw: i64,
    maxw: i64,
    tm:f64,
}
impl<T> UnpackNodeWayNodeCombTile<T> 
where T: CallFinish<CallType=NodeWayNodeCombTile> {
    pub fn new(out: Box<T>, minw: i64, maxw: i64) -> UnpackNodeWayNodeCombTile<T> {
        let tm= 0.0;
        UnpackNodeWayNodeCombTile{out,minw, maxw,tm}
    }
}

impl<T> CallFinish for UnpackNodeWayNodeCombTile<T> 
where T: CallFinish<CallType=NodeWayNodeCombTile,ReturnType=Timings> {
    type CallType=(usize,FileBlock);
    type ReturnType=Timings;
    
    fn call(&mut self, fb: (usize,FileBlock)) {
        let t = Timer::new();
        if fb.1.block_type == "NodeWayNodes" {
            
            let nn = NodeWayNodeCombTile::unpack(&fb.1.data(), self.minw, self.maxw);
            self.out.call(nn);
        } else {
            self.out.call(NodeWayNodeCombTile::new(Vec::new()));
        }
        self.tm+=t.since();
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        let mut t = self.out.finish()?;
        t.add("unpack nodewaynodes", self.tm);
        Ok(t)
    }
}

fn calc_way_quadtrees_split_part(
    waynodesfn: &str,
    minw: i64, maxw: i64,
    writeqts: Box<WriteQuadTree>,
    qt_level: usize, qt_buffer: f64) -> (usize,Box<WriteQuadTree>)
    
{
    
    let wbs = CallbackSync::new(Box::new(WayBoxesSplit::new(writeqts, qt_level, qt_buffer)),4);    
    let mut conv: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
    for wb in wbs {
        let wb2 = Box::new(ReplaceNoneWithTimings::new(wb));
        conv.push(Box::new(Callback::new(Box::new(UnpackNodeWayNodeCombTile::new(wb2, minw, maxw)))));
    }
    
    let conv_merge = Box::new(CallbackMerge::new(conv, Box::new(MergeTimings::new())));
    let mut t = read_all_blocks(waynodesfn, conv_merge).expect("failed");
    println!("{}", t);
    let mut nb=0;
    let mut wq: Option<Box<WriteQuadTree>> = None;
    for (_,b) in std::mem::take(&mut t.others) {
        match b {
            OtherData::NumTiles(f) => nb+=f,
            OtherData::WriteQuadTree(q) => wq=Some(q),
            _ => {}
        }
    }
    (nb, wq.unwrap())
}

fn calc_way_quadtrees_split(waynodesfn: &str, qt_level: usize, qt_buffer: f64) -> Box<QuadtreeSplit> {
    
    
    let tempfn = format!("{}-wayqts", String::from(waynodesfn).substr(0,waynodesfn.len()-9));
    
    let writeqts = Box::new(WriteQuadTree::new(&tempfn));
    
    let (a,writeqts) = calc_way_quadtrees_split_part(waynodesfn, 0, 350i64 << 20, writeqts, qt_level, qt_buffer);
    
    println!("write {} qts", a);
    
    let (b, writeqts) = calc_way_quadtrees_split_part(waynodesfn, 350i64 << 20, 700i64 << 20, writeqts, qt_level, qt_buffer);
    println!("write {} qts", b);
    let (c,mut writeqts) = calc_way_quadtrees_split_part(waynodesfn, 700i64 << 20, 0, writeqts, qt_level, qt_buffer);
    println!("write {} qts", c);
    writeqts.finish().expect("writeqts.finish() failed");
    
    //Boxx::new(QuadtreeSplit::new())
    load_way_qts(&tempfn)
    
}

fn read_quadtree_block_ways(data: Vec<u8>, res: &mut Box<QuadtreeSplit>) {
    
    for x in read_pbf::IterTags::new(&data,0) {
        match x {
            read_pbf::PbfTag::Data(2,d) => {
                for y in read_pbf::IterTags::new(&d,0) {
                    match y {
                        read_pbf::PbfTag::Data(3, d) => {
                            let mut i=0;
                            let mut q=Quadtree::new(-1);
                            for z in read_pbf::IterTags::new(&d,0) {
                                
                                match z {
                                    read_pbf::PbfTag::Value(1, v) => { i = v as i64; },
                                    read_pbf::PbfTag::Value(20, v) => { q = Quadtree::new(read_pbf::un_zig_zag(v)); }
                                    _ => {}
                                }
                                
                            }
                            res.set(i,q);
                        },
                        _ => {}
                    }
                }
            },
            _ => {}
        }
    }
    
}
    

fn load_way_qts(infn: &str) -> Box<QuadtreeSplit> {
    
    let mut res = Box::new(QuadtreeSplit::new());
    
    let fobj = File::open(&infn).expect("file not present");
    let mut fbuf = BufReader::new(fobj);
    
    for bl in read_file_block::ReadFileBlocks::new(&mut fbuf) {
        if bl.block_type=="OSMData" {
            read_quadtree_block_ways(bl.data(), &mut res);
        }
    }
    res
}
    
        
        
    
    

struct ExpandNodeQuadtree<T> {
    wayqts: Option<Box<dyn QuadtreeGetSet>>,
    nodeqts: Option<Box<QuadtreeSimple>>,
    tm: f64,
    outb: Box<T>,
    curr: Box<QuadtreeBlock>,
    qt_level: usize,
    qt_buffer: f64
}
const NODE_LIMIT: usize = 100000;

impl<T> ExpandNodeQuadtree<T>
    where T: CallFinish<CallType=Vec<Box<QuadtreeBlock>>>
{
    pub fn new(wayqts: Box<dyn QuadtreeGetSet>, nodeqts: Box<QuadtreeSimple>, outb: Box<T>, qt_level: usize, qt_buffer: f64) -> ExpandNodeQuadtree<T> {
        let wayqts=Some(wayqts);
        let nodeqts=Some(nodeqts);
        let tm = 0.0;
        let curr = Box::new(QuadtreeBlock::with_capacity(NODE_LIMIT));
        ExpandNodeQuadtree{wayqts, nodeqts, tm, outb, curr, qt_level, qt_buffer}
    }
}

impl<T> CallFinish for ExpandNodeQuadtree<T> 
    where T: CallFinish<CallType=Vec<Box<QuadtreeBlock>>,ReturnType=Timings>
{
    type CallType = NodeWayNodeCombTile;
    //type ReturnType = (T::ReturnType, Box<dyn QuadtreeGetSet>, QuadtreeSimple);
    type ReturnType = Timings;
    
    fn call(&mut self, nn: NodeWayNodeCombTile) {
        let tx=Timer::new();
        if nn.vals.is_empty() { return; }
        
        //let mut bl = Box::new(QuadtreeBlock::with_capacity(nn.vals.len())); 
        let mut bl = Vec::new();
        for n in nn.vals {
            let q = if n.ways.is_empty() {
                Quadtree::calculate_point(n.lon, n.lat, self.qt_level, self.qt_buffer)
            } else {
                let mut q = Quadtree::new(-1);
                for wi in n.ways {
                    match self.wayqts.as_ref().unwrap().get(wi) {
                        None => {},
                        Some(qi) => { q = q.common(&qi); }
                    }
                }
                q
            };
            self.nodeqts.as_mut().unwrap().expand_if_present(n.id, &q);
            //bl.add_node(n.id,q);
            self.curr.add_node(n.id,q);
            if self.curr.len() >= NODE_LIMIT {
                let p = std::mem::replace(&mut self.curr, Box::new(QuadtreeBlock::with_capacity(NODE_LIMIT)));
                bl.push(p);
            }
            
        }
        self.tm += tx.since();
        self.outb.call(bl);
        
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        
        self.outb.call(vec![std::mem::replace(&mut self.curr, Box::new(QuadtreeBlock::new()))]);
        
        let mut r = self.outb.finish()?;
        r.add("calc node quadtrees", self.tm);
        r.add_other("way_quadtrees", OtherData::QuadtreeGetSet(self.wayqts.take().unwrap()));
        r.add_other("node_quadtrees", OtherData::QuadtreeSimple(self.nodeqts.take().unwrap()));
        Ok(r)
        
    }
}

struct DontFinish {
    t: Option<Box<WriteQuadTree>>
}

impl DontFinish
{
    pub fn new(t: Box<WriteQuadTree>) -> DontFinish {
        DontFinish{t: Some(t)}
    }
}

impl CallFinish for DontFinish
{
    type CallType = <WriteQuadTree as CallFinish>::CallType;
    type ReturnType = Timings;
    
    fn call(&mut self, x: Self::CallType) {
        self.t.as_mut().unwrap().call(x);
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let o = self.t.take().unwrap();
        let mut f = Timings::new();
        f.add_other("writequadtree", OtherData::WriteQuadTree(o));
        
        Ok(f)
    }
}

    
use std::marker::PhantomData;


struct FlattenCF<T, U> {
    out: Box<T>,
    x: PhantomData<U>
}
impl<T, U> FlattenCF<T, U> 
    where T: CallFinish<CallType=U, ReturnType=Timings>,
          U: Sync+Send+'static
{
    pub fn new(out: Box<T>) -> FlattenCF<T,U> {
        FlattenCF{out: out, x: PhantomData}
    }
}

impl<T,U> CallFinish for FlattenCF<T,U>
    where T: CallFinish<CallType=U, ReturnType=Timings>,
          U: Sync+Send+'static
{
    type CallType = Vec<U>;
    type ReturnType = Timings;
    
    fn call(&mut self, us: Vec<U>) {
        for u in us {
            self.out.call(u);
        }
    }
    fn finish(&mut self) -> io::Result<Timings> {
        self.out.finish()
    }
}
    
    
    
fn find_node_quadtrees_flatvec(wqt: Box<WriteQuadTree>, waynodesfn: &str, qts: Box<dyn QuadtreeGetSet>, nqts: Box<QuadtreeSimple>, qt_level: usize, qt_buffer: f64) -> (Box<WriteQuadTree>, Box<dyn QuadtreeGetSet>, Box<QuadtreeSimple>) {
    
    let wqt_wrap = Box::new(DontFinish::new(wqt));
    let wqt_wrap2 = Box::new(FlattenCF::new(wqt_wrap));
    let eqt = CallbackSync::new(Box::new(ExpandNodeQuadtree::new(qts, nqts,wqt_wrap2, qt_level, qt_buffer)),4);
    
    //let mut conv: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Option<(Box<WriteQuadTree>, Box<dyn QuadtreeGetSet>, QuadtreeSimple)>>>> = Vec::new();
    let mut conv: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
    for wb in eqt {
        let wb2=Box::new(ReplaceNoneWithTimings::new(wb));
        conv.push(Box::new(Callback::new(Box::new(UnpackNodeWayNodeCombTile::new(wb2, 0, 0)))));
    }
    
    let conv_merge = Box::new(CallbackMerge::new(conv, Box::new(MergeTimings::new())));
    let mut t = read_all_blocks(waynodesfn, conv_merge).expect("failed");
    println!("find_node_quadtrees_flatvec {}", t);
    let mut a: Option<Box<WriteQuadTree>> = None;
    let mut b: Option<Box<dyn QuadtreeGetSet>>=None;
    let mut c: Option<Box<QuadtreeSimple>>=None;
    for (x,y) in std::mem::take(&mut t.others) {
        match (x.as_str(),y) {
            ("writequadtree", OtherData::WriteQuadTree(wt)) => a=Some(wt),
            ("way_quadtrees", OtherData::QuadtreeGetSet(wq)) => b=Some(wq),
            ("node_quadtrees", OtherData::QuadtreeSimple(nq)) => c=Some(nq),
            _ => {}
        }
    }
    (a.unwrap(), b.unwrap(), c.unwrap())
}
    
fn find_node_quadtrees_simple(wqt: Box<WriteQuadTree>, infn: &str, waynodevals: &Vec<(i64,Vec<Box<Vec<u8>>>)>, qts: Box<dyn QuadtreeGetSet>, nqts: Box<QuadtreeSimple>, qt_level: usize, qt_buffer: f64,) -> (Box<WriteQuadTree>, Box<dyn QuadtreeGetSet>, Box<QuadtreeSimple>) {
    
    let wqt_wrap = Box::new(DontFinish::new(wqt));
    let wqt_wrap2 = Box::new(FlattenCF::new(wqt_wrap));
    let eqt = Box::new(Callback::new(Box::new(ExpandNodeQuadtree::new(qts, nqts,wqt_wrap2, qt_level, qt_buffer))));
    
    let wn_iter = ChannelReadWayNodeFlatIter::filter_vals(waynodevals.clone(),0,0);
    
    let combines = CallbackSync::new(Box::new(CombineNodeWayNodeCB::new(wn_iter, eqt)), 4);
    //let mut converts: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Option<(Box<WriteQuadTree>, Box<dyn QuadtreeGetSet>, QuadtreeSimple)>>>> = Vec::new();
    let mut converts: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
    for c in combines {
        let c2=Box::new(ReplaceNoneWithTimings::new(c));
        //converts.push(Box::new(Callback::new(Box::new(ConvertMinimalblock::new(true,false,false,c2)))));
        converts.push(Box::new(Callback::new(make_convert_minimal_block(true,false,false,c2))));
    }
    
    let conv_merge = Box::new(CallbackMerge::new(converts, Box::new(MergeTimings::new())));
    let mut t = read_all_blocks(infn, conv_merge).expect("failed");
    
    println!("find_node_quadtrees_simple {}", t);
    let mut a: Option<Box<WriteQuadTree>> = None;
    let mut b: Option<Box<dyn QuadtreeGetSet>>=None;
    let mut c: Option<Box<QuadtreeSimple>>=None;
    for (x,y) in std::mem::take(&mut t.others) {
        match (x.as_str(),y) {
            ("writequadtree", OtherData::WriteQuadTree(wt)) => a=Some(wt),
            ("way_quadtrees", OtherData::QuadtreeGetSet(wq)) => b=Some(wq),
            ("node_quadtrees", OtherData::QuadtreeSimple(nq)) => c=Some(nq),
            _ => {}
        }
    }
    (a.unwrap(), b.unwrap(), c.unwrap())
    
}

pub struct WriteFile {
    filename: String,
    openedfile: bool,
    file: Option<File>,
    tm: f64
    
}

impl WriteFile {
    pub fn new(outfn: &str) -> WriteFile {
        WriteFile{filename: outfn.to_owned(), openedfile: false, file: None, tm: 0.0}
    }
    
    fn check_file(&mut self) {
        if !self.openedfile {
            self.file = Some(File::create(&self.filename).expect("failed to create file"));
            self.openedfile=true;
        }
    }
}

impl CallFinish for WriteFile {
    type CallType = Vec<u8>;
    type ReturnType = Timings;
    
    fn call(&mut self, data: Vec<u8>) {
        let t=Timer::new();
        self.check_file();
        self.file.as_mut().unwrap().write_all(&data).expect("failed to write data");
        self.tm+=t.since();
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let p = self.file.as_mut().unwrap().seek(SeekFrom::Current(0))?;
        self.file=None;
        let mut t = Timings::new();
        t.add("writefile", self.tm);
        t.add_other("file len", OtherData::FileLen(p));
        
        Ok(t)
    }
}


struct WriteQuadTreePack<T> {
    out: Box<T>
}
impl<T> WriteQuadTreePack<T>
    where T: CallFinish<CallType=Vec<u8>> + Sync + Send + 'static
{
    pub fn new(out: Box<T>) -> WriteQuadTreePack<T> {
        WriteQuadTreePack{out}
    }
}

impl<T> CallFinish for WriteQuadTreePack<T>
    where T: CallFinish<CallType=Vec<u8>> + Sync + Send + 'static
{
    type CallType=Box<QuadtreeBlock>;
    type ReturnType=T::ReturnType;

    fn call(&mut self, t: Self::CallType) {
        let mut t=t;
        let p = t.pack().expect("failed to pack");
        let b = read_file_block::pack_file_block("OSMData", &p, true).expect("failed to pack");
        
        self.out.call(b);
    }
    
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        self.out.finish()
    }
}


pub struct WriteQuadTree {
    //out: Box<WriteFile>,
    packs: Vec<Box<Callback<Box<QuadtreeBlock>,Timings>>>,
    numwritten: usize,
//    byteswritten: usize,
    ct: Checktime
}

impl WriteQuadTree
{
    pub fn new(outfn: &str) -> WriteQuadTree {
        let outs = CallbackSync::new(Box::new(WriteFile::new(outfn)),4);
        let mut packs = Vec::new();
        for o in outs {
            let o2 = Box::new(ReplaceNoneWithTimings::new(o));
            packs.push(Box::new(Callback::new(Box::new(WriteQuadTreePack::new(o2)))));
        }
        
        let numwritten = 0;
        //let byteswritten = 0;
        let ct = Checktime::new();
        WriteQuadTree{packs, numwritten,ct}
    }
}
        
impl CallFinish for WriteQuadTree
{
    type CallType = Box<QuadtreeBlock>;
    type ReturnType = Timings;
    
    fn call(&mut self, t: Self::CallType) {
        /*let mut t=t;
        let p = t.pack().expect("failed to pack");
        let b = read_file_block::pack_file_block("OSMData", &p, true).expect("failed to pack");*/
        
        let i = self.numwritten % 4;
        self.numwritten += 1;
        //self.byteswritten += b.len();
        
        match self.ct.checktime() {
            None => {},
            Some(d) => {
                println!("{:6.1}s: {} written [{}]", d, self.numwritten,&t);
                //println!("{:6.1}s: {} written, [{} bytes] [{} => {} bytes, {} compressed]", d, self.numwritten, self.byteswritten, &t, p.len(), b.len());
            }
        }
        
        self.packs[i].call(t);
     
    }
    fn finish(&mut self) -> io::Result<Timings> {
        
        let mut r = Timings::new();
        let mut byteswritten=0;
        for p in self.packs.iter_mut() {
            r.combine(p.finish().expect("finish failed"));
        }
        for (_,b) in &r.others {
            match b {
                OtherData::FileLen(f) => byteswritten+=f,
                _ => {},
            }
        }
                
        //let x = self.out.finish()?;
        println!("{:6.1}s: {} written, [{} bytes]", self.ct.gettime(),self.numwritten,byteswritten);
        
        Ok(r)
    }
}
    

struct PackQuadtrees {
    out: Box<dyn CallFinish<CallType=Box<QuadtreeBlock>, ReturnType=Timings>>,
    limit: usize,
    curr: Box<QuadtreeBlock>
    
}

impl PackQuadtrees {
    pub fn new(out: Box<dyn CallFinish<CallType=Box<QuadtreeBlock>, ReturnType=Timings>>, limit: usize) -> PackQuadtrees {
        
        let curr = Box::new(QuadtreeBlock::with_capacity(limit));
        PackQuadtrees{out,limit,curr}
    }
    /*
    pub fn add_node(&mut self, n: i64, q: Quadtree) {
        self.curr.add_node(n,q);
        self.check_pack_and_write();
    }*/
    
    pub fn add_way(&mut self, n: i64, q: Quadtree) {
        self.curr.add_way(n,q);
        self.check_pack_and_write();
    }
    pub fn add_relation(&mut self, n: i64, q: Quadtree) {
        self.curr.add_relation(n,q);
        self.check_pack_and_write();
    }
    pub fn finish(&mut self) {
        self.pack_and_write();
        self.out.finish().expect("out.finish() failed?");
    }
    
    fn check_pack_and_write(&mut self) {
        if self.curr.len() >= self.limit {
            self.pack_and_write();
        }
    }
    
    fn pack_and_write(&mut self) {
        
        let t = std::mem::replace(&mut self.curr, Box::new(QuadtreeBlock::with_capacity(self.limit)));
        
        self.out.call(t);
        
        
    }
        
}

fn calc_quadtrees_simple(infn: &str, waynodevals: Vec<(i64,Vec<Box<Vec<u8>>>)>, outfn: &str, relmems: RelMems, qt_level: usize, qt_buffer: f64) {
    
    let mut nqts = Box::new(QuadtreeSimple::new());
    
    for (_,b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }
    
                
    println!("expecting {} rel nodes qts", nqts.len());
    
    let qts = calc_way_quadtrees_simple(&infn, &waynodevals, qt_level, qt_buffer) as Box<dyn QuadtreeGetSet>;
    println!("have {} way quadtrees", qts.len());
    
    let writeqts = Box::new(WriteQuadTree::new(outfn));
    let (writeqts, qts, nqts) = find_node_quadtrees_simple(writeqts, &infn, &waynodevals, qts, nqts, qt_level, qt_buffer);
    
    
    write_ways_rels(writeqts, qts, nqts, relmems);
}

fn calc_quadtrees_flatvec(infn: &str, waynodevals: Vec<(i64,Vec<Box<Vec<u8>>>)>, outfn: &str, relmems: RelMems,  qt_level: usize, qt_buffer: f64) {
    let relmfn = format!("{}-relmems", &outfn);
    let mut relmfo = File::create(&relmfn).expect("couldn't create locs file");
    relmfo.write_all(&relmems.pack()).expect("failed to write relmems");
    
    let mut nqts = Box::new(QuadtreeSimple::new());
    
    for (_,b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }
    drop(relmems);
    
    let waynodesfn = format!("{}-nodewaynodes", outfn);
    let fz = write_nodewaynodevals(&infn, waynodevals, &waynodesfn);
    println!("wrote {} bytes", fz);
    
    
                
    println!("expecting {} rel nodes qts", nqts.len());
    
    let qts = calc_way_quadtrees_split(&waynodesfn, qt_level, qt_buffer) as Box<dyn QuadtreeGetSet>;
    
    println!("have {} way quadtrees", qts.len());
    
    let writeqts = Box::new(WriteQuadTree::new(outfn));
    let (writeqts, qts, nqts) = find_node_quadtrees_flatvec(writeqts, &waynodesfn, qts, nqts, qt_level, qt_buffer);
    
    let relmems = load_relmems(&relmfn, true, true);
    write_ways_rels(writeqts, qts, nqts, relmems);
}

fn write_ways_rels(writeqts: Box<WriteQuadTree>, qts: Box<dyn QuadtreeGetSet>, nqts: Box<QuadtreeSimple>, relmems: RelMems) {
    
    println!("write {} way qts", qts.len());
    let mut allqts = PackQuadtrees::new(writeqts, 50000);
    for (w,q) in qts.items() {
        allqts.add_way(w,q);
    }
    
    println!("prep rel qts");
    let mut rqts = QuadtreeSimple::new();
    
    for (a,c) in &relmems.nodes {
        match nqts.get(*c) {
            Some(q) => {rqts.expand(*a, q); },
            None => {println!("missing node {}", *c);}
        }
    }
    
    println!("have {} rel qts", rqts.len());
    
    let mut nmw=0;
    for (a,c) in &relmems.ways {
        match qts.get(*c) {
            Some(q) => {rqts.expand(*a, q); },
            None => {
                if nmw < 5 || (nmw % 18451)==0{
                    println!("missing way {}: {} for {}", nmw,*c,*a);
                }
                nmw+=1;
            }
        }
    }
    println!("missing {} ways", nmw);
    println!("have {} rel qts", rqts.len());
    println!("and {} empty rels", relmems.empty_rels.len());
    for r in &relmems.empty_rels {
        rqts.expand(*r, Quadtree::new(0));
    }
    
    println!("and {} rel rels", relmems.relations.len());
    let mut sn=0;
    for i in 0..5 {
        for (a,b) in &relmems.relations {
            match rqts.get(*b) {
                None => {
                    
                    if i==4 {
                        //println!("no rel??");
                        sn+=1;
                        rqts.expand(*a,Quadtree::new(0));
                    }
                },
                Some(q) => {rqts.expand(*a,q); }
            }
        }
    }
    println!("{} missing parent rels?",sn);
    
    println!("have {} rel qts", rqts.len());
    let mut nneg=0;
    for (r,q) in rqts.items() {
        if q.as_int()<0 {
            allqts.add_relation(r, Quadtree::new(0));
            nneg+=1;
        } else {
            allqts.add_relation(r,q);
        }
    }
    println!("replaced {} neg qt rels with 0", nneg);
    allqts.finish();
    
    
}


struct WayNodeCombProg<T> {
    cnt: usize,
    tot: usize,
    ct: Checktime,
    out: Box<T>,
    tm:f64
}

impl<T> WayNodeCombProg<T> {
    pub fn new(out: Box<T>) -> WayNodeCombProg<T> {
        WayNodeCombProg{cnt:0, tot:0,ct: Checktime::new(), out:out,tm:0.0}
    }
}
impl<T> CallFinish for WayNodeCombProg<T>
    where T: CallFinish<CallType=NodeWayNodeCombTile,ReturnType=Timings>
{
    type CallType = NodeWayNodeCombTile;
    //type ReturnType = (T::ReturnType,usize,usize,f64);
    type ReturnType = T::ReturnType;
    
    fn call(&mut self, cc: NodeWayNodeCombTile) {
        let a = self.ct.gettime();
        self.cnt += cc.vals.len();
        self.tot += cc.vals.iter().map(|x| { x.ways.len() }).sum::<usize>();
        match self.ct.checktime() {
            
            Some(d) => {
                let mut a = String::new();
                let mut b = String::new();
                if cc.vals.len()>0 {
                    a = format!("{}", cc.vals[0]);
                    if cc.vals.len()>1 {
                        b = format!("{}", cc.vals[cc.vals.len()-1]);
                    }
                }
                print!("\r{:5.2}s {:10} // {:15} {:5} {} => {}", d,self.cnt, self.tot, cc.vals.len(), a,b);
                io::stdout().flush().expect("");
            }
            None => {}
        }
        self.tm += self.ct.gettime()-a;
        self.out.call(cc);
    }
    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        println!("{:5.2}s {:10} // {:15} Finished", self.ct.gettime(),self.cnt, self.tot);
        let mut t =self.out.finish()?;
        t.add("waynodecomb", self.tm);
        Ok(t)
        
    }
}
  

fn load_relmems(relmfn: &str, load_nodes: bool, load_others: bool) -> RelMems {
    let mut f = File::open(&relmfn).expect("couldn't open relmems file");
            
            
    let mut data = Vec::new();
    f.read_to_end(&mut data).expect("failed to read relmems file");

    let mut relmems = RelMems::new();
    relmems.unpack(&data, load_nodes, load_others);
    drop(data);
    println!("read relmems: {}", relmems);
    relmems
}


fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
    
    let mut numchan = 4;
    let mut outfn = String::new();
    let mut use_simple=false;
    let mut load_existing=false;
    let mut qt_level=17usize;
    let mut qt_buffer=0.05;
    
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("outfn=") {
                outfn = args[i].substr(6,args[i].len());
            } else if args[i] == "use_simple" {
                use_simple = true;
            } else if args[i] == "load_existing" {
                load_existing=true;
            } else if args[i].starts_with("qt_level=") {
                qt_level = args[i].substr(9,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("qt_buffer=") {
                qt_buffer = args[i].substr(10,args[i].len()).parse().unwrap();
            } 
        }
    }
    if outfn.is_empty() {
        outfn = format!("{}-qts.pbf", &fname.substr(0, fname.len()-4));
    }
    
    
    if load_existing {
        
        
        let relmfn = format!("{}-relmems", &outfn);
        let relmems = load_relmems(&relmfn, true, false);
        let mut nqts = Box::new(QuadtreeSimple::new());
    
        for (_,b) in &relmems.nodes {
            nqts.set(*b, Quadtree::new(-1));
        }
        drop(relmems);
        
        let waynodesfn = format!("{}-nodewaynodes", outfn);
                
        println!("expecting {} rel nodes qts", nqts.len());
        
        let qts = calc_way_quadtrees_split(&waynodesfn, qt_level, qt_buffer) as Box<dyn QuadtreeGetSet>;
        
        println!("have {} way quadtrees", qts.len());
        
        let writeqts = Box::new(WriteQuadTree::new(&outfn));
        let (writeqts, qts, nqts) = find_node_quadtrees_flatvec(writeqts, &waynodesfn, qts, nqts, qt_level, qt_buffer);
        
        let relmems = load_relmems(&relmfn, true, true);
        write_ways_rels(writeqts, qts, nqts, relmems); 
        
    } else {
        let (relmems,waynodevals) = prep_way_nodes(&fname,numchan).expect("prep_way_nodes failed");
        if use_simple {
            calc_quadtrees_simple(&fname,waynodevals,&outfn,relmems, qt_level, qt_buffer);
        } else {
            calc_quadtrees_flatvec(&fname,waynodevals,&outfn,relmems, qt_level, qt_buffer);
        }
    }
    
}



