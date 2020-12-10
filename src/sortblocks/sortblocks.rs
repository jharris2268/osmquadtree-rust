use std::fs::File;
use std::io;
use std::io::{Write,BufReader,Seek,SeekFrom};
use std::collections::{HashMap,BTreeMap};

use crate::callback::{Callback,CallbackSync,CallbackMerge,CallFinish};
use crate::elements::{Quadtree,PrimitiveBlock,Node,Way,Relation};

use crate::utils::{Timer,MergeTimings,ReplaceNoneWithTimings};
use crate::pbfformat::read_file_block::{read_all_blocks,FileBlock,read_file_block,unpack_file_block,pack_file_block,ProgBarWrap,read_all_blocks_prog,file_length};
use crate::sortblocks::{QuadtreeTree,Timings,OtherData};
use crate::stringutils::StringUtils;
pub use crate::sortblocks::addquadtree::{AddQuadtree,make_unpackprimblock};
pub use crate::sortblocks::writepbf::{make_packprimblock,WriteFile,make_packprimblock_many};
use crate::pbfformat::header_block::HeaderType;

fn get_block<'a>(blocks: &'a mut HashMap<i64,PrimitiveBlock>, groups: &'a Box<QuadtreeTree>, q: Quadtree) -> &'a mut PrimitiveBlock {
    let (_,b) = groups.find(q);
    let q = b.qt.as_int();
    if !blocks.contains_key(&q) {
        let mut t = PrimitiveBlock::new(0,0);
        t.quadtree = b.qt;
        blocks.insert(q.clone(), t);
    }
    blocks.get_mut(&q).unwrap()
}

struct SortBlocks {
    groups: Option<Box<QuadtreeTree>>,
    blocks: HashMap<i64,PrimitiveBlock>,
    
}



impl<'a> SortBlocks {
    pub fn new(groups: Option<Box<QuadtreeTree>>) -> SortBlocks {
        SortBlocks{groups: groups,blocks:HashMap::new()}
    }
    
    fn get_block(&'a mut self, q: Quadtree) -> &'a mut PrimitiveBlock {
        get_block(&mut self.blocks, self.groups.as_ref().unwrap(), q)
    }
    
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
    }
    
    fn add_all(&mut self, bl: PrimitiveBlock) {
        for n in bl.nodes { 
            self.add_node(n);
        }
        for w in bl.ways {
            self.add_way(w);
        }
        for r in bl.relations {
            self.add_relation(r);
        }
    }
    
    fn finish(&mut self) -> (Vec<PrimitiveBlock>, Option<Box<QuadtreeTree>>) {
        let mut bv = Vec::new();
        for (_,b) in std::mem::take(&mut self.blocks) {
            bv.push(b);
        }
        bv.sort_by_key( |b| { b.quadtree.as_int() });
        (bv, self.groups.take())
    }
    
}

struct CollectBlocks {
    sb: SortBlocks,
    tm: f64
}
impl CollectBlocks {
    pub fn new(groups: Box<QuadtreeTree>) -> CollectBlocks {
        CollectBlocks{sb: SortBlocks::new(Some(groups)), tm: 0.0}
    }
}
impl CallFinish for CollectBlocks {
    type CallType=PrimitiveBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        let tx=Timer::new();
        self.sb.add_all(bl);
        self.tm+=tx.since();
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let mut t = Timings::new();
        t.add("find blocks", self.tm);
        let (bv,groups) = self.sb.finish();
        
        t.add_other("groups", OtherData::QuadtreeTree(groups.unwrap()));
        t.add_other("blocks", OtherData::AllBlocks(bv));
        
        Ok(t)
    }
}
    

fn get_blocks(infn: &str, qtsfn: &str, groups: Box<QuadtreeTree>, numchan: usize) -> io::Result<Vec<PrimitiveBlock>> {
    
    
    let (mut res,d) = 
        if numchan == 0 {
            let cc = Box::new(CollectBlocks::new(groups));
            let aq = Box::new(AddQuadtree::new(qtsfn, cc));
            let pp = make_unpackprimblock(aq);
            read_all_blocks(infn, pp)
        } else {
            let cc = Box::new(Callback::new(Box::new(CollectBlocks::new(groups))));
            let aqs = CallbackSync::new(Box::new(AddQuadtree::new(qtsfn, cc)),numchan);
            let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
            for aq in aqs {
                let aq2=Box::new(ReplaceNoneWithTimings::new(aq));
                pps.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
            }
            
            let pp = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
            read_all_blocks(infn, pp)
        };
    
    let mut blocks: Option<Vec<PrimitiveBlock>>=None;
    
    for o in std::mem::take(&mut res.others) {
        match o {
            (_,OtherData::AllBlocks(l)) => { blocks=Some(l); },
             _ => {}
        }
    }
    println!("\n{:8.3}s Total, {} [{} blocks]", d, res, blocks.as_ref().unwrap().len()); 
    
    
    Ok(blocks.unwrap())
    
}
    
fn write_blocks(outfn: &str, blocks: Vec<PrimitiveBlock>, numchan: usize, timestamp: i64) -> io::Result<()> {
    
    let wf = Box::new(WriteFile::new(&outfn, HeaderType::NoLocs));
    
    let t = 
        if numchan == 0 {
            let mut wq = make_packprimblock(wf, true);
            for mut b in blocks {
                b.end_date = timestamp;
                wq.call(b);
            }
            wq.finish()?
        } else {
            let wfs = CallbackSync::new(wf, 4);
            let mut wqs: Vec<Box<dyn CallFinish<CallType=PrimitiveBlock,ReturnType=Timings>>> = Vec::new();
            for w in wfs {
                let w2=Box::new(ReplaceNoneWithTimings::new(w));
                wqs.push(Box::new(Callback::new(make_packprimblock(w2,true))));
            }
            let mut wq = Box::new(CallbackMerge::new(wqs,Box::new(MergeTimings::new())));
            
            for mut b in blocks {
                b.end_date = timestamp;
                wq.call(b);
            }
            wq.finish()?
        };
    
   
    println!("{}", t);
    Ok(())
}

enum TempData {
    TempBlocks(Vec<(i64,Vec<Vec<u8>>)>),
    TempFile((String,Vec<(i64,Vec<(u64,u64)>)>))
}

struct WriteTemp {
    tempf: Option<WriteFile>,
    tempd: BTreeMap<i64,Vec<Vec<u8>>>,
    tm: f64
}

impl WriteTemp {
    pub fn new(tempfn: &str) -> WriteTemp {
        if tempfn == "NONE" {
            WriteTemp{tempf: None, tempd: BTreeMap::new(), tm:0.0}
        } else {
            WriteTemp{tempf: Some(WriteFile::new(tempfn,HeaderType::NoLocs)), tempd: BTreeMap::new(), tm:0.0}
        }
    }
    
    fn add_temps(&mut self, temps: Vec<(i64,Vec<u8>)>) {
        for (a,b) in temps {
            match self.tempd.get_mut(&a) {
                Some(t) => { t.push(b); },
                None => { self.tempd.insert(a,vec![b]); }
            }
        }
    }           
    
}
impl CallFinish for WriteTemp {
    type CallType=Vec<(i64,Vec<u8>)>;
    type ReturnType=Timings;
    
    fn call(&mut self, bl: Self::CallType) {
        
        match self.tempf.as_mut() {
            Some(wf) => { wf.call(bl); },
            None => { 
                let tx=Timer::new();
                self.add_temps(bl);
                self.tm+=tx.since();
            }
        }
        
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        match self.tempf.as_mut() {
            Some(wf) => {
                return wf.finish();
            },
            None => {
                let mut t = Timings::new();
                t.add("store temp", self.tm);
                let mut td=Vec::new();
                for (a,b) in std::mem::take(&mut self.tempd) {
                    td.push((a,b));
                }
                
                t.add_other("tempdata", OtherData::TempData(td));
                return Ok(t);
            }
        }
    }
}
            
            
struct CollectTemp<T> {
    out: Box<T>,
    limit: usize,
    splitat: i64,
    groups: Option<Box<QuadtreeTree>>,
    pending: BTreeMap<i64,PrimitiveBlock>,
    qttoidx: BTreeMap<Quadtree,i64>,
    tm: f64,
}

impl<'a, T> CollectTemp<T>
    where T: CallFinish<CallType=PrimitiveBlock, ReturnType=Timings> 
{
    pub fn new(out: Box<T>, limit: usize, splitat: i64, groups: Box<QuadtreeTree>) -> CollectTemp<T> {
        let mut qttoidx = BTreeMap::new();
        let mut i=0;
        for (_,t) in groups.iter() {
            qttoidx.insert(t.qt,i);
            i+=1;
        }
        CollectTemp{out: out, limit:limit,splitat:splitat,groups:Some(groups),qttoidx:qttoidx,pending: BTreeMap::new(),tm:0.0}
    }
    
    fn add_all(&mut self, bl: PrimitiveBlock) -> Vec<PrimitiveBlock> {
        let mut mm = Vec::new();
        for n in bl.nodes {
            match self.add_node(n) {
                Some(m) => mm.push(m),
                None => {}
            }
        
        }
        for w in bl.ways {
            match self.add_way(w) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        for r in bl.relations {
            match self.add_relation(r) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        mm
    }
    
    fn get_block(&'a mut self, q: Quadtree) -> &'a mut PrimitiveBlock {
        let q = self.groups.as_ref().unwrap().find(q).1.qt;
        let i = self.qttoidx.get(&q).unwrap();
        let k = i/self.splitat;
        if !self.pending.contains_key(&k) {
            let t = PrimitiveBlock::new(k,0);
            self.pending.insert(k.clone(), t);
        }
        self.pending.get_mut(&k).unwrap()
    }
    
    fn add_node(&mut self, n: Node) -> Option<PrimitiveBlock> {
        let l=self.limit;
        let t = self.get_block(n.quadtree);
        t.nodes.push(n);
        if t.nodes.len()+8*t.ways.len()+20*t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index,0)));
        }
        None
    }
    
    fn add_way(&mut self, w: Way) -> Option<PrimitiveBlock> {
        let l=self.limit;
        let t = self.get_block(w.quadtree);
        t.ways.push(w);
        if t.nodes.len()+8*t.ways.len()+20*t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index,0)));
        }
        None
    }
    
    fn add_relation(&mut self, r: Relation) -> Option<PrimitiveBlock> {
        let l=self.limit;
        let t = self.get_block(r.quadtree);
        t.relations.push(r);
        //self.check_tile(t)
        if t.nodes.len()+8*t.ways.len()+20*t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index,0)));
        }
        None
    }
     /*           
    fn check_tile(&mut self, t: &mut PrimitiveBlock) -> Option<PrimitiveBlock> {
        if t.len() == self.limit {
            return Some(std::mem::replace(t, PrimitiveBlock::new(0,0)));
        }
        None
    }*/
            
}

impl<T> CallFinish for CollectTemp<T>
    where T: CallFinish<CallType=PrimitiveBlock, ReturnType=Timings>
{
    type CallType=PrimitiveBlock;
    type ReturnType=Timings;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        let tx=Timer::new();
        let mm = self.add_all(bl);
        self.tm+=tx.since();
        for m in mm { 
            self.out.call(m);
        }
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        //let mut mm = Vec::new();
        for (_,b) in std::mem::take(&mut self.pending) {
            self.out.call(b);
            //mm.push(b);
        }
        //self.out.call(mm);
        
        let mut r = self.out.finish()?;
        r.add_other("quadtreetree", OtherData::QuadtreeTree(self.groups.take().unwrap()));
        r.add("collect temp", self.tm);
        Ok(r)
    }
}
    

fn write_temp_blocks(infn: &str, qtsfn: &str, tempfn: &str, groups: Box<QuadtreeTree>, numchan: usize, splitat: i64, limit: usize) -> io::Result<(TempData,Box<QuadtreeTree>)> {
    
    let wt = Box::new(WriteTemp::new(&tempfn));
    
    let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!("write_temp_blocks {} to {}, numchan={}", infn,tempfn,numchan));
    let flen = file_length(&infn);
    let inf = File::open(&infn)?;
    let mut infb = BufReader::new(inf);
            
    let (mut res,d) = 
        if numchan == 0 {
            let pc = make_packprimblock(wt,true);
            let cc = Box::new(CollectTemp::new(pc, limit, splitat, groups));
            let aq = Box::new(AddQuadtree::new(qtsfn, cc));
            let pp = make_unpackprimblock(aq);
            
            
            read_all_blocks_prog(&mut infb, flen, pp, &prog, 100.0)
        } else {
            let wts = CallbackSync::new(wt, numchan);
            let mut pcs: Vec<Box<dyn CallFinish<CallType=PrimitiveBlock,ReturnType=Timings>>> = Vec::new();
            for wt in wts {
                let wt2=Box::new(ReplaceNoneWithTimings::new(wt));
                pcs.push(Box::new(Callback::new(make_packprimblock(wt2,true))));
            }
            let pc = Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())));
            
            let ccw = Box::new(Callback::new(Box::new(CollectTemp::new(pc, limit, splitat, groups))));
            let aqs = CallbackSync::new(Box::new(AddQuadtree::new(qtsfn, ccw)),numchan);
            let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
            for aq in aqs {
                let aq2=Box::new(ReplaceNoneWithTimings::new(aq));
                pps.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
            }
            
            let pp = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
            read_all_blocks_prog(&mut infb, flen, pp, &prog, 100.0)
        };
    println!("write_temp_blocks {} {}", res, d);
    let mut groups: Option<Box<QuadtreeTree>> = None;
    let mut td: Option<TempData> = None;
    if tempfn == "NONE" {
        for (_,b) in std::mem::take(&mut res.others) {
            match b {
                OtherData::TempData(t) => {td=Some(TempData::TempBlocks(t)); },
                OtherData::QuadtreeTree(g) => { groups=Some(g); },
                _ => {}
            }
        }
    } else {
        for (_,b) in std::mem::take(&mut res.others) {
            match b {
                OtherData::FileLocs(fl) => {td=Some(TempData::TempFile((String::from(tempfn),fl)));},
                OtherData::QuadtreeTree(g) => { groups=Some(g); },
                _ => {}
            }
        }
    }
    Ok((td.unwrap(),groups.unwrap()))
}
    


pub fn sort_blocks_inmem(infn: &str, qtsfn: &str, outfn: &str, groups: Box<QuadtreeTree>, numchan: usize, timestamp: i64) -> io::Result<()> {
    let groupsfn = format!("{}-groups.txt", outfn);
    let outf = File::create(&groupsfn)?;
    for (_,g) in groups.iter() {
        writeln!(&outf, "{};{};{}", g.qt,g.weight,g.total)?;
    }
    
    println!("call get_blocks({}, {}, {}, {})", infn, qtsfn, groups, numchan);
    let blocks = get_blocks(infn, qtsfn, groups, numchan)?;
    
    println!("call write_blocks({}, {}, {}, {})", outfn, blocks.len(), numchan, timestamp);
    //Err(io::Error::new(io::ErrorKind::Other,"not impl"))
    write_blocks(outfn, blocks, numchan, timestamp)
}

struct CollectBlocksTemp<T> {
    out: Box<T>,
    groups: Option<Box<QuadtreeTree>>,
    tm: f64,
    tm2: f64,
    timestamp: i64
}

impl<T> CollectBlocksTemp<T>
    where T: CallFinish<CallType=Vec<(i64,Vec<u8>)>, ReturnType=Timings>
{
    pub fn new(out: Box<T>, groups: Box<QuadtreeTree>, timestamp: i64) -> CollectBlocksTemp<T> {
        CollectBlocksTemp{out: out, groups: Some(groups), tm: 0.0, tm2: 0.0, timestamp: timestamp}
    }
    
    fn sort_all(&mut self, blocks: Vec<FileBlock>) -> Vec<PrimitiveBlock> {
        let mut sb = SortBlocks::new(self.groups.take());
        for bl in blocks {
            if bl.block_type=="OSMData" {
                let pb = PrimitiveBlock::read(0,0,&bl.data(),false,false).expect("?");
                sb.add_all(pb);
            }
        }
        let (mut bv, gg) = sb.finish();
        for b in &mut bv {
            b.end_date = self.timestamp;
        }
        self.groups = gg;
        bv
    }
}

fn pack_all(bls: Vec<PrimitiveBlock>) -> Vec<(i64,Vec<u8>)> {
    let mut r = Vec::new();
    for bl in bls {
        let p = bl.pack(true,false).expect("?");
        let q = pack_file_block("OSMData", &p, true).expect("?");
        r.push((bl.quadtree.as_int(), q));
    }
    r
}

impl<T> CallFinish for CollectBlocksTemp<T>
    where T: CallFinish<CallType=Vec<(i64,Vec<u8>)>, ReturnType=Timings>
{
    type CallType=(i64,Vec<FileBlock>);
    type ReturnType = Timings;
    
    fn call(&mut self, (_, bls): (i64,Vec<FileBlock>)) {
        let tx=Timer::new();
        let bv = self.sort_all(bls);
        let ta=tx.since();
        let tp = pack_all(bv);
        let tb=tx.since();
        self.tm += ta;
        self.tm2 += tb-ta;
        
        self.out.call(tp);
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let mut r = self.out.finish()?;
        r.add("resortblocks", self.tm);
        r.add("packblocks", self.tm2);
        r.add_other("quadtreetree", OtherData::QuadtreeTree(self.groups.take().unwrap()));
        Ok(r)
    }
}

fn read_temp_data<T: CallFinish<CallType=(i64,Vec<FileBlock>),ReturnType=Timings>>(xx: TempData, mut out: Box<T>) -> io::Result<Timings> {
    //let mut ct = Checktime::with_threshold(2.0);
    
    
    match xx {
        TempData::TempBlocks(tb) => {
            
            let tbl = tb.iter().map(|(_,x)| { x.iter().map(|y| { y.len()  as u64 }).sum::<u64>() }).sum::<u64>();
            let prog = ProgBarWrap::new_filebytes(tbl);
            prog.set_message("read tempblocks from memory");
            let mut tl=0;
            for (a,t) in tb {
                
                let mut mm = Vec::new();
                
                for x in t {
                    tl+=x.len();
                    mm.push(unpack_file_block(0,&x)?);
                }
                /*match ct.checktime() {
                    Some(d) => {
                        print!("\r[{:6.1}s] tile {}, {} // {}", d,a, mm.len(),tl);
                        io::stdout().flush().expect("");
                    },
                    None => {}
                }
                //println!("{:6.1}s] tile {}, {} // {}", ct.gettime(),a, mm.len(),tl);*/
                prog.prog(tl as f64);
                
                out.call((a,mm));
            }
            prog.finish();
            return out.finish();
        },
        TempData::TempFile((fname, locs)) => {
            let tbl = file_length(&fname);            
            let prog = ProgBarWrap::new_filebytes(tbl);
            prog.set_message(&format!("read temp blocks from {}", fname));
            let ff = File::open(&fname)?;
            let mut fb = BufReader::new(ff);
            let mut tl=0;
            for (a,b) in locs {
                let mut mm = Vec::new();
                
                for (p,q) in b {
                    fb.seek(SeekFrom::Start(p))?;
                    mm.push(read_file_block(&mut fb)?);
                    tl+=q;
                }
                /*match ct.checktime() {
                    Some(d) => {
                        print!("\r[{:6.1}s] tile {}, {} // {}", d,a, mm.len(),tl);
                        io::stdout().flush().expect("");
                    },
                    None => {}
                }
                //println!("{:6.1}s] tile {}, {} // {}", ct.gettime(),a, mm.len(),tl);
                */
                prog.prog(tl as f64);
                
                out.call((a,mm));
            }
            prog.finish();
            return out.finish();
        }
    }
}
            
            


fn write_blocks_from_temp(xx: TempData, outfn: &str, groups: Box<QuadtreeTree>, numchan: usize, timestamp: i64) -> io::Result<()> {
    let wf = Box::new(WriteFile::new(&outfn, HeaderType::ExternalLocs));
    
    let t = 
        if numchan == 0 {
            
            let cq = Box::new(CollectBlocksTemp::new(wf, groups, timestamp));
            
            read_temp_data(xx, cq)
            
        } else {
            let wfs = CallbackSync::new(wf, numchan);
            let mut cqs: Vec<Box<dyn CallFinish<CallType=(i64,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
            for w in wfs {
                let w2=Box::new(ReplaceNoneWithTimings::new(w));
                
                cqs.push(Box::new(Callback::new(Box::new(CollectBlocksTemp::new(w2,Box::new(groups.clone()), timestamp)))));
            }
            
            let cq = Box::new(CallbackMerge::new(cqs,Box::new(MergeTimings::new())));
            
            read_temp_data(xx, cq)
        }?;
    
   
    println!("{}", t);
    Ok(())
} 


pub fn sort_blocks(infn: &str, qtsfn: &str, outfn: &str, groups: Box<QuadtreeTree>, numchan: usize, splitat: i64, tempinmem: bool, limit: usize, timestamp: i64) -> io::Result<()> {
    println!("sort_blocks({},{},{},{},{},{},{},{})", infn,qtsfn,outfn,groups.len(),numchan,splitat,tempinmem,limit);
    
    let mut tempfn = String::from("NONE");
    if !tempinmem {
        tempfn = format!("{}-temp.pbf", String::from(outfn).substr(0,outfn.len()-4));
    }
    
    let (xx, groups) = write_temp_blocks(infn, qtsfn, &tempfn, groups, numchan, splitat, limit)?;
    
    match &xx { 
        TempData::TempFile((fname,locs)) => {
            let nl: usize = locs.iter().map(|(_,y)| { y.len() }).sum();
            let nb: u64 = locs.iter().map(|(_,y)| { y }).flatten().map( |(_,b)| { b }).sum();
            println!("temp file {}, {} tiles {} locs {} bytes", fname, locs.len(), nl, nb);
        },
        TempData::TempBlocks(data) => {
            let nl: usize = data.iter().map(|(_,y)| { y.len() }).sum();
            let nb: usize = data.iter().map(|(_,y)| { y }).flatten().map( |x| { x.len() }).sum();            
            println!("temp blocks {} tiles, {} blobs {} bytes", data.len(), nl, nb);
        },
    }
    
    write_blocks_from_temp(xx, outfn, groups, numchan, timestamp)?;
    
    Ok(())
    //Err(io::Error::new(io::ErrorKind::Other,"not impl"))
}
