use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io;
use std::io::{BufReader, Seek, SeekFrom, Write,Read,/*Result*/};
use std::sync::Arc;

use crate::callback::{CallFinish, Callback, CallbackMerge, CallbackSync};
use crate::elements::{Node, PrimitiveBlock, Quadtree, Relation, Way};

use crate::pbfformat::header_block::HeaderType;
use crate::pbfformat::read_file_block::{
    file_length, pack_file_block, read_all_blocks_with_progbar,
    unpack_file_block, FileBlock, ProgBarWrap, read_file_block_with_pos
};
pub use crate::sortblocks::addquadtree::{make_unpackprimblock, AddQuadtree};
pub use crate::sortblocks::writepbf::{make_packprimblock, make_packprimblock_many, WriteFile};
use crate::sortblocks::{OtherData, QuadtreeTree, Timings,TempData,FileLocs};
use crate::stringutils::StringUtils;
use crate::utils::{MergeTimings, ReplaceNoneWithTimings, Timer,ThreadTimer};

use serde_json;
use serde::{Serialize,Deserialize};

fn get_block<'a>(
    blocks: &'a mut HashMap<i64, PrimitiveBlock>,
    groups: &'a QuadtreeTree,
    q: Quadtree,
) -> &'a mut PrimitiveBlock {
    let (_, b) = groups.find(q);
    let q = b.qt.as_int();
    if !blocks.contains_key(&q) {
        let mut t = PrimitiveBlock::new(0, 0);
        t.quadtree = b.qt;
        blocks.insert(q.clone(), t);
    }
    blocks.get_mut(&q).unwrap()
}

struct SortBlocks {
    groups: Arc<QuadtreeTree>,
    blocks: HashMap<i64, PrimitiveBlock>,
}

impl<'a> SortBlocks {
    pub fn new(groups: Arc<QuadtreeTree>) -> SortBlocks {
        SortBlocks {
            groups: groups,
            blocks: HashMap::new(),
        }
    }

    fn get_block(&'a mut self, q: Quadtree) -> &'a mut PrimitiveBlock {
        get_block(&mut self.blocks, &self.groups, q)
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

    fn finish(&mut self) -> Vec<PrimitiveBlock> {
        let mut bv = Vec::new();
        for (_, mut b) in std::mem::take(&mut self.blocks) {
            b.sort();
            bv.push(b);
        }
        bv.sort_by_key(|b| b.quadtree.as_int());
        bv
    }
}

struct CollectBlocks {
    sb: SortBlocks,
    tm: f64,
}
impl CollectBlocks {
    pub fn new(groups: Arc<QuadtreeTree>) -> CollectBlocks {
        CollectBlocks {
            sb: SortBlocks::new(groups),
            tm: 0.0,
        }
    }
}
impl CallFinish for CollectBlocks {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: PrimitiveBlock) {
        let tx = Timer::new();
        self.sb.add_all(bl);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut t = Timings::new();
        t.add("find blocks", self.tm);
        let bv = self.sb.finish();

        //t.add_other("groups", OtherData::QuadtreeTree(groups.unwrap()));
        t.add_other("blocks", OtherData::AllBlocks(bv));

        Ok(t)
    }
}

fn get_blocks(
    infn: &str,
    qtsfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
) -> io::Result<Vec<PrimitiveBlock>> {
    let pp: Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>> = if numchan == 0 {
        let cc = Box::new(CollectBlocks::new(groups));
        let aq = Box::new(AddQuadtree::new(qtsfn, cc));
        make_unpackprimblock(aq)
        
    } else {
        let cc = Box::new(Callback::new(Box::new(CollectBlocks::new(groups))));
        let aqs = CallbackSync::new(Box::new(AddQuadtree::new(qtsfn, cc)), numchan);
        let mut pps: Vec<Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>> =
            Vec::new();
        for aq in aqs {
            let aq2 = Box::new(ReplaceNoneWithTimings::new(aq));
            pps.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
        }

        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
        
    };
    let (mut res, d) = read_all_blocks_with_progbar(infn, pp, "get_blocks");
    let mut blocks: Option<Vec<PrimitiveBlock>> = None;

    for o in std::mem::take(&mut res.others) {
        match o {
            (_, OtherData::AllBlocks(l)) => {
                blocks = Some(l);
            }
            _ => {}
        }
    }
    println!(
        "\n{:8.3}s Total, {} [{} blocks]",
        d,
        res,
        blocks.as_ref().unwrap().len()
    );

    Ok(blocks.unwrap())
}

fn write_blocks(
    outfn: &str,
    blocks: Vec<PrimitiveBlock>,
    numchan: usize,
    timestamp: i64,
) -> io::Result<()> {
    let wf = Box::new(WriteFile::new(&outfn, HeaderType::NoLocs));

    let t = if numchan == 0 {
        let mut wq = make_packprimblock(wf, true);
        for mut b in blocks {
            b.end_date = timestamp;
            wq.call(b);
        }
        wq.finish()?
    } else {
        let wfs = CallbackSync::new(wf, 4);
        let mut wqs: Vec<Box<dyn CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>>> =
            Vec::new();
        for w in wfs {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            wqs.push(Box::new(Callback::new(make_packprimblock(w2, true))));
        }
        let mut wq = Box::new(CallbackMerge::new(wqs, Box::new(MergeTimings::new())));

        for mut b in blocks {
            b.end_date = timestamp;
            wq.call(b);
        }
        wq.finish()?
    };

    println!("{}", t);
    Ok(())
}




pub struct WriteTempData {
    tempd: BTreeMap<i64, Vec<Vec<u8>>>,
    tm: f64
}

impl WriteTempData {
    pub fn new() -> WriteTempData {
        WriteTempData{tempd: BTreeMap::new(), tm: 0.0}
    }
}

impl CallFinish for WriteTempData {
    type CallType=Vec<(i64,Vec<u8>)>;
    type ReturnType=Timings;
    
    fn call(&mut self, temps: Vec<(i64,Vec<u8>)>) {
        let tx=ThreadTimer::new();
        for (a, b) in temps {
            match self.tempd.get_mut(&a) {
                Some(t) => {
                    t.push(b);
                }
                None => {
                    self.tempd.insert(a, vec![b]);
                }
            }
        }
        self.tm += tx.since();
    }
    
    fn finish(&mut self) -> io::Result<Timings> {
        let mut tms = Timings::new();
        tms.add("WriteTempData", self.tm);
        let mut td = Vec::new();
        for (a,b) in std::mem::take(&mut self.tempd) {
            td.push((a,b));
        }
        tms.add_other("tempdata", OtherData::TempData(TempData::TempBlocks(td)));
        Ok(tms)
    }
}

pub struct WriteTempFile {
    tempfn: String,
    tempf: WriteFile,
}

impl WriteTempFile {
    pub fn new(tempfn: &str) -> WriteTempFile {
        WriteTempFile{tempfn: String::from(tempfn), tempf: WriteFile::new(tempfn, HeaderType::NoLocs)}
    }
}
impl CallFinish for WriteTempFile {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, bl: Self::CallType) {
        self.tempf.call(bl);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut t = self.tempf.finish()?;
        let fl = match t.others.pop().unwrap().1 {
                OtherData::FileLocs(p) => p,
                _ => {panic!("!!"); }
            };
        
        t.add_other("tempdata", OtherData::TempData(TempData::TempFile((self.tempfn.clone(), fl))));
        Ok(t)
        
        
    }
}

pub struct WriteTempFileSplit {
    prfx: String,
    tempfs: BTreeMap<i64, (String,WriteFile)>,
    splitat: i64,
}
impl WriteTempFileSplit {
    pub fn new(prfx: &str, splitat: i64) -> WriteTempFileSplit {
        WriteTempFileSplit{prfx: String::from(prfx), splitat: splitat, tempfs: BTreeMap::new()}
    }
}

impl CallFinish for WriteTempFileSplit {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, bl: Self::CallType) {
        for (a,b) in bl {
            let k = a/self.splitat;
            match self.tempfs.get_mut(&k) {
                None => {
                    let fname=format!("{}-part-{}.pbf", self.prfx, k);
                    let mut tt=WriteFile::new(&fname, HeaderType::NoLocs);
                    tt.call(vec![(a,b)]);
                    self.tempfs.insert(k,(fname,tt));
                },
                Some(tempf) => {
                    tempf.1.call(vec![(a,b)]);
                }
            }
        }
    }
    fn finish(&mut self) -> io::Result<Timings> {
        let mut tm = Timings::new();
        
        let mut parts = Vec::new();
        for (k,(fname,mut wf)) in std::mem::take(&mut self.tempfs) {
            let mut t = wf.finish()?;
            
            let fl = match std::mem::take(&mut t.others).pop().unwrap().1 {
                OtherData::FileLocs(p) => p,
                _ => {panic!("!!"); }
            };
            
            parts.push((k,fname,fl));
            tm.combine(t);
        }
        
        tm.add_other("tempdata", OtherData::TempData(TempData::TempFileSplit(parts)));
        Ok(tm)
    }
}
    
    
pub enum WriteTempWhich {
    Data(WriteTempData),
    File(WriteTempFile),
    Split(WriteTempFileSplit)
}

impl CallFinish for WriteTempWhich {
    type CallType=Vec<(i64,Vec<u8>)>;
    type ReturnType=Timings;
    
    fn call(&mut self, bl: Vec<(i64,Vec<u8>)>) {
        match self {
            WriteTempWhich::Data(t) => t.call(bl),
            WriteTempWhich::File(t) => t.call(bl),
            WriteTempWhich::Split(t) => t.call(bl),
        }
    }
    
    fn finish(&mut self) ->io::Result<Timings> {
        match self {
            WriteTempWhich::Data(t) => t.finish(),
            WriteTempWhich::File(t) => t.finish(),
            WriteTempWhich::Split(t) => t.finish(),
        }
    }
}
        
        
    
struct CollectTemp<T> {
    out: Box<T>,
    limit: usize,
    splitat: i64,
    groups: Arc<QuadtreeTree>,
    pending: BTreeMap<i64, PrimitiveBlock>,
    qttoidx: BTreeMap<Quadtree, i64>,
    tm: f64,
    //count: usize
}

impl<'a, T> CollectTemp<T>
where
    T: CallFinish<CallType = Vec<PrimitiveBlock>, ReturnType = Timings>,
{
    pub fn new(
        out: Box<T>,
        limit: usize,
        splitat: i64,
        groups: Arc<QuadtreeTree>,
    ) -> CollectTemp<T> {
        let mut qttoidx = BTreeMap::new();
        let mut i = 0;
        for (_, t) in groups.iter() {
            qttoidx.insert(t.qt, i);
            i += 1;
        }
        CollectTemp {
            out: out,
            limit: limit,
            //write_at: write_at,
            splitat: splitat,
            groups: groups,
            qttoidx: qttoidx,
            pending: BTreeMap::new(),
            tm: 0.0,
            //count: 0
        }
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
        let q = self.groups.find(q).1.qt;
        let i = self.qttoidx.get(&q).unwrap();
        let k = i / self.splitat;
        if !self.pending.contains_key(&k) {
            let t = PrimitiveBlock::new(k, 0);
            self.pending.insert(k.clone(), t);
        }
        self.pending.get_mut(&k).unwrap()
    }
    
    fn add_node(&mut self, n: Node) -> Option<PrimitiveBlock> {
        let l = self.limit;
        let t = self.get_block(n.quadtree);
        t.nodes.push(n);
        if t.nodes.len() + 8 * t.ways.len() + 20 * t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index, 0)));
        }
        None
    }

    fn add_way(&mut self, w: Way) -> Option<PrimitiveBlock> {
        let l = self.limit;
        let t = self.get_block(w.quadtree);
        t.ways.push(w);
        if t.nodes.len() + 8 * t.ways.len() + 20 * t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index, 0)));
        }
        None
    }

    fn add_relation(&mut self, r: Relation) -> Option<PrimitiveBlock> {
        let l = self.limit;
        let t = self.get_block(r.quadtree);
        t.relations.push(r);
        if t.nodes.len() + 8 * t.ways.len() + 20 * t.relations.len() >= l {
            return Some(std::mem::replace(t, PrimitiveBlock::new(t.index, 0)));
        }
        None
    }
    
}

impl<T> CallFinish for CollectTemp<T>
where
    T: CallFinish<CallType = Vec<PrimitiveBlock>, ReturnType = Timings>,
{
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: PrimitiveBlock) {
        let tx = Timer::new();
        let mm = self.add_all(bl);
        self.tm += tx.since();
        
        self.out.call(mm);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut mm = Vec::new();
        for (_, b) in std::mem::take(&mut self.pending) {
            mm.push(b);
        }
        self.out.call(mm);
        
        let mut r = self.out.finish()?;
        r.add("collect temp", self.tm);
        Ok(r)
    }
}



    











fn write_temp_blocks(
    infn: &str,
    qtsfn: &str,
    tempfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
    splitat: i64,
    limit: usize,
    //write_at: usize
) -> io::Result<TempData> {
    let flen = file_length(&infn);
    
    let wt: Box<WriteTempWhich> = if tempfn == "NONE" {
        Box::new(WriteTempWhich::Data(WriteTempData::new()))
    } else {
        if flen < 2*1024*1024*1024 {
            Box::new(WriteTempWhich::File(WriteTempFile::new(tempfn)))
        } else {
            let nsp = (flen / (1*1024*1024*1024)) as i64;
            let sp = groups.len() as i64/splitat/nsp;
            Box::new(WriteTempWhich::Split(WriteTempFileSplit::new(tempfn, sp)))
        }
    };

    

    let pp: Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>> = if numchan == 0 {
        let pc = make_packprimblock_many(wt, true);
        let cc = Box::new(CollectTemp::new(pc, limit, splitat, groups));
        let aq = Box::new(AddQuadtree::new(qtsfn, cc));
        make_unpackprimblock(aq)
    } else {
        let wts = CallbackSync::new(wt, numchan);
        
        let mut pcs: Vec<Box<dyn CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>>> =
            Vec::new();
        
        for wt in wts { 
            let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
            let pp = make_packprimblock_many(wt2, true);
            pcs.push(Box::new(Callback::new(Box::new(CollectTemp::new(pp, limit/numchan, splitat, groups.clone())))));
        
        }
        let ccw = Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())));
        
        
        
        
        let aqs = CallbackSync::new(Box::new(AddQuadtree::new(qtsfn, ccw)), numchan);
        let mut pps: Vec<Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>>> =
            Vec::new();
        for aq in aqs {
            let aq2 = Box::new(ReplaceNoneWithTimings::new(aq));
            pps.push(Box::new(Callback::new(make_unpackprimblock(aq2))));
        }
        
        
        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
        
    };
    let (mut res, d) = read_all_blocks_with_progbar(infn, pp, &format!("write_temp_blocks to {}, numchan={}", tempfn, numchan));
    
    println!("write_temp_blocks {} {}", res, d);
    //let mut groups: Option<Box<QuadtreeTree>> = None;
    let mut td: Option<TempData> = None;
    for (_, b) in std::mem::take(&mut res.others) {
        match b {
            OtherData::TempData(t) => {
                td = Some(t);
            }
            /*OtherData::QuadtreeTree(g) => {
                groups = Some(g);
            }*/
            _ => {}
        }
    }
    
    //Ok((td.unwrap(), groups.unwrap()))
    Ok(td.unwrap())
}

pub fn sort_blocks_inmem(
    infn: &str,
    qtsfn: &str,
    outfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
    timestamp: i64,
) -> io::Result<()> {
    let groupsfn = format!("{}-groups.txt", outfn);
    let outf = File::create(&groupsfn)?;
    for (_, g) in groups.iter() {
        writeln!(&outf, "{};{};{}", g.qt, g.weight, g.total)?;
    }

    println!(
        "call get_blocks({}, {}, {}, {})",
        infn, qtsfn, groups, numchan
    );
    let blocks = get_blocks(infn, qtsfn, groups, numchan)?;

    println!(
        "call write_blocks({}, {}, {}, {})",
        outfn,
        blocks.len(),
        numchan,
        timestamp
    );
    //Err(io::Error::new(io::ErrorKind::Other,"not impl"))
    write_blocks(outfn, blocks, numchan, timestamp)
}

struct CollectBlocksTemp<T> {
    out: Box<T>,
    groups: Arc<QuadtreeTree>,
    tm: f64,
    tm2: f64,
    timestamp: i64,
}

impl<T> CollectBlocksTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>,
{
    pub fn new(out: Box<T>, groups: Arc<QuadtreeTree>, timestamp: i64) -> CollectBlocksTemp<T> {
        CollectBlocksTemp {
            out: out,
            groups: groups,
            tm: 0.0,
            tm2: 0.0,
            timestamp: timestamp,
        }
    }

    fn sort_all(&mut self, blocks: Vec<FileBlock>) -> Vec<PrimitiveBlock> {
        let mut sb = SortBlocks::new(self.groups.clone());
        for bl in blocks {
            if bl.block_type == "OSMData" {
                let pb = PrimitiveBlock::read(0, 0, &bl.data(), false, false).expect("?");
                sb.add_all(pb);
            }
        }
        let mut bv = sb.finish();
        for b in &mut bv {
            b.end_date = self.timestamp;
        }
        
        bv
    }
}

fn pack_all(bls: Vec<PrimitiveBlock>) -> Vec<(i64, Vec<u8>)> {
    let mut r = Vec::new();
    for bl in bls {
        let p = bl.pack(true, false).expect("?");
        let q = pack_file_block("OSMData", &p, true).expect("?");
        r.push((bl.quadtree.as_int(), q));
    }
    r
}

impl<T> CallFinish for CollectBlocksTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>,
{
    type CallType = (i64, Vec<FileBlock>);
    type ReturnType = Timings;

    fn call(&mut self, (_, bls): (i64, Vec<FileBlock>)) {
        let tx = Timer::new();
        let bv = self.sort_all(bls);
        let ta = tx.since();
        let tp = pack_all(bv);
        let tb = tx.since();
        self.tm += ta;
        self.tm2 += tb - ta;

        self.out.call(tp);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut r = self.out.finish()?;
        r.add("resortblocks", self.tm);
        r.add("packblocks", self.tm2);
        
        Ok(r)
    }
}

pub fn read_temp_data<T: CallFinish<CallType = (i64, Vec<FileBlock>), ReturnType = Timings>>(
    xx: TempData,
    mut out: Box<T>,
    remove_files: bool
) -> io::Result<Timings> {
    //let mut ct = Checktime::with_threshold(2.0);

    match xx {
        TempData::TempBlocks(tb) => {
            let tbl = tb
                .iter()
                .map(|(_, x)| x.iter().map(|y| y.len() as u64).sum::<u64>())
                .sum::<u64>();
            let prog = ProgBarWrap::new_filebytes(tbl);
            prog.set_message("read tempblocks from memory");
            let mut tl = 0;
            for (a, t) in tb {
                let mut mm = Vec::new();

                for x in t {
                    tl += x.len();
                    mm.push(unpack_file_block(0, &x)?);
                }
                
                prog.prog(tl as f64);
                out.call((a, mm));
            }
            prog.finish();
            out.finish()
        }
        TempData::TempFile((fname, locs)) => {
            let tbl = file_length(&fname);
            let prog = ProgBarWrap::new_filebytes(tbl);
            prog.set_message(&format!("read temp blocks from {}", fname));
            
            let mut tl=0;
            let mut fbuf = BufReader::new(File::open(&fname)?);
            read_blocks(&mut fbuf, locs, 128*1024*1024, &mut tl, &prog, &mut out)?;
            
            if remove_files {
                std::fs::remove_file(&fname)?;
            }
            
            prog.finish();
            out.finish()
            
        },
        TempData::TempFileSplit(parts) => {
            let mut tbl = 0;
            for (_,f,_) in &parts {
                tbl += file_length(f);
            }
            
            let prog = ProgBarWrap::new_filebytes(tbl);
            prog.set_message(&format!("read temp blocks from {} files", parts.len()));
            
            let mut tl = 0;
            for (_,f,locs) in parts {
                let mut fbuf = BufReader::new(File::open(&f)?);
                read_blocks(&mut fbuf, locs, 128*1024*1024, &mut tl, &prog, &mut out)?;
                
                if remove_files {
                    std::fs::remove_file(f)?;
                }
            }
            
            prog.finish();
            out.finish()
        }
    }
}

fn read_blocks<R: Read+Seek, T: CallFinish<CallType=(i64, Vec<FileBlock>)>>(fbuf: &mut R, locs: Vec<(i64,Vec<(u64,u64)>)>, split_size: u64, tl: &mut u64, prog: &ProgBarWrap, out: &mut Box<T>) -> io::Result<()> {
    let mut curr = Vec::new();
    let mut curr_len = 0;
    for (a,b) in locs {
        curr_len += b.iter().map(|(_,q)| { q }).sum::<u64>();
        curr.push((a,b));
        if curr_len >= split_size {
            for (k, (fbs, ll)) in read_blocks_parts(fbuf, &curr)? {
                *tl += ll;
                prog.prog(*tl as f64);
                out.call((k,fbs));
            }
            curr.clear();
            curr_len = 0;
            
        }
    }
    if curr_len > 0 {
        for (k, (fbs, ll)) in read_blocks_parts(fbuf, &curr)? {
            *tl += ll;
            prog.prog(*tl as f64);
            out.call((k,fbs));
        }
    }
    
    Ok(())
}
   

fn read_blocks_parts<R: Read+Seek>(fbuf: &mut R, curr: &Vec<(i64,Vec<(u64,u64)>)>) -> io::Result<BTreeMap<i64,(Vec<FileBlock>,u64)>> {
    let mut curr_flat = Vec::new();
    for (a,b) in curr {
        for (p,q) in b {
            curr_flat.push((*a,*p,*q));
        }
    }
    curr_flat.sort_by_key(|x| { x.1 });
    
    let mut res = BTreeMap::new();
    for (a,p,q) in curr_flat {
        
        fbuf.seek(SeekFrom::Start(p))?;
        let (_,t)=read_file_block_with_pos(fbuf, p)?;
        match res.get_mut(&a) {
            None => { res.insert(a,(vec![t],q)); },
            Some(x) => {
                x.0.push(t);
                x.1+=q;
            }
        }
    }
    Ok(res)
}
            
    
    



fn write_blocks_from_temp(
    xx: TempData,
    outfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
    timestamp: i64,
    keep_temps: bool
) -> io::Result<()> {
    let wf = Box::new(WriteFile::new(&outfn, HeaderType::ExternalLocs));

    let t = if numchan == 0 {
        let cq = Box::new(CollectBlocksTemp::new(wf, groups, timestamp));

        read_temp_data(xx, cq, !keep_temps)
    } else {
        let wfs = CallbackSync::new(wf, numchan);
        let mut cqs: Vec<
            Box<dyn CallFinish<CallType = (i64, Vec<FileBlock>), ReturnType = Timings>>,
        > = Vec::new();
        for w in wfs {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));

            cqs.push(Box::new(Callback::new(Box::new(CollectBlocksTemp::new(
                w2,
                groups.clone(),
                timestamp,
            )))));
        }

        let cq = Box::new(CallbackMerge::new(cqs, Box::new(MergeTimings::new())));

        read_temp_data(xx, cq, !keep_temps)
    }?;

    println!("{}", t);
    Ok(())
}

#[derive(Serialize,Deserialize)]
struct TempFileLocs {
    fname: String,
    locs: FileLocs
}

pub fn write_tempfile_locs(tempfn: &str, locs: &FileLocs) -> io::Result<()> {
    serde_json::to_writer(std::fs::File::create(&format!("{}-locs.json", tempfn))?,&TempFileLocs{fname: String::from(tempfn), locs:locs.clone()})?;
    Ok(())
}

pub fn read_tempfile_locs(tempfn: &str) -> io::Result<TempData> {
    let xx: TempFileLocs = serde_json::from_reader(BufReader::new(File::open(&format!("{}-locs.json", tempfn))?))?;
    Ok(TempData::TempFile((xx.fname, xx.locs)))
}


#[derive(Serialize,Deserialize)]
struct TempFileSplitLocs {
    idx: i64,
    fname: String,
    locs: FileLocs
}

pub fn write_tempfilesplit_locs(tempfn: &str, parts: &Vec<(i64,String,FileLocs)>) -> io::Result<()> {
    let mut rr = Vec::new();
    for (a,b,c) in parts {
        rr.push(TempFileSplitLocs{idx: *a, fname: b.clone(), locs: c.clone()});
    }
    serde_json::to_writer(std::fs::File::create(&format!("{}-locs.json", tempfn))?,&rr)?;
    Ok(())
}

pub fn read_tempfilesplit_locs(tempfn: &str) -> io::Result<TempData> {
    let xx: Vec<TempFileSplitLocs> = serde_json::from_reader(BufReader::new(File::open(&format!("{}-locs.json", tempfn))?))?;
    let mut parts=Vec::new();
    for t in xx {
        parts.push((t.idx, t.fname, t.locs));
    }
    Ok(TempData::TempFileSplit(parts))
}

    

pub fn sort_blocks(
    infn: &str,
    qtsfn: &str,
    outfn: &str,
    groups: Arc<QuadtreeTree>,
    numchan: usize,
    splitat: i64,
    tempinmem: bool,
    limit/*write_at*/: usize,
    timestamp: i64,
    keep_temps: bool
) -> io::Result<()> {
    println!(
        "sort_blocks({},{},{},{},{},{},{},{},{},{})",
        infn,
        qtsfn,
        outfn,
        groups.len(),
        numchan,
        splitat,
        tempinmem,
        limit/*write_at*/,
        timestamp,
        keep_temps
    );

    let mut tempfn = String::from("NONE");
    if !tempinmem {
        tempfn = format!(
            "{}-temp.pbf",
            String::from(outfn).substr(0, outfn.len() - 4)
        );
    }

    let xx = write_temp_blocks(infn, qtsfn, &tempfn, groups.clone(), numchan, splitat, limit/*write_at*/)?;

    match &xx {
        TempData::TempFile((fname, locs)) => {
            let nl: usize = locs.iter().map(|(_, y)| y.len()).sum();
            let nb: u64 = locs.iter().map(|(_, y)| y).flatten().map(|(_, b)| b).sum();
            println!(
                "temp file {}, {} tiles {} locs {} bytes",
                fname,
                locs.len(),
                nl,
                nb
            );
            if keep_temps {
                write_tempfile_locs(&tempfn, locs)?;
            }
        }
        TempData::TempBlocks(data) => {
            let nl: usize = data.iter().map(|(_, y)| {y.len()}).sum();
            let nb: usize = data.iter().map(|(_, y)| y).flatten().map(|x| x.len()).sum();
            println!(
                "temp blocks {} tiles, {} blobs {} bytes",
                data.len(),
                nl,
                nb
            );
            
                
        },
        TempData::TempFileSplit(parts) => {
            let nk: usize = parts.iter().map(|(_, _,y)| {y.len()}).sum();
            let nl: usize = parts.iter().map(|(_, _,y)| y).flatten().map(|(_,y)| { y.len() }).sum();
            let nb: u64 = parts.iter().map(|(_, _,y)| y).flatten().map(|(_,y)| y).flatten().map(|(_, b)| b).sum();
            println!(
                "temp files {} files, {} tiles {} locs {} bytes",
                parts.len(),
                nk,
                nl,
                nb
            );
            if keep_temps {
                write_tempfilesplit_locs(&tempfn, parts)?;
            }
        },
    }

    write_blocks_from_temp(xx, outfn, groups, numchan, timestamp, keep_temps)?;

    Ok(())
    //Err(io::Error::new(io::ErrorKind::Other,"not impl"))
}
