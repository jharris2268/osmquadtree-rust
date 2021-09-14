use std::fs::File;

use std::io::{BufReader, Error, ErrorKind, Result};

use std::collections::BTreeMap;

use std::sync::{Arc, Mutex};

use simple_protocolbuffers::{un_zig_zag, IterTags, PbfTag};

use crate::pbfformat::{file_length, pack_file_block, ReadFileBlocks};

use channelled_callbacks::{CallFinish, Callback, CallbackSync, CallAll, ReplaceNoneWithTimings};
use crate::elements::Quadtree;
use crate::elements::QuadtreeBlock;
use crate::pbfformat::HeaderType;
use crate::pbfformat::WriteFile;

use crate::utils::{LogTimes, Timer};

use crate::calcqts::expand_wayboxes::{WayBoxesSimple, WayBoxesSplit, WayBoxesVec};
use crate::calcqts::node_waynodes::{
    read_nodewaynodes,
    /*write_nodewaynode_file, write_waynode_sorted,*/ write_waynode_sorted_resort,
    NodeWayNodeComb,
};
use crate::calcqts::packwaynodes::{
    prep_relation_node_vals, prep_way_nodes, prep_way_nodes_tempfile, RelMems,
};
use crate::calcqts::quadtree_store::{
    QuadtreeGetSet, QuadtreeSimple, QuadtreeSplit, QuadtreeTileInt, WAY_SPLIT_VAL,
};
use crate::calcqts::write_quadtrees::{PackQuadtrees, WrapWriteFile, WriteQuadTree};
use crate::calcqts::{run_calcqts_inmem, NodeWayNodes, OtherData, Timings};

use crate::logging::messenger;
use crate::{message,progress_percent};

fn calc_way_quadtrees_simple(
    nodewaynodes: NodeWayNodes,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize
) -> Box<QuadtreeSimple> {
    let wb = Box::new(WayBoxesSimple::new(qt_level, qt_buffer));

    let t = read_nodewaynodes(nodewaynodes, wb, 0, 0, "calc_way_quadtrees_simple", numchan);

    //message!("calc_way_quadtrees_simple {}", t);
    messenger().message(&format!("calc_way_quadtrees_simple {}", t));
    let mut o: Option<Box<QuadtreeSimple>> = None;
    for (_, b) in t.others {
        match b {
            OtherData::QuadtreeSimple(q) => o = Some(q),
            _ => {}
        }
    }
    o.unwrap()
}

fn calc_and_write_qts(
    wf: Arc<Mutex<Box<WrapWriteFile>>>,
    tts: BTreeMap<i64, Box<WayBoxesVec>>,
    qt_level: usize,
    qt_buffer: f64
) -> usize {
    let wfp = CallbackSync::new(Box::new(DontFinishArc::new(wf)), 4);
    let mut v = Vec::new();
    for w in wfp {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));
        v.push(Box::new(Callback::new(Box::new(CallAll::new(
            w2,
            "calc and write",
            Box::new(move |mut t: Box<WayBoxesVec>| {
                let mut q = t.calculate(qt_level, qt_buffer);
                pack_file_block("OSMData", &q.pack().expect("!"), true).expect("?")
            }),
        )))));
    }
    
    let pg = progress_percent!(&format!(
        "calc quadtrees {} tiles [{}mb]",
        tts.len(),
        tts.len() * WAY_SPLIT_VAL / 1024 / 1024
    ));
    
    let mut i = 0;
    let pf = 100.0 / (tts.len() as f64);
    for (_, t) in tts {
        //pg.prog((i as f64) * pf);
        pg.progress_percent((i as f64) * pf);
        v[i % 4].call(t);
        i += 1;
    }
    for mut vi in v {
        vi.finish().expect("?");
    }
    pg.finish();
    
    
    i
}
struct StoreQtTile {
    qts: Arc<Mutex<Box<QuadtreeSplit>>>,
}
impl CallFinish for StoreQtTile {
    type CallType = Box<QuadtreeTileInt>;
    type ReturnType = Timings;

    fn call(&mut self, t: Box<QuadtreeTileInt>) {
        self.qts.lock().unwrap().add_tile(t);
    }
    fn finish(&mut self) -> Result<Timings> {
        Ok(Timings::new())
    }
}

fn calc_and_store_qts(
    qts: Arc<Mutex<Box<QuadtreeSplit>>>,
    tts: BTreeMap<i64, Box<WayBoxesVec>>,
    qt_level: usize,
    qt_buffer: f64
) -> usize {
    let wfp = CallbackSync::new(Box::new(StoreQtTile { qts: qts }), 4);
    let mut v = Vec::new();
    for w in wfp {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));
        v.push(Box::new(Callback::new(Box::new(CallAll::new(
            w2,
            "calc",
            Box::new(move |mut t: Box<WayBoxesVec>| t.calculate_tile(qt_level, qt_buffer)),
        )))));
    }
    let pg = messenger().start_progress_percent("calc quadtrees");
    
    let mut i = 0;
    let pf = 100.0 / (tts.len() as f64);
    for (_, t) in tts {
        
        //messenger().progress_percent((i as f64) * pf);
        pg.progress_percent((i as f64) * pf);
        

        v[i % 4].call(t);
        i += 1;
    }
    for mut vi in v {
        vi.finish().expect("?");
    }
    pg.finish();
    //messenger().finish_progress_percent();
    i
}

fn calc_way_quadtrees_split_part(
    nodewaynodes: NodeWayNodes,
    minw: i64,
    maxw: i64,
    wf: Arc<Mutex<Box<WrapWriteFile>>>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
    ram_gb: usize,
) -> usize {
    let wb = Box::new(WayBoxesSplit::new(ram_gb as u64));

    let mut t = read_nodewaynodes(
        nodewaynodes,
        wb,
        minw,
        maxw,
        &format!("calc_way_quadtrees_split_part {} to {}", minw, maxw),
        numchan,
    );

    let mut nb = 0;

    for (_, b) in std::mem::take(&mut t.others) {
        match b {
            OtherData::WayBoxTiles(tts) => {
                nb += calc_and_write_qts(wf.clone(), tts, qt_level, qt_buffer);
            }
            _ => {}
        }
    }
    nb
}

fn calc_way_quadtrees_split_part_inmem(
    nodewaynodes: NodeWayNodes,
    minw: i64,
    maxw: i64,
    qts: Arc<Mutex<Box<QuadtreeSplit>>>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
    ram_gb: usize,
) -> usize {
    let wb = Box::new(WayBoxesSplit::new(ram_gb as u64));

    let mut t = read_nodewaynodes(
        nodewaynodes,
        wb,
        minw,
        maxw,
        &format!("calc_way_quadtrees_split_part_inmem {} to {}", minw, maxw),
        numchan,
    );

    let mut nb = 0;

    for (_, b) in std::mem::take(&mut t.others) {
        match b {
            OtherData::WayBoxTiles(tts) => {
                nb += calc_and_store_qts(qts.clone(), tts, qt_level, qt_buffer);
            }

            _ => {}
        }
    }
    nb
}

fn calc_way_quadtrees_split(
    nodewaynodes: NodeWayNodes,
    outfn: &str,
    qt_level: usize,
    qt_buffer: f64,
    splits: Vec<(i64,i64)>,
    numchan: usize,
    ram_gb: usize,
) -> Box<QuadtreeSplit> {
    let tempfn = format!("{}-wayqts", outfn);
    let wf = Arc::new(Mutex::new(Box::new(WrapWriteFile::new(WriteFile::new(
        &tempfn,
        HeaderType::None,
    )))));

    //let mut qts = Box::new(QuadtreeSplit::new());
    
    
    
    for (a, b) in &splits {
        calc_way_quadtrees_split_part(
            nodewaynodes.clone(),
            a << 20,
            b << 20,
            wf.clone(),
            qt_level,
            qt_buffer,
            numchan,
            ram_gb,
        );
        //trim_memory();
    }

    wf.lock().unwrap().finish().expect("?");
    load_way_qts(&tempfn)
}

fn calc_way_quadtrees_split_inmem(
    nodewaynodes: NodeWayNodes,
    qt_level: usize,
    qt_buffer: f64,
    splits: Vec<(i64,i64)>,
    numchan: usize,
    ram_gb: usize,
) -> Box<QuadtreeSplit> {
    let qts = Arc::new(Mutex::new(Box::new(QuadtreeSplit::new())));

    //for (a,b) in &vec![(0,350),(350,700),(700,0)] {
    for (a, b) in &splits {
        calc_way_quadtrees_split_part_inmem(
            nodewaynodes.clone(),
            a << 20,
            b << 20,
            qts.clone(),
            qt_level,
            qt_buffer,
            numchan,
            ram_gb,
        );
        //trim_memory();
    }

    match Arc::try_unwrap(qts) {
        Ok(q) => match q.into_inner() {
            Ok(p) => p,
            Err(_) => {
                panic!("can't release Mutex");
            }
        },
        Err(_) => {
            panic!("can't release Arc");
        }
    }
}

fn read_quadtree_block_ways(data: &[u8], res: &mut Box<QuadtreeSplit>) {
    for x in IterTags::new(data) {
        match x {
            PbfTag::Data(2, d) => {
                for y in IterTags::new(&d) {
                    match y {
                        PbfTag::Data(3, d) => {
                            let mut i = 0;
                            let mut q = Quadtree::new(-1);
                            for z in IterTags::new(&d) {
                                match z {
                                    PbfTag::Value(1, v) => {
                                        i = v as i64;
                                    }
                                    PbfTag::Value(20, v) => {
                                        q = Quadtree::new(un_zig_zag(v));
                                    }
                                    _ => {}
                                }
                            }
                            res.set(i, q);
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
}

fn load_way_qts(infn: &str) -> Box<QuadtreeSplit> {
    let mut res = Box::new(QuadtreeSplit::new());

    let fobj = File::open(&infn).expect("file not present");
    let mut fbuf = BufReader::new(fobj);

    for bl in ReadFileBlocks::new(&mut fbuf) {
        if bl.block_type == "OSMData" {
            read_quadtree_block_ways(&bl.data(), &mut res);
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
    qt_buffer: f64,
}
const NODE_LIMIT: usize = 100000;

impl<T> ExpandNodeQuadtree<T>
where
    T: CallFinish<CallType = Vec<Box<QuadtreeBlock>>>,
{
    pub fn new(
        wayqts: Box<dyn QuadtreeGetSet>,
        nodeqts: Box<QuadtreeSimple>,
        outb: Box<T>,
        qt_level: usize,
        qt_buffer: f64,
    ) -> ExpandNodeQuadtree<T> {
        let wayqts = Some(wayqts);
        let nodeqts = Some(nodeqts);
        let tm = 0.0;
        let curr = Box::new(QuadtreeBlock::with_capacity(NODE_LIMIT));
        ExpandNodeQuadtree {
            wayqts,
            nodeqts,
            tm,
            outb,
            curr,
            qt_level,
            qt_buffer,
        }
    }
}

impl<T> CallFinish for ExpandNodeQuadtree<T>
where
    T: CallFinish<CallType = Vec<Box<QuadtreeBlock>>, ReturnType = Timings>,
{
    type CallType = Vec<NodeWayNodeComb>;
    //type ReturnType = (T::ReturnType, Box<dyn QuadtreeGetSet>, QuadtreeSimple);
    type ReturnType = Timings;

    fn call(&mut self, nn: Vec<NodeWayNodeComb>) {
        let tx = Timer::new();
        if nn.is_empty() {
            return;
        }

        //let mut bl = Box::new(QuadtreeBlock::with_capacity(nn.vals.len()));
        let mut bl = Vec::new();
        for n in nn {
            let q = if n.ways.is_empty() {
                Quadtree::calculate_point(n.lon, n.lat, self.qt_level, self.qt_buffer)
            } else {
                let mut q = Quadtree::new(-1);
                for wi in n.ways {
                    match self.wayqts.as_ref().unwrap().get(wi) {
                        None => {
                            message!("missing way {} for node {}", wi, n.id)
                        }
                        Some(qi) => {
                            q = q.common(&qi);
                        }
                    }
                }
                q
            };
            if q.as_int()<0 {
                message!("\n\n?? node {} {} {} qt {}??\n\n", n.id, n.lon, n.lat, q.as_int());
            }
            self.nodeqts.as_mut().unwrap().expand_if_present(n.id, &q);
            //bl.add_node(n.id,q);
            self.curr.add_node(n.id, q);
            if self.curr.len() >= NODE_LIMIT {
                let p = std::mem::replace(
                    &mut self.curr,
                    Box::new(QuadtreeBlock::with_capacity(NODE_LIMIT)),
                );
                bl.push(p);
            }
        }
        self.tm += tx.since();
        self.outb.call(bl);
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        self.outb.call(vec![std::mem::replace(
            &mut self.curr,
            Box::new(QuadtreeBlock::new()),
        )]);

        let mut r = self.outb.finish()?;
        r.add("calc node quadtrees", self.tm);
        r.add_other(
            "way_quadtrees",
            OtherData::QuadtreeGetSet(self.wayqts.take().unwrap()),
        );
        r.add_other(
            "node_quadtrees",
            OtherData::QuadtreeSimple(self.nodeqts.take().unwrap()),
        );
        Ok(r)
    }
}

struct DontFinishArc<T> {
    t: Arc<Mutex<Box<T>>>,
}

impl<T> DontFinishArc<T>
where
    T: CallFinish<ReturnType = Timings>,
{
    pub fn new(t: Arc<Mutex<Box<T>>>) -> DontFinishArc<T> {
        DontFinishArc { t: t }
    }
}

impl<T> CallFinish for DontFinishArc<T>
where
    T: CallFinish<ReturnType = Timings>,
{
    type CallType = <T as CallFinish>::CallType;
    type ReturnType = Timings;

    fn call(&mut self, x: Self::CallType) {
        self.t.lock().unwrap().call(x);
    }

    fn finish(&mut self) -> Result<Timings> {
        Ok(Timings::new())
        //self.t.lock().unwrap().finish()
    }
}

struct DontFinish {
    t: Option<Box<WriteQuadTree>>,
}

impl DontFinish {
    pub fn new(t: Box<WriteQuadTree>) -> DontFinish {
        DontFinish { t: Some(t) }
    }
}

impl CallFinish for DontFinish {
    type CallType = <WriteQuadTree as CallFinish>::CallType;
    type ReturnType = Timings;

    fn call(&mut self, x: Self::CallType) {
        self.t.as_mut().unwrap().call(x);
    }

    fn finish(&mut self) -> Result<Timings> {
        let o = self.t.take().unwrap();
        let mut f = Timings::new();
        f.add_other("writequadtree", OtherData::WriteQuadTree(o));

        Ok(f)
    }
}

use std::marker::PhantomData;

struct FlattenCF<T, U> {
    out: Box<T>,
    x: PhantomData<U>,
}
impl<T, U> FlattenCF<T, U>
where
    T: CallFinish<CallType = U, ReturnType = Timings>,
    U: Sync + Send + 'static,
{
    pub fn new(out: Box<T>) -> FlattenCF<T, U> {
        FlattenCF {
            out: out,
            x: PhantomData,
        }
    }
}

impl<T, U> CallFinish for FlattenCF<T, U>
where
    T: CallFinish<CallType = U, ReturnType = Timings>,
    U: Sync + Send + 'static,
{
    type CallType = Vec<U>;
    type ReturnType = Timings;

    fn call(&mut self, us: Vec<U>) {
        for u in us {
            self.out.call(u);
        }
    }
    fn finish(&mut self) -> Result<Timings> {
        self.out.finish()
    }
}

fn find_node_quadtrees_flatvec(
    wqt: Box<WriteQuadTree>,
    nodewaynodes: NodeWayNodes,
    qts: Box<dyn QuadtreeGetSet>,
    nqts: Box<QuadtreeSimple>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> (
    Box<WriteQuadTree>,
    Box<dyn QuadtreeGetSet>,
    Box<QuadtreeSimple>,
) {
    let wqt_wrap = Box::new(DontFinish::new(wqt));
    let wqt_wrap2 = Box::new(FlattenCF::new(wqt_wrap));
    let eqt = Box::new(ExpandNodeQuadtree::new(
        qts, nqts, wqt_wrap2, qt_level, qt_buffer,
    ));

    let mut t = read_nodewaynodes(
        nodewaynodes,
        eqt,
        0,
        0,
        "find_node_quadtrees_flatvec",
        numchan,
    );

    message!("find_node_quadtrees_flatvec {}", t);
    let mut a: Option<Box<WriteQuadTree>> = None;
    let mut b: Option<Box<dyn QuadtreeGetSet>> = None;
    let mut c: Option<Box<QuadtreeSimple>> = None;
    for (x, y) in std::mem::take(&mut t.others) {
        match (x.as_str(), y) {
            ("writequadtree", OtherData::WriteQuadTree(wt)) => a = Some(wt),
            ("way_quadtrees", OtherData::QuadtreeGetSet(wq)) => b = Some(wq),
            ("node_quadtrees", OtherData::QuadtreeSimple(nq)) => c = Some(nq),
            _ => {}
        }
    }
    (a.unwrap(), b.unwrap(), c.unwrap())
}

fn find_node_quadtrees_simple(
    wqt: Box<WriteQuadTree>,
    nodewaynodes: NodeWayNodes,
    qts: Box<dyn QuadtreeGetSet>,
    nqts: Box<QuadtreeSimple>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> (
    Box<WriteQuadTree>,
    Box<dyn QuadtreeGetSet>,
    Box<QuadtreeSimple>,
) {
    let wqt_wrap = Box::new(DontFinish::new(wqt));
    let wqt_wrap2 = Box::new(FlattenCF::new(wqt_wrap));
    let eqt = Box::new(ExpandNodeQuadtree::new(
        qts, nqts, wqt_wrap2, qt_level, qt_buffer,
    ));

    let mut t = read_nodewaynodes(
        nodewaynodes,
        eqt,
        0,
        0,
        "find_node_quadtrees_simple",
        numchan,
    );

    message!("find_node_quadtrees_simple {}", t);
    let mut a: Option<Box<WriteQuadTree>> = None;
    let mut b: Option<Box<dyn QuadtreeGetSet>> = None;
    let mut c: Option<Box<QuadtreeSimple>> = None;
    for (x, y) in std::mem::take(&mut t.others) {
        match (x.as_str(), y) {
            ("writequadtree", OtherData::WriteQuadTree(wt)) => a = Some(wt),
            ("way_quadtrees", OtherData::QuadtreeGetSet(wq)) => b = Some(wq),
            ("node_quadtrees", OtherData::QuadtreeSimple(nq)) => c = Some(nq),
            _ => {}
        }
    }
    (a.unwrap(), b.unwrap(), c.unwrap())
}

fn calc_quadtrees_simple(
    nodewaynodes: NodeWayNodes,
    outfn: &str,
    relmems: Option<RelMems>,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
    lt: &mut LogTimes,
) {
    let mut nqts = Box::new(QuadtreeSimple::new());

    let relmems = match relmems {
        Some(mut rm) => {
            rm.unpack_stored(true, true);
            lt.add("unpack relmems");
            rm
        }
        None => {
            let relmfn = format!("{}-relmems", outfn);
            let rm = load_relmems(&relmfn, true, true);
            lt.add("load_relmems");
            rm
        }
    };

    for (_, b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }

    message!("expecting {} rel nodes qts", nqts.len());
    
    
    
    let qts = calc_way_quadtrees_simple(nodewaynodes.clone(), qt_level, qt_buffer, numchan)
        as Box<dyn QuadtreeGetSet>;
    message!("have {} way quadtrees", qts.len());
    lt.add("calc_way_quadtrees_simple");
    let writeqts = Box::new(WriteQuadTree::new(outfn));
    let (writeqts, qts, nqts) = find_node_quadtrees_simple(
        writeqts,
        nodewaynodes,
        qts,
        nqts,
        qt_level,
        qt_buffer,
        numchan,
    );
    lt.add("find_node_quadtrees_simple");
    write_ways_rels(writeqts, qts, nqts, relmems);
    lt.add("write_ways_rels");
}

fn calc_quadtrees_flatvec(
    nodewaynodes: NodeWayNodes,
    outfn: &str,
    relmems: Option<RelMems>,
    qt_level: usize,
    qt_buffer: f64,
    qinmem: bool,
    numchan: usize,
    ram_gb: usize,
    lt: &mut LogTimes,
) {
    //trim_memory();
    
    
    let splits = if ram_gb > 16 {
            vec![(0,0)]
        } else {
            vec![(0,350),(350,700),(700,0)]
        };
        
    
    let qts = if qinmem {
        calc_way_quadtrees_split_inmem(nodewaynodes.clone(), qt_level, qt_buffer, splits, numchan, ram_gb)
            as Box<dyn QuadtreeGetSet>
    } else {
        calc_way_quadtrees_split(nodewaynodes.clone(), outfn, qt_level, qt_buffer, splits, numchan, ram_gb)
            as Box<dyn QuadtreeGetSet>
    };
    lt.add("calc_way_quadtrees_split");
    message!("have {} way quadtrees", qts.len());
    
    let relmfn = format!("{}-relmems", &outfn);

    let nqts = match &relmems {
        Some(rm) => prep_relation_node_vals(&rm),
        None => {
            let rm = load_relmems(&relmfn, true, false);
            prep_relation_node_vals(&rm)
        }
    };

    message!("expecting {} rel nodes qts", nqts.len());
    lt.add("prep_relation_node_vals");
    let writeqts = Box::new(WriteQuadTree::new(outfn));
    let (writeqts, qts, nqts) = find_node_quadtrees_flatvec(
        writeqts,
        nodewaynodes,
        qts,
        nqts,
        qt_level,
        qt_buffer,
        numchan,
    );
    //trim_memory();
    lt.add("find_node_quadtrees_flatvec");

    let rm = match relmems {
        Some(mut rm) => {
            rm.unpack_stored(true, true);
            lt.add("relmems.unpack_stored");
            rm
        }
        None => {
            let rm = load_relmems(&relmfn, true, true);
            lt.add("load_relmems");
            rm
        }
    };

    //let relmems = load_relmems(&relmfn, true, true);
    write_ways_rels(writeqts, qts, nqts, rm);
    lt.add("write_ways_rels");
}

fn write_ways_rels(
    writeqts: Box<WriteQuadTree>,
    qts: Box<dyn QuadtreeGetSet>,
    nqts: Box<QuadtreeSimple>,
    relmems: RelMems,
) {
    message!("write {} way qts", qts.len());
    let mut allqts = PackQuadtrees::new(writeqts, 50000);
    for (w, q) in qts.items() {
        allqts.add_way(w, q);
    }

    message!("prep rel qts");
    let mut rqts = QuadtreeSimple::new();

    for (a, c) in &relmems.nodes {
        match nqts.get(*c) {
            Some(q) => {
                rqts.expand(*a, q);
            }
            None => {
                message!("missing node {}", *c);
            }
        }
    }

    message!("have {} rel qts", rqts.len());

    let mut nmw = 0;
    for (a, c) in &relmems.ways {
        match qts.get(*c) {
            Some(q) => {
                rqts.expand(*a, q);
            }
            None => {
                if nmw < 5 || (nmw % 18451) == 0 {
                    message!("missing way {}: {} for {}", nmw, *c, *a);
                }
                nmw += 1;
            }
        }
    }
    message!("missing {} ways", nmw);
    message!("have {} rel qts", rqts.len());
    message!("and {} empty rels", relmems.empty_rels.len());
    for r in &relmems.empty_rels {
        rqts.expand(*r, Quadtree::new(0));
    }

    message!("and {} rel rels", relmems.relations.len());
    let mut sn = 0;
    for i in 0..5 {
        for (a, b) in &relmems.relations {
            match rqts.get(*b) {
                None => {
                    if i == 4 {
                        //message!("no rel??");
                        message!("{} missing rel {} for {}", sn, b, a);
                        sn += 1;
                        rqts.expand(*a, Quadtree::new(0));
                    }
                }
                Some(q) => {
                    rqts.expand(*a, q);
                }
            }
        }
    }
    message!("{} missing parent rels?", sn);

    message!("have {} rel qts", rqts.len());
    let mut nneg = 0;
    for (r, q) in rqts.items() {
        if q.as_int() < 0 {
            allqts.add_relation(r, Quadtree::new(0));
            nneg += 1;
        } else {
            allqts.add_relation(r, q);
        }
    }
    message!("replaced {} neg qt rels with 0", nneg);
    allqts.finish();
}

fn load_relmems(relmfn: &str, load_nodes: bool, load_others: bool) -> RelMems {
    let mut f = File::open(&relmfn).expect("couldn't open relmems file");

    let mut relmems = RelMems::new();
    for fb in ReadFileBlocks::new(&mut f) {
        relmems.unpack(&fb.data(), load_nodes, load_others);
    }

    message!("read relmems: {}", relmems);
    relmems
}

fn write_relmems(mut relmems: RelMems, relmfn: &str) -> Result<()> {
    let mut wf = WriteFile::new(&relmfn, HeaderType::None);
    for p in std::mem::take(&mut relmems.packed) {
        wf.call(vec![(0, p)]);
    }
    wf.finish()?;
    Ok(())
}
pub fn run_calcqts_load_existing(
    fname: &str,
    outfn: Option<&str>,
    qt_level: usize,
    qt_buffer: f64,
    stop_at: Option<u64>,
    numchan: usize,
) -> Result<()> {
    let mut lt = LogTimes::new();
    let outfn_ = match outfn {
        Some(o) => String::from(o),
        None => format!("{}-qts.pbf", &fname[0..fname.len() - 4]),
    };
    let outfn = &outfn_;

    let relmfn = format!("{}-relmems", &outfn);
    let relmems = load_relmems(&relmfn, true, false);
    /*let mut nqts = Box::new(QuadtreeSimple::new());

    for (_,b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }
    drop(relmems);*/
    let nqts = prep_relation_node_vals(&relmems);
    drop(relmems);

    message!("expecting {} rel nodes qts", nqts.len());
    lt.add("prep_relation_node_vals");

    let nodewaynodes = NodeWayNodes::Seperate(
        String::from(fname),
        format!("{}-waynodes", outfn),
        Vec::new(),
        match stop_at {
            Some(s) => s,
            None => file_length(fname),
        },
    );

    calc_quadtrees_flatvec(
        nodewaynodes,
        outfn,
        None,
        qt_level,
        qt_buffer,
        true, //seperate,
        numchan,
        8,
        &mut lt,
    );

    /*
    let qts = calc_way_quadtrees_split_inmem(nodewaynodes.clone(), qt_level, qt_buffer, numchan)
        as Box<dyn QuadtreeGetSet>;

    message!("have {} way quadtrees", qts.len());

    let writeqts = Box::new(WriteQuadTree::new(&outfn));
    let (writeqts, qts, nqts) = find_node_quadtrees_flatvec(
        writeqts,
        nodewaynodes,
        qts,
        nqts,
        qt_level,
        qt_buffer,
        numchan,
    );

    let relmems = load_relmems(&relmfn, true, true);
    write_ways_rels(writeqts, qts, nqts, relmems);
    */
    message!("{}", lt);
    Ok(())
}

pub fn run_calcqts_prelim(fname: &str, outfn: Option<&str>, numchan: usize) -> Result<()> {
    let mut lt = LogTimes::new();

    let outfn_ = match outfn {
        Some(o) => String::from(o),
        None => format!("{}-qts.pbf", &fname[0..fname.len() - 4]),
    };
    let outfn = &outfn_;

    let (relmems, waynodevals, first_waytile_pos) = prep_way_nodes_tempfile(fname, outfn, numchan)?;

    lt.add("prep_way_nodes");

    let relmfn = format!("{}-relmems", &outfn);
    write_relmems(relmems, &relmfn)?;

    message!("stop reading {} after {}", &fname, first_waytile_pos + 1);

    write_waynode_sorted_resort(waynodevals, outfn);
    lt.add("write_waynode_sorted_resort");

    message!("{}", lt);
    Ok(())
}

pub fn run_calcqts(
    fname: &str,
    outfn: Option<&str>,
    qt_level: usize,
    qt_buffer: f64,
    mode: Option<&str>,
    //seperate: bool,
    //resort_waynodes: bool,
    numchan: usize,
    ram_gb: usize,
) -> Result<LogTimes> {
    let mut use_simple = false;
    let fl = file_length(fname) / 1024 / 1024;
    match mode {
        None => {
            if fl < 512 {
                return run_calcqts_inmem(fname, outfn, qt_level, qt_buffer, numchan);
            } else if fl < 4096 {
                use_simple = true;
            }
        }
        Some("INMEM") => {
            return run_calcqts_inmem(fname, outfn, qt_level, qt_buffer, numchan);
        }

        Some("SIMPLE") => {
            use_simple = true;
        }
        Some("FLATVEC") => {}
        Some(x) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("unexpected mode {}", x),
            ));
        }
    }

    let mut lt = LogTimes::new();

    let outfn_ = match outfn {
        Some(o) => String::from(o),
        None => format!("{}-qts.pbf", &fname[0..fname.len() - 4]),
    };
    let outfn = &outfn_;

    if use_simple && fl > (ram_gb as u64)*1024 {
        return Err(Error::new(
            ErrorKind::Other,
            format!("run_calcqts mode = SIMPLE only suitable for pbf files smaller than {}gb",ram_gb),
        ));
    }
    /*if !use_simple && fl < 2048 {
        return Err(Error::new(
            ErrorKind::Other,
            "run_calcqts mode = FLATVEC has no advantages for pbf files smaller than 2gb",
        ));
    }*/
    let relmfn = format!("{}-relmems", &outfn);

    let (relmems, waynodevals, first_waytile_pos) = if fl > 4096 * (ram_gb as u64) {
        let (rl, wn, fw) = prep_way_nodes_tempfile(fname, outfn, numchan)?;
        write_relmems(rl, &relmfn)?;
        (None, wn, fw)
    } else {
        let (rl, wn, fw) = prep_way_nodes(fname, numchan)?;
        (Some(rl), wn, fw)
    };

    lt.add("prep_way_nodes");

    message!("stop reading {} after {}", &fname, first_waytile_pos + 1);

    //trim_memory();
    if use_simple {
        let nodewaynodes = NodeWayNodes::InMem(
            String::from(fname),
            Arc::new(waynodevals),
            first_waytile_pos + 1,
        );
        calc_quadtrees_simple(
            nodewaynodes,
            outfn,
            relmems,
            qt_level,
            qt_buffer,
            numchan,
            &mut lt,
        );
    } else {
        
        let nodewaynodes = if ram_gb > 32 {
            NodeWayNodes::InMem(
                String::from(fname),
                Arc::new(waynodevals),
                first_waytile_pos + 1,
            )
        } else {
        
            let a = write_waynode_sorted_resort(waynodevals, outfn);
            lt.add("write_waynode_sorted_resort");
            NodeWayNodes::Seperate(String::from(fname), a, vec![], first_waytile_pos + 1)
        };

        //trim_memory();
        calc_quadtrees_flatvec(
            nodewaynodes,
            outfn,
            relmems,
            qt_level,
            qt_buffer,
            true, //seperate,
            numchan,
            ram_gb,
            &mut lt,
        );
    }
    //message!("{}", lt);
    Ok(lt)
}
