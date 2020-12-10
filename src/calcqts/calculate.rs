use std::fs::File;

use std::io::{BufReader, Error, ErrorKind, Result};

use std::collections::BTreeMap;

use std::sync::{Arc, Mutex};

use crate::pbfformat::read_file_block;
use crate::pbfformat::read_file_block::{file_length, pack_file_block, ProgBarWrap};
use crate::pbfformat::read_pbf;

use crate::callback::{CallFinish, Callback, CallbackSync};
use crate::elements::Quadtree;
use crate::elements::QuadtreeBlock;
use crate::pbfformat::header_block::HeaderType;
use crate::pbfformat::writefile::WriteFile;

use crate::utils::{trim_memory, CallAll, ReplaceNoneWithTimings, Timer};

use crate::calcqts::expand_wayboxes::{WayBoxesSimple, WayBoxesSplit, WayBoxesVec};
use crate::calcqts::node_waynodes::{
    read_nodewaynodes, write_nodewaynode_file, write_waynode_sorted, write_waynode_sorted_resort,
    NodeWayNodeCombTile,
};
use crate::calcqts::packwaynodes::{prep_relation_node_vals, prep_way_nodes, RelMems};
use crate::calcqts::quadtree_store::{
    QuadtreeGetSet, QuadtreeSimple, QuadtreeSplit, QuadtreeTileInt, WAY_SPLIT_VAL,
};
use crate::calcqts::write_quadtrees::{PackQuadtrees, WrapWriteFile, WriteQuadTree};
use crate::calcqts::{NodeWayNodes, OtherData, Timings};

fn calc_way_quadtrees_simple(
    nodewaynodes: NodeWayNodes,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Box<QuadtreeSimple> {
    let wb = Box::new(WayBoxesSimple::new(qt_level, qt_buffer));

    let t = read_nodewaynodes(nodewaynodes, wb, 0, 0, "calc_way_quadtrees_simple", numchan);

    println!("calc_way_quadtrees_simple {}", t);
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
    qt_buffer: f64,
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

    let mut pg = ProgBarWrap::new(100);
    pg.set_range(100);
    pg.set_message(&format!(
        "calc quadtrees {} tiles [{}mb]",
        tts.len(),
        tts.len() * WAY_SPLIT_VAL / 1024 / 1024
    ));
    let mut i = 0;
    let pf = 100.0 / (tts.len() as f64);
    for (_, t) in tts {
        pg.prog((i as f64) * pf);

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
    qt_buffer: f64,
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

    let mut pg = ProgBarWrap::new(100);
    pg.set_range(100);
    pg.set_message("calc quadtrees");
    let mut i = 0;
    let pf = 100.0 / (tts.len() as f64);
    for (_, t) in tts {
        pg.prog((i as f64) * pf);

        v[i % 4].call(t);
        i += 1;
    }
    for mut vi in v {
        vi.finish().expect("?");
    }
    pg.finish();
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
) -> usize {
    let wb = Box::new(WayBoxesSplit::new());

    let mut t = read_nodewaynodes(
        nodewaynodes,
        wb,
        minw,
        maxw,
        &format!("calc_way_quadtrees_split {} to {}", minw, maxw),
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
) -> usize {
    let wb = Box::new(WayBoxesSplit::new());

    let mut t = read_nodewaynodes(
        nodewaynodes,
        wb,
        minw,
        maxw,
        &format!("calc_way_quadtrees_split {} to {}", minw, maxw),
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
    numchan: usize,
) -> Box<QuadtreeSplit> {
    let tempfn = format!("{}-wayqts", outfn);
    let wf = Arc::new(Mutex::new(Box::new(WrapWriteFile::new(WriteFile::new(
        &tempfn,
        HeaderType::None,
    )))));

    //let mut qts = Box::new(QuadtreeSplit::new());

    calc_way_quadtrees_split_part(
        nodewaynodes.clone(),
        0,
        350i64 << 20,
        wf.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();
    calc_way_quadtrees_split_part(
        nodewaynodes.clone(),
        350i64 << 20,
        700i64 << 20,
        wf.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();
    calc_way_quadtrees_split_part(
        nodewaynodes.clone(),
        700i64 << 20,
        0,
        wf.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();
    wf.lock().unwrap().finish().expect("?");
    load_way_qts(&tempfn)
}

fn calc_way_quadtrees_split_inmem(
    nodewaynodes: NodeWayNodes,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) -> Box<QuadtreeSplit> {
    let qts = Arc::new(Mutex::new(Box::new(QuadtreeSplit::new())));
    calc_way_quadtrees_split_part_inmem(
        nodewaynodes.clone(),
        0,
        350i64 << 20,
        qts.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();
    calc_way_quadtrees_split_part_inmem(
        nodewaynodes.clone(),
        350i64 << 20,
        700i64 << 20,
        qts.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();
    calc_way_quadtrees_split_part_inmem(
        nodewaynodes.clone(),
        700i64 << 20,
        0,
        qts.clone(),
        qt_level,
        qt_buffer,
        numchan,
    );
    trim_memory();

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

fn read_quadtree_block_ways(data: Vec<u8>, res: &mut Box<QuadtreeSplit>) {
    for x in read_pbf::IterTags::new(&data, 0) {
        match x {
            read_pbf::PbfTag::Data(2, d) => {
                for y in read_pbf::IterTags::new(&d, 0) {
                    match y {
                        read_pbf::PbfTag::Data(3, d) => {
                            let mut i = 0;
                            let mut q = Quadtree::new(-1);
                            for z in read_pbf::IterTags::new(&d, 0) {
                                match z {
                                    read_pbf::PbfTag::Value(1, v) => {
                                        i = v as i64;
                                    }
                                    read_pbf::PbfTag::Value(20, v) => {
                                        q = Quadtree::new(read_pbf::un_zig_zag(v));
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

    for bl in read_file_block::ReadFileBlocks::new(&mut fbuf) {
        if bl.block_type == "OSMData" {
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
    type CallType = NodeWayNodeCombTile;
    //type ReturnType = (T::ReturnType, Box<dyn QuadtreeGetSet>, QuadtreeSimple);
    type ReturnType = Timings;

    fn call(&mut self, nn: NodeWayNodeCombTile) {
        let tx = Timer::new();
        if nn.vals.is_empty() {
            return;
        }

        //let mut bl = Box::new(QuadtreeBlock::with_capacity(nn.vals.len()));
        let mut bl = Vec::new();
        for n in nn.vals {
            let q = if n.ways.is_empty() {
                Quadtree::calculate_point(n.lon, n.lat, self.qt_level, self.qt_buffer)
            } else {
                let mut q = Quadtree::new(-1);
                for wi in n.ways {
                    match self.wayqts.as_ref().unwrap().get(wi) {
                        None => {
                            println!("missing way {} for node {}", wi, n.id)
                        }
                        Some(qi) => {
                            q = q.common(&qi);
                        }
                    }
                }
                q
            };
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

    println!("find_node_quadtrees_flatvec {}", t);
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

    println!("find_node_quadtrees_simple {}", t);
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
    mut relmems: RelMems,
    qt_level: usize,
    qt_buffer: f64,
    numchan: usize,
) {
    let mut nqts = Box::new(QuadtreeSimple::new());

    relmems.unpack_stored(true, true);

    for (_, b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }

    println!("expecting {} rel nodes qts", nqts.len());

    let qts = calc_way_quadtrees_simple(nodewaynodes.clone(), qt_level, qt_buffer, numchan)
        as Box<dyn QuadtreeGetSet>;
    println!("have {} way quadtrees", qts.len());

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

    write_ways_rels(writeqts, qts, nqts, relmems);
}

fn calc_quadtrees_flatvec(
    nodewaynodes: NodeWayNodes,
    outfn: &str,
    mut relmems: RelMems,
    qt_level: usize,
    qt_buffer: f64,
    qinmem: bool,
    numchan: usize,
) {
    trim_memory();

    let qts = if qinmem {
        calc_way_quadtrees_split_inmem(nodewaynodes.clone(), qt_level, qt_buffer, numchan)
            as Box<dyn QuadtreeGetSet>
    } else {
        calc_way_quadtrees_split(nodewaynodes.clone(), outfn, qt_level, qt_buffer, numchan)
            as Box<dyn QuadtreeGetSet>
    };

    println!("have {} way quadtrees", qts.len());

    let nqts = prep_relation_node_vals(&relmems);
    println!("expecting {} rel nodes qts", nqts.len());

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
    trim_memory();
    relmems.unpack_stored(true, true);
    //let relmems = load_relmems(&relmfn, true, true);
    write_ways_rels(writeqts, qts, nqts, relmems);
}

fn write_ways_rels(
    writeqts: Box<WriteQuadTree>,
    qts: Box<dyn QuadtreeGetSet>,
    nqts: Box<QuadtreeSimple>,
    relmems: RelMems,
) {
    println!("write {} way qts", qts.len());
    let mut allqts = PackQuadtrees::new(writeqts, 50000);
    for (w, q) in qts.items() {
        allqts.add_way(w, q);
    }

    println!("prep rel qts");
    let mut rqts = QuadtreeSimple::new();

    for (a, c) in &relmems.nodes {
        match nqts.get(*c) {
            Some(q) => {
                rqts.expand(*a, q);
            }
            None => {
                println!("missing node {}", *c);
            }
        }
    }

    println!("have {} rel qts", rqts.len());

    let mut nmw = 0;
    for (a, c) in &relmems.ways {
        match qts.get(*c) {
            Some(q) => {
                rqts.expand(*a, q);
            }
            None => {
                if nmw < 5 || (nmw % 18451) == 0 {
                    println!("missing way {}: {} for {}", nmw, *c, *a);
                }
                nmw += 1;
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
    let mut sn = 0;
    for i in 0..5 {
        for (a, b) in &relmems.relations {
            match rqts.get(*b) {
                None => {
                    if i == 4 {
                        //println!("no rel??");
                        println!("{} missing rel {} for {}", sn, b, a);
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
    println!("{} missing parent rels?", sn);

    println!("have {} rel qts", rqts.len());
    let mut nneg = 0;
    for (r, q) in rqts.items() {
        if q.as_int() < 0 {
            allqts.add_relation(r, Quadtree::new(0));
            nneg += 1;
        } else {
            allqts.add_relation(r, q);
        }
    }
    println!("replaced {} neg qt rels with 0", nneg);
    allqts.finish();
}

fn load_relmems(relmfn: &str, load_nodes: bool, load_others: bool) -> RelMems {
    let mut f = File::open(&relmfn).expect("couldn't open relmems file");

    let mut relmems = RelMems::new();
    for fb in read_file_block::ReadFileBlocks::new(&mut f) {
        relmems.unpack(&fb.data(), load_nodes, load_others);
    }

    println!("read relmems: {}", relmems);
    relmems
}

pub fn run_calcqts_load_existing(
    fname: &str,
    outfn: &str,
    qt_level: usize,
    qt_buffer: f64,
    seperate: bool,
    numchan: usize,
) -> Result<()> {
    let relmfn = format!("{}-relmems", &outfn);
    let relmems = load_relmems(&relmfn, true, false);
    /*let mut nqts = Box::new(QuadtreeSimple::new());

    for (_,b) in &relmems.nodes {
        nqts.set(*b, Quadtree::new(-1));
    }
    drop(relmems);*/
    let nqts = prep_relation_node_vals(&relmems);
    drop(relmems);

    println!("expecting {} rel nodes qts", nqts.len());
    let nodewaynodes = if seperate {
        NodeWayNodes::Seperate(
            String::from(fname),
            format!("{}-waynodes", outfn),
            Vec::new(),
        )
    } else {
        NodeWayNodes::Combined(format!("{}-nodewaynodes", outfn))
    };

    let qts = calc_way_quadtrees_split_inmem(nodewaynodes.clone(), qt_level, qt_buffer, numchan)
        as Box<dyn QuadtreeGetSet>;

    println!("have {} way quadtrees", qts.len());

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

    Ok(())
}

pub fn run_calcqts(
    fname: &str,
    outfn: Option<&str>,
    qt_level: usize,
    qt_buffer: f64,
    use_simple: bool,
    seperate: bool,
    resort_waynodes: bool,
    numchan: usize,
) -> Result<()> {
    let outfn_ = match outfn {
        Some(o) => String::from(o),
        None => format!("{}-qts.pbf", &fname[0..fname.len() - 4]),
    };
    let outfn = &outfn_;

    if use_simple && file_length(fname) > 8 * 1024 * 1024 * 1024 {
        return Err(Error::new(
            ErrorKind::Other,
            "run_calcqts use_simple=true only suitable for pbf files smaller than 8gb",
        ));
    }

    let (relmems, waynodevals) = prep_way_nodes(fname, numchan).expect("prep_way_nodes failed");

    let nodewaynodes = NodeWayNodes::InMem(String::from(fname), waynodevals);
    trim_memory();
    if use_simple {
        calc_quadtrees_simple(nodewaynodes, outfn, relmems, qt_level, qt_buffer, numchan);
    } else {
        let nodewaynodes2 = if seperate {
            match nodewaynodes {
                NodeWayNodes::InMem(inf, w) => {
                    //let (a,b) = write_waynode_sorted(w,&outfn);
                    if resort_waynodes {
                        let a = write_waynode_sorted_resort(w, outfn);
                        NodeWayNodes::Seperate(inf, a, vec![])
                    } else {
                        let (a, b) = write_waynode_sorted(w, outfn);
                        NodeWayNodes::Seperate(inf, a, b)
                    }
                }
                p => p,
            }
        } else {
            write_nodewaynode_file(nodewaynodes, outfn)
        };
        trim_memory();
        calc_quadtrees_flatvec(
            nodewaynodes2,
            outfn,
            relmems,
            qt_level,
            qt_buffer,
            seperate,
            numchan,
        );
    }

    Ok(())
}
