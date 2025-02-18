use channelled_callbacks::{CallFinish, Callback, CallbackMerge, MergeTimings, Timings, Result as ccResult};
use crate::elements::{
    Bbox, Changetype, ElementType, IdSetSet, Node, PrimitiveBlock, Quadtree, Relation, Way,
};
use crate::pbfformat::{
    pack_file_block, read_all_blocks_locs_prog, read_file_block_with_pos,
    FileBlock, HeaderBlock,FilelistEntry,CompressionType
};
use crate::sortblocks::{QuadtreeTree, WriteFileInternalLocs};

use crate::update::{check_index_file, read_xml_change, ChangeBlock};
use crate::utils::{ThreadTimer, Timer, Error, Result, at_end_of_file};
use crate::{message,progress_percent};
use crate::logging::{ProgressPercent};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::File;
use std::io::{BufReader};
use std::sync::Arc;


pub struct OrigData {
    pub node_qts: BTreeMap<i64, (Quadtree, Quadtree)>,
    pub way_qts: BTreeMap<i64, (Quadtree, Quadtree)>,
    pub relation_qts: BTreeMap<i64, (Quadtree, Quadtree)>,
    pub othernodes: BTreeMap<i64, Option<Node>>,
}
impl OrigData {
    pub fn new() -> OrigData {
        OrigData {
            node_qts: BTreeMap::new(),
            way_qts: BTreeMap::new(),
            relation_qts: BTreeMap::new(),
            othernodes: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, pb: PrimitiveBlock, idset: &IdSetSet) {
        for n in pb.nodes {
            match n.changetype {
                Changetype::Normal
                | Changetype::Unchanged
                | Changetype::Modify
                | Changetype::Create => {
                    self.node_qts.insert(n.id, (n.quadtree, pb.quadtree));
                    if idset.is_exnode(n.id) {
                        let mut n = n.clone();
                        n.changetype = Changetype::Normal;
                        self.othernodes.insert(n.id, Some(n));
                    }
                }
                Changetype::Delete => {
                    self.node_qts
                        .insert(n.id, (Quadtree::empty(), Quadtree::empty()));
                    if idset.is_exnode(n.id) {
                        self.othernodes.insert(n.id, None);
                    }
                }
                _ => {}
            }
        }
        for w in pb.ways {
            match w.changetype {
                Changetype::Normal
                | Changetype::Unchanged
                | Changetype::Modify
                | Changetype::Create => {
                    self.way_qts.insert(w.id, (w.quadtree, pb.quadtree));
                }
                Changetype::Delete => {
                    self.way_qts
                        .insert(w.id, (Quadtree::empty(), Quadtree::empty()));
                }
                _ => {}
            }
        }

        for r in pb.relations {
            match r.changetype {
                Changetype::Normal
                | Changetype::Unchanged
                | Changetype::Modify
                | Changetype::Create => {
                    self.relation_qts.insert(r.id, (r.quadtree, pb.quadtree));
                }
                Changetype::Delete => {
                    self.relation_qts
                        .insert(r.id, (Quadtree::empty(), Quadtree::empty()));
                }
                _ => {}
            }
        }
    }

    pub fn extend(&mut self, other: OrigData) {
        self.node_qts.extend(other.node_qts);
        self.way_qts.extend(other.way_qts);
        self.relation_qts.extend(other.relation_qts);
        self.othernodes.extend(other.othernodes);
    }

    fn get_quadtree(&self, t: &ElementType, r: &i64) -> Option<Quadtree> {
        match *t {
            ElementType::Node => get_quadtree(&self.node_qts, r),
            ElementType::Way => get_quadtree(&self.way_qts, r),
            ElementType::Relation => get_quadtree(&self.relation_qts, r),
            _ => None
        }
    }

    fn get_alloc(&self, t: &ElementType, r: &i64) -> Option<Quadtree> {
        match t {
            ElementType::Node => get_alloc(&self.node_qts, r),
            ElementType::Way => get_alloc(&self.way_qts, r),
            ElementType::Relation => get_alloc(&self.relation_qts, r),
            _ => None
        }
    }

    fn expand_node(&mut self, r: &i64, q: Quadtree) {
        expand_quadtree(&mut self.node_qts, r, q);
    }

    /*fn set_node(&mut self, r: &i64, q: Quadtree) {
        replace_quadtree(&mut self.node_qts, r, q);
    }*/
    fn set_way(&mut self, r: &i64, q: Quadtree) {
        replace_quadtree(&mut self.way_qts, r, q);
    }

    fn expand_relation(&mut self, r: &i64, q: Quadtree) {
        expand_quadtree(&mut self.relation_qts, r, q);
    }

    fn set_relation(&mut self, r: &i64, q: Quadtree) {
        replace_quadtree(&mut self.relation_qts, r, q);
    }
}

fn expand_quadtree(curr: &mut BTreeMap<i64, (Quadtree, Quadtree)>, i: &i64, q: Quadtree) {
    match curr.get_mut(i) {
        None => {
            curr.insert(*i, (q, Quadtree::empty()));
        }
        Some(p) => {
            p.0 = p.0.common(&q);
        }
    }
}
fn replace_quadtree(curr: &mut BTreeMap<i64, (Quadtree, Quadtree)>, i: &i64, q: Quadtree) {
    match curr.get_mut(i) {
        None => {
            curr.insert(*i, (q, Quadtree::empty()));
        }
        Some(p) => {
            p.0 = q;
        }
    }
}
fn get_quadtree(curr: &BTreeMap<i64, (Quadtree, Quadtree)>, i: &i64) -> Option<Quadtree> {
    match curr.get(i) {
        Some(q) => {
            if q.0.as_int() < 0 {
                None
            } else {
                Some(q.0)
            }
        }
        None => None,
    }
}
fn get_alloc(curr: &BTreeMap<i64, (Quadtree, Quadtree)>, i: &i64) -> Option<Quadtree> {
    match curr.get(i) {
        Some(q) => {
            if q.1.as_int() < 0 {
                None
            } else {
                Some(q.1)
            }
        }
        None => None,
    }
}

impl fmt::Display for OrigData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OrigData {:10} node qts, {:8} way qts, {:7} rel qts, {:8} other nodes",
            self.node_qts.len(),
            self.way_qts.len(),
            self.relation_qts.len(),
            self.othernodes.len()
        )
    }
}

struct ReadPB {
    origdata: Option<OrigData>,

    ids: Arc<IdSetSet>,
    ischange: bool,

    tm: f64,
}
impl ReadPB {
    pub fn new(ischange: bool, ids: Arc<IdSetSet>) -> ReadPB {
        ReadPB {
            origdata: Some(OrigData::new()),
            ids: ids,
            ischange: ischange,
            tm: 0.0,
        }
    }
}

impl CallFinish for ReadPB {
    type CallType = (usize, FileBlock);
    type ReturnType = Timings<OrigData>;    
    type ErrorType = Error;

    fn call(&mut self, idx_blocks: (usize, FileBlock)) {
        let tx = ThreadTimer::new();
        let b = PrimitiveBlock::read_check_ids(
            idx_blocks.0 as i64,
            idx_blocks.1.pos,
            &idx_blocks.1.data(),
            self.ischange,
            false,
            Some(self.ids.as_ref()),
        )
        .expect("?");

        self.origdata.as_mut().unwrap().add(b, self.ids.as_ref());

        self.tm += tx.since();
    }
    fn finish(&mut self) -> ccResult<Self::ReturnType, Self::ErrorType> {
        let mut tm = Timings::new(); //self.out.finish()?;
        tm.add("read_primitive_blocks_combine", self.tm);
        tm.add_other("origdata", self.origdata.take().unwrap());
        Ok(tm)
    }
}

fn read_change_tiles(
    fname: &str,
    tiles: &BTreeSet<Quadtree>,
    idset: Arc<IdSetSet>,
    numchan: usize,
    pb: &Box<dyn ProgressPercent>,
    start_percent: f64,
    end_percent: f64
) -> Result<(OrigData, f64)> {
    let ischange = fname.ends_with(".pbfc");
    let mut file = File::open(fname)?;
    let (p, fb) = read_file_block_with_pos(&mut file, 0)?;
    if fb.block_type != "OSMHeader" {
        return Err(Error::PbfDataError("first block not an OSMHeader".to_string()));
    }
    let head = HeaderBlock::read(p, &fb.data(), fname)?;
    if head.index.is_empty() {
        if at_end_of_file(&mut file)? {
            message!("file {} empty", fname);
        } else {
            return Err(Error::PbfDataError(format!("file {} has no locs in header", fname)));
        }
    }
    let mut locs = Vec::new();

    for ii in &head.index {
        if tiles.contains(&ii.quadtree) {
            locs.push(ii.location);
        }
    }
    let (mut tm, b) = if numchan == 0 {
        let convert = Box::new(ReadPB::new(ischange, idset));
        read_all_blocks_locs_prog(&mut file, fname, locs, convert, pb, start_percent, end_percent)
    } else {
        let mut convs: Vec<
            Box<dyn CallFinish<CallType = (usize, FileBlock), ReturnType = Timings<OrigData>, ErrorType = Error>>,
        > = Vec::new();
        for _ in 0..numchan {
            convs.push(Box::new(Callback::new(Box::new(ReadPB::new(
                ischange,
                idset.clone(),
            )))));
        }
        let convsm = Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())));
        read_all_blocks_locs_prog(&mut file, fname, locs, convsm, pb, start_percent, end_percent)
    };

    let mut tls = tm.others.pop().unwrap().1;
    while !tm.others.is_empty() {
        tls.extend(tm.others.pop().unwrap().1);
    }
    Ok((tls, b))
}

fn collect_existing(
    prfx: &str,
    filelist: &Vec<FilelistEntry>,
    idset: Arc<IdSetSet>,
    numchan: usize,
) -> Result<(OrigData, f64, f64)> {
    let mut origdata = OrigData::new();
    let mut total_scan = 0.0;
    let mut total_read = 0.0;


    let pf = 100.0 / (147 + 388 + (filelist.len() as u64 - 1) * 2) as f64;
    let pb = progress_percent!(&format!("read from {} files", filelist.len()));
    
    
    
    for (i, fle) in filelist.iter().enumerate() {
        let nc = if i == 0 { numchan } else { 1 };

        let fnameidx = format!("{}{}-index.pbf", prfx, fle.filename);

        let (start_percent, mid_percent, end_percent) = if i == 0 {
            (0.0, 147.0 * pf, 535.0 * pf)
        } else {
            ((533 + 2*i) as f64 * pf, (534 + 2*i) as f64 * pf, (535 + 2*i) as f64 * pf)
        };
        let (a, c) = check_index_file(&fnameidx, idset.clone(), nc, Some((&pb, start_percent, mid_percent)))?;
        total_scan += c;
        
        let mut ctiles = BTreeSet::new();
        ctiles.extend(a);

        let fname = format!("{}{}", prfx, fle.filename);
        
        let (bb, t) = read_change_tiles(&fname, &ctiles, idset.clone(), nc, &pb, mid_percent, end_percent)?;
        
        
        total_read += t;
        origdata.extend(bb);
    }
    pb.finish();
    Ok((origdata, total_scan, total_read))
}

fn find_tile(tree: &QuadtreeTree, q: Option<Quadtree>) -> Option<Quadtree> {
    match q {
        None => None,
        Some(q) => {
            let (_, t) = tree.find(&q);
            Some(t.qt)
        }
    }
}

struct AllocBlocks {
    st: i64,
    et: i64,
    pub blocks: BTreeMap<Quadtree, PrimitiveBlock>,
}

impl AllocBlocks {
    pub fn new(st: i64, et: i64) -> AllocBlocks {
        AllocBlocks {
            st: st,
            et: et,
            blocks: BTreeMap::new(),
        }
    }

    fn add_node(&mut self, qt: Quadtree, nd: Node) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0, 0);
                pb.quadtree = qt.clone();
                pb.start_date = self.st;
                pb.end_date = self.et;
                pb.nodes.push(nd);
                self.blocks.insert(qt, pb);
            }
            Some(pb) => {
                pb.nodes.push(nd);
            }
        }
    }
    fn add_way(&mut self, qt: Quadtree, wy: Way) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0, 0);
                pb.quadtree = qt.clone();
                pb.start_date = self.st;
                pb.end_date = self.et;
                pb.ways.push(wy);
                self.blocks.insert(qt, pb);
            }
            Some(pb) => {
                pb.ways.push(wy);
            }
        }
    }
    fn add_relation(&mut self, qt: Quadtree, rl: Relation) {
        match self.blocks.get_mut(&qt) {
            None => {
                let mut pb = PrimitiveBlock::new(0, 0);
                pb.quadtree = qt.clone();
                pb.start_date = self.st;
                pb.end_date = self.et;
                pb.relations.push(rl);
                self.blocks.insert(qt, pb);
            }
            Some(pb) => {
                pb.relations.push(rl);
            }
        }
    }
}
const MISSING_NODES_LIMIT: usize = 0;
fn calc_qts(
    changeblock: &ChangeBlock,
    orig_data: &mut OrigData,
    tree: &QuadtreeTree,
    maxlevel: usize,
    buffer: f64,
    st: i64,
    et: i64,
) -> Result<BTreeMap<Quadtree, PrimitiveBlock>> {
    let mut nq = BTreeSet::new();
    let mut rel_rels = Vec::new();
    let mut missing_nodes = 0;
    for (_, w) in changeblock.ways.iter() {
        if w.changetype != Changetype::Delete {
            let mut bbox = Bbox::empty();
            for r in w.refs.iter() {
                match changeblock.nodes.get(&r) {
                    Some(n) => {
                        if n.changetype == Changetype::Delete {
                            message!(
                                "[{}] missing node deleted {:?} from {}",
                                missing_nodes,
                                n,
                                &format!("{:?}", w)[0..100]
                            );
                            missing_nodes += 1;
                            if missing_nodes > MISSING_NODES_LIMIT {
                                return Err(Error::PbfDataError("too many missing nodes".to_string()));
                            }
                        } else {
                            bbox.expand(n.lon, n.lat);
                        }
                    }
                    None => {
                        message!("[{}] missing node {} from {:?}", missing_nodes, r, w);
                        missing_nodes += 1;
                        if missing_nodes > MISSING_NODES_LIMIT {
                            return Err(Error::PbfDataError("too many missing nodes".to_string()));
                        }
                    }
                }
            }

            let q = Quadtree::calculate(&bbox, maxlevel, buffer);
            orig_data.set_way(&w.id, q);

            for r in w.refs.iter() {
                orig_data.expand_node(r, q);
                nq.insert(r);
            }
        }
    }

    for (_, n) in changeblock.nodes.iter() {
        if n.changetype != Changetype::Delete {
            if !nq.contains(&n.id) {
                let q = Quadtree::calculate_point(n.lon, n.lat, maxlevel, buffer);
                orig_data.expand_node(&n.id, q);
            }
        }
    }

    for (_, r) in changeblock.relations.iter() {
        if r.changetype != Changetype::Delete {
            if r.members.is_empty() {
                orig_data.set_relation(&r.id, Quadtree::new(0));
            } else if r.members.len() == 1 && r.members[0].mem_type == ElementType::Relation && r.members[0].mem_ref == r.id {
                message!("circular relation {}", r.id);
                orig_data.set_relation(&r.id, Quadtree::new(0));
            } else {
                let mut qt = Quadtree::empty();
                for m in r.members.iter() {
                    if m.mem_type == ElementType::Relation {
                        rel_rels.push((r.id, m.mem_ref));
                    } else {
                        match orig_data.get_quadtree(&m.mem_type, &m.mem_ref) {
                            Some(q) => {
                                qt = qt.common(&q);
                            }
                            None => {
                                message!(
                                    "missing member {:?} {} for relation {}",
                                    m.mem_type, m.mem_ref, r.id
                                );
                            }
                        }
                    }
                }

                orig_data.expand_relation(&r.id, qt);
            }
        }
    }

    for i in 0..5 {
        for (a, b) in rel_rels.iter() {
            match orig_data.get_quadtree(&ElementType::Relation, b) {
                Some(q) => {
                    orig_data.expand_relation(a, q);
                }
                None => {
                    if i == 4 {
                        
                        
                        message!(
                            "missing member {:?} {} for relation {}",
                            ElementType::Relation,
                            b,
                            a
                        );
                    }
                }
            }
        }
    }

    let mut unneeded_extra_nodes = 0;
    let mut create_delete = 0;
    let mut res = AllocBlocks::new(st, et);

    for (_, n) in changeblock.nodes.iter() {
        let q = orig_data.get_quadtree(&ElementType::Node, &n.id);

        let na = find_tile(&tree, q);

        let a = orig_data.get_alloc(&ElementType::Node, &n.id);

        match (n.changetype, a, q) {
            (Changetype::Normal, Some(alloc), Some(q)) => {
                if n.quadtree == q {
                    unneeded_extra_nodes += 1;
                } else {
                    let mut n2 = n.clone();
                    n2.quadtree = q;
                    n2.changetype = Changetype::Unchanged;
                    res.add_node(na.unwrap(), n2);

                    if na.unwrap() != alloc {
                        let mut n3 = n.clone();
                        //n3.tags.clear();
                        n3.quadtree = Quadtree::new(0);
                        n3.changetype = Changetype::Remove;
                        res.add_node(alloc, n3);
                    }
                }
            }
            (Changetype::Delete, Some(alloc), _) => {
                let mut n2 = n.clone();
                n2.quadtree = Quadtree::new(0);
                res.add_node(alloc, n2);
            }
            (Changetype::Delete, None, _) => {
                create_delete += 1;
            }
            (Changetype::Modify, Some(alloc), Some(q)) => {
                let mut n2 = n.clone();
                n2.quadtree = q;
                res.add_node(na.unwrap(), n2);
                if na.unwrap() != alloc {
                    let mut n3 = n.clone();
                    //n3.tags.clear();
                    n3.quadtree = Quadtree::new(0);
                    n3.changetype = Changetype::Remove;
                    res.add_node(alloc, n3);
                }
            }
            (Changetype::Modify, None, Some(q)) | (Changetype::Create, None, Some(q)) => {
                let mut n2 = n.clone();
                n2.quadtree = q;
                res.add_node(na.unwrap(), n2);
            }
            (rt, ra, rq) => {
                message!("dont understand {} {:?} {:?} {:?} {:?} {:?}", rt, ra, rq, n, q, na);
            }
        }
    }

    for (_, w) in changeblock.ways.iter() {
        let q = orig_data.get_quadtree(&ElementType::Way, &w.id);

        let na = find_tile(&tree, q);

        let a = orig_data.get_alloc(&ElementType::Way, &w.id);

        match (w.changetype, a, q) {
            (Changetype::Delete, Some(alloc), _) => {
                let mut w2 = w.clone();
                w2.quadtree = Quadtree::new(0);
                res.add_way(alloc, w2);
            }
            (Changetype::Delete, None, _) => {
                create_delete += 1;
            }
            (Changetype::Modify, Some(alloc), Some(q)) => {
                let mut w2 = w.clone();
                w2.quadtree = q;
                res.add_way(na.unwrap(), w2);
                if na.unwrap() != alloc {
                    let mut w3 = w.clone();
                    //w3.tags.clear();
                    w3.quadtree = Quadtree::new(0);
                    w3.changetype = Changetype::Remove;
                    res.add_way(alloc, w3);
                }
            }
            (Changetype::Modify, None, Some(q)) | (Changetype::Create, None, Some(q)) => {
                let mut w2 = w.clone();
                w2.quadtree = q;
                res.add_way(na.unwrap(), w2);
            }
            (rt, ra, rq) => {
                message!("dont understand {} {:?} {:?} {:?} {:?} {:?}", rt, ra, rq, w, q, na);
            }
        }
    }
    for (_, r) in changeblock.relations.iter() {
        let q = orig_data.get_quadtree(&ElementType::Relation, &r.id);

        let na = find_tile(&tree, q);

        let a = orig_data.get_alloc(&ElementType::Relation, &r.id);

        match (r.changetype, a, q) {
            (Changetype::Delete, Some(alloc), _) => {
                let mut r2 = r.clone();
                r2.quadtree = Quadtree::new(0);
                res.add_relation(alloc, r2);
            }
            (Changetype::Delete, None, _) => {
                create_delete += 1;
            }
            (Changetype::Modify, Some(alloc), Some(q)) => {
                let mut r2 = r.clone();
                r2.quadtree = q;
                res.add_relation(na.unwrap(), r2);
                if na.unwrap() != alloc {
                    let mut r3 = r.clone();
                    //r3.tags.clear();
                    r3.quadtree = Quadtree::new(0);
                    r3.changetype = Changetype::Remove;
                    res.add_relation(alloc, r3);
                }
            }
            (Changetype::Modify, None, Some(q)) | (Changetype::Create, None, Some(q)) => {
                let mut r2 = r.clone();
                r2.quadtree = q;
                res.add_relation(na.unwrap(), r2);
            }
            (rt, ra, rq) => {
                message!("dont understand {} {:?} {:?} {:?} {:?} {:?}", rt, ra, rq, r, q, na);
            }
        }
    }

    message!(
        "{} unneeded extra nodes, {} create_delete",
        unneeded_extra_nodes, create_delete
    );

    Ok(res.blocks)
}

fn prep_tree(prfx: &str, filelist: &Vec<FilelistEntry>) -> Result<QuadtreeTree> {
    let fname = format!("{}{}", prfx, filelist[0].filename);
    let mut fobj = File::open(&fname)?;
    let (x, fb) = read_file_block_with_pos(&mut fobj, 0)?;
    if fb.block_type != "OSMHeader" {
        return Err(Error::PbfDataError("first block not an OSMHeader".to_string()));
    }
    let head = HeaderBlock::read(x, &fb.data(), &fname)?;

    let mut tree = QuadtreeTree::new();
    for ii in &head.index {
        tree.add(&ii.quadtree, 1);
    }

    Ok(tree)
}

fn prep_idset(changeblock: &ChangeBlock) -> Arc<IdSetSet> {
    let mut idset = Box::new(IdSetSet::new());

    for (_, n) in changeblock.nodes.iter() {
        idset.nodes.insert(n.id);
    }
    message!("{}", idset);
    for (_, w) in changeblock.ways.iter() {
        idset.ways.insert(w.id);
        for n in w.refs.iter() {
            idset.nodes.insert(*n);
            if !changeblock.nodes.contains_key(n) {
                idset.exnodes.insert(*n);
            }
        }
    }
    message!("{}", idset);

    for (_, r) in changeblock.relations.iter() {
        idset.relations.insert(r.id);
        for m in r.members.iter() {
            match m.mem_type {
                ElementType::Node => {
                    idset.nodes.insert(m.mem_ref);
                }
                ElementType::Way => {
                    idset.ways.insert(m.mem_ref);
                }
                ElementType::Relation => {
                    idset.relations.insert(m.mem_ref);
                }
                _ => {}
            }
        }
    }
    message!("{}", idset);
    Arc::<IdSetSet>::from(idset)
}

pub fn find_update(
    prfx: &str,
    filelist: &Vec<FilelistEntry>,
    change_filename: &str,
    prev_ts: i64,
    ts: i64,
    max_qt_level: usize,
    qt_buffer: f64,
    fname: &str,
    numchan: usize,
) -> Result<(f64, usize)> {
    let mut chgf = BufReader::new(File::open(change_filename)?);

    let compression_type = CompressionType::Zlib;

    let tx = Timer::new();

    let mut changeblock = if change_filename.ends_with(".gz") {
        read_xml_change(&mut BufReader::new(flate2::bufread::GzDecoder::new(chgf)))
    } else {
        read_xml_change(&mut chgf)
    }?;

    let a = tx.since();

    let idset = prep_idset(&changeblock);

    let b = tx.since();

    let (mut orig_data, _, _) = collect_existing(prfx, filelist, idset, numchan)?;

    let c = tx.since();
    message!("{}", orig_data);

    let on = std::mem::take(&mut orig_data.othernodes);
    for (a, b) in on {
        match b {
            None => {}
            Some(n) => {
                changeblock.nodes.insert(a, n);
            }
        }
    }
    //changeblock.nodes.append(&mut orig_data.othernodes);

    let d = tx.since();

    let tree = prep_tree(prfx, filelist)?;
    let e = tx.since();
    message!("{}", tree);

    let tiles = calc_qts(&changeblock, &mut orig_data, &tree, max_qt_level, qt_buffer, prev_ts, ts)?;
    let f = tx.since();

    message!(
        "{} tiles, {} objs",
        tiles.len(),
        tiles.iter().map(|(_, b)| { b.nodes.len() }).sum::<usize>()
    );

    let mut wf = WriteFileInternalLocs::new(&format!("{}{}", prfx, fname), true);
    for (k, v) in tiles.iter() {
        let pp = v.pack(true, true)?;
        let qq = pack_file_block("OSMData", &pp, &compression_type)?;
        wf.call((*k, qq))
    }
    wf.finish()?;

    let g = tx.since();

    message!("read xml: {:5.1}s\nprep_idset: {:5.1}s\ncollect_existing: {:5.1}s\nextend nodes: {:5.1}s\nprep_tree: {:5.1}s\ncalc_qts: {:5.1}s\npack and write{:5.1}s\nTOTAL: {:5.1}s",a,b-a,c-b,d-c,e-d,f-e,g-f,g);

    Ok((tx.since(), tiles.len()))
}
