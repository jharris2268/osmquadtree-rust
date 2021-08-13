use simple_protocolbuffers::{
    pack_data, pack_delta_int_ref, pack_value, un_zig_zag, zig_zag, DeltaPackedInt, IterTags,
    PackedInt, PbfTag,
};

use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings, ReplaceNoneWithTimings};
use crate::elements::{MinimalBlock, Quadtree};
use crate::pbfformat::{
    pack_file_block, read_all_blocks_with_progbar, unpack_file_block, FileBlock, FileLocs,
    HeaderType, WriteFile,
};

use crate::utils::ThreadTimer;
use crate::message;
use crate::calcqts::quadtree_store::{QuadtreeGetSet, QuadtreeSimple};
use crate::calcqts::{CallFinishFileBlocks, OtherData, Timings, WayNodeVals};

use std::fmt;
use std::io::{Error, ErrorKind, Result};
//use std::sync::Arc;

pub struct WayNodeTile {
    key: i64,
    pub vals: Vec<(i64, i64)>,
}

impl WayNodeTile {
    pub fn new(key: i64, capacity: usize) -> WayNodeTile {
        WayNodeTile {
            key: key,
            vals: Vec::with_capacity(capacity),
        }
    }
    pub fn tile_key(&self) -> i64 {
        self.key
    }

    pub fn add(&mut self, n: i64, w: i64) {
        self.vals.push((n, w));
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
            idx += self.len() as i64;
        }
        self.vals[idx as usize]
    }
    /*pub fn iter(&self) -> impl Iterator<Item=&(i64,i64)> {
        self.vals.iter()
    }*/
    pub fn pack_chunks(&self, sz: usize) -> Vec<Vec<u8>> {
        if sz > self.vals.len() {
            return vec![self.pack()];
        }
        let mut res = Vec::new();
        let mut s = 0;
        while s < self.vals.len() {
            let t = usize::min(s + sz, self.vals.len());
            res.push(self.pack_part(s, t));
            s = t;
        }
        res
    }

    pub fn pack_part(&self, s: usize, t: usize) -> Vec<u8> {
        let nn = pack_delta_int_ref(self.vals[s..t].iter().map(|(n, _w)| n));
        let ww = pack_delta_int_ref(self.vals[s..t].iter().map(|(_n, w)| w));

        let l = 20 + nn.len() + ww.len();

        let mut res = Vec::with_capacity(l);

        pack_value(&mut res, 1, zig_zag(self.key));
        pack_data(&mut res, 2, &nn[..]);
        pack_data(&mut res, 3, &ww[..]);
        pack_value(&mut res, 4, (t - s) as u64);

        return res;
    }

    pub fn pack(&self) -> Vec<u8> {
        self.pack_part(0, self.vals.len())
    }

    pub fn unpack(&mut self, data: &Vec<u8>, minw: i64, maxw: i64) -> Result<usize> {
        let ti = self.vals.len();
        let mut nv = Vec::new();
        let mut wv = Vec::new();
        for tg in IterTags::new(&data[..]) {
            match tg {
                PbfTag::Value(1, k) => {
                    if self.key != 0 && un_zig_zag(k) != self.key {
                        return Err(Error::new(ErrorKind::Other, "wrong key"));
                    }
                }
                PbfTag::Data(2, nn) => {
                    nv.extend(DeltaPackedInt::new(&nn));
                }

                PbfTag::Data(3, ww) => {
                    wv.extend(DeltaPackedInt::new(&ww));
                }
                PbfTag::Value(4, l) => {
                    nv.reserve(l as usize);
                    wv.reserve(l as usize);
                    self.vals.reserve(l as usize + ti);
                }
                _ => {
                    return Err(Error::new(ErrorKind::Other, "unexpected tag"));
                }
            };
        }

        if minw > 0 || maxw > 0 {
            self.vals.extend(
                nv.iter()
                    .zip(wv)
                    .filter(|(_, b)| b >= &minw && (maxw == 0 || b < &maxw))
                    .map(|(a, b)| (*a, b)),
            );
        } else {
            self.vals.extend(nv.iter().zip(wv).map(|(a, b)| (*a, b)));
        }

        Ok(self.vals.len() - ti)
    }
}

impl IntoIterator for WayNodeTile {
    type Item = (i64, i64);
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
            return write!(
                f,
                "Tile {}: {} way nodes ({}, {}) to ({}, {})",
                self.tile_key(),
                self.len(),
                self.at(0).0,
                self.at(0).1,
                self.at(-1).0,
                self.at(-1).1
            );
        }
    }
}

pub struct CollectTilesStore {
    //filename: String,
    //vals: Vec<(i64, Vec<Vec<u8>>)>,
    vals: Vec<(i64, Vec<u8>)>,

    tm: f64,
}

impl CollectTilesStore {
    pub fn new() -> CollectTilesStore {
        CollectTilesStore {
            vals: Vec::new(),
            tm: 0.0,
        }
    }
}

impl CallFinish for CollectTilesStore /*<'_>*/ {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, p: Self::CallType) {
        //let vals = self.vals.as_mut().unwrap();
        let tt = ThreadTimer::new();
        //let vals = Arc::get_mut(&mut self.vals).unwrap();
        /*for (qi, qd) in p {
            let qv = qi as usize;

            if qv >= self.vals.len() {
                for i in self.vals.len()..qv + 1 {
                    self.vals.push((i as i64, Vec::new()));
                }
            }
            self.vals[qv].1.push(qd);
        }*/

        self.vals.extend(p);

        self.tm += tt.since();
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut tt = Timings::new();
        self.vals.sort();
        tt.add("collecttilestore", self.tm);
        tt.add_other(
            "waynodes",
            OtherData::PackedWayNodes(WayNodeVals::PackedInMem(std::mem::take(&mut self.vals))),
        );
        Ok(tt)
    }
}

struct WriteWayNodeTemp<T: ?Sized> {
    ww: Box<T>,
    pending: Vec<(i64, Vec<u8>)>,
    pending_size: usize,
    fname: String,
}

impl<T> WriteWayNodeTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = (f64, FileLocs)> + ?Sized,
{
    pub fn new(ww: Box<T>, fname: String, pending_size: usize) -> WriteWayNodeTemp<T> {
        WriteWayNodeTemp {
            ww: ww,
            pending: Vec::with_capacity(pending_size),
            pending_size: pending_size,
            fname: fname,
        }
    }
}

impl<T> CallFinish for WriteWayNodeTemp<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = (f64, FileLocs)> + ?Sized,
{
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, bls: Self::CallType) {
        if self.pending_size == 0 {
            self.ww.call(bls);
            return;
        }

        if self.pending.len() + bls.len() > self.pending_size {
            let mut tt =
                std::mem::replace(&mut self.pending, Vec::with_capacity(self.pending_size));
            tt.sort();
            self.ww.call(tt);
        }
        self.pending.extend(bls);
    }

    fn finish(&mut self) -> Result<Timings> {
        if !self.pending.is_empty() {
            let mut tt = std::mem::take(&mut self.pending);
            tt.sort();
            self.ww.call(tt);
        }
        let (a, mut b) = self.ww.finish()?;
        b.sort();
        let mut tm = Timings::new();

        tm.add("WriteWayNodeTemp", a);
        tm.add_other(
            "waynodes",
            OtherData::PackedWayNodes(WayNodeVals::TempFile(std::mem::take(&mut self.fname), b)),
        );
        Ok(tm)
    }
}

pub struct RelMems {
    pub nodes: Vec<(i64, i64)>,
    pub ways: Vec<(i64, i64)>,
    pub relations: Vec<(i64, i64)>,
    pub empty_rels: Vec<i64>,

    pub packed: Vec<Vec<u8>>,
}
impl RelMems {
    pub fn new() -> RelMems {
        RelMems {
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
            empty_rels: Vec::new(),
            packed: Vec::new(),
        }
    }
    pub fn len(&self) -> usize {
        self.nodes.len() + self.ways.len() + self.relations.len()
    }
    pub fn extend(&mut self, other: RelMems) {
        self.nodes.extend(other.nodes);
        self.ways.extend(other.ways);
        self.relations.extend(other.relations);
        self.empty_rels.extend(other.empty_rels);
        self.packed.extend(other.packed);
    }

    pub fn pack(&self) -> Vec<u8> {
        let mut res = Vec::new();
        pack_data(
            &mut res,
            1,
            &pack_delta_int_ref(self.nodes.iter().map(|(x, _)| x)),
        );
        pack_data(
            &mut res,
            2,
            &pack_delta_int_ref(self.nodes.iter().map(|(_, y)| y)),
        );

        pack_data(
            &mut res,
            3,
            &pack_delta_int_ref(self.ways.iter().map(|(x, _)| x)),
        );
        pack_data(
            &mut res,
            4,
            &pack_delta_int_ref(self.ways.iter().map(|(_, y)| y)),
        );

        pack_data(
            &mut res,
            5,
            &pack_delta_int_ref(self.relations.iter().map(|(x, _)| x)),
        );
        pack_data(
            &mut res,
            6,
            &pack_delta_int_ref(self.relations.iter().map(|(_, y)| y)),
        );

        pack_data(
            &mut res,
            7,
            &pack_delta_int_ref(self.empty_rels.iter().map(|x| x)),
        );

        res
    }

    pub fn pack_and_store(&mut self) {
        if self.nodes.is_empty()
            && self.ways.is_empty()
            && self.relations.is_empty()
            && self.empty_rels.is_empty()
        {
            return;
        }

        let p = self.pack();
        self.packed
            .push(pack_file_block("RelMems", &p, true).expect("?"));
        self.clear();
    }

    pub fn clear(&mut self) {
        self.nodes = Vec::new();
        self.ways = Vec::new();
        self.relations = Vec::new();
        self.empty_rels = Vec::new();
    }

    pub fn unpack_stored(&mut self, load_nodes: bool, load_others: bool) {
        for i in 0..self.packed.len() {
            let f = unpack_file_block(0, &self.packed[i]).expect("?");
            self.unpack(&f.data(), load_nodes, load_others)
        }
    }

    pub fn unpack(&mut self, data: &[u8], load_nodes: bool, load_others: bool) {
        let mut a = Vec::new();
        let mut b = Vec::new();
        let mut c = Vec::new();
        let mut d = Vec::new();
        let mut e = Vec::new();
        let mut f = Vec::new();

        for t in IterTags::new(data) {
            match t {
                PbfTag::Data(1, x) => {
                    if load_nodes {
                        a = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(2, x) => {
                    if load_nodes {
                        b = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(3, x) => {
                    if load_others {
                        c = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(4, x) => {
                    if load_others {
                        d = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(5, x) => {
                    if load_others {
                        e = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(6, x) => {
                    if load_others {
                        f = DeltaPackedInt::new(x).collect();
                    }
                }
                PbfTag::Data(7, x) => {
                    if load_others {
                        self.empty_rels.extend(DeltaPackedInt::new(x));
                    }
                }
                _ => {}
            }
        }
        self.nodes.extend(a.iter().zip(b).map(|(x, y)| (*x, y)));
        self.ways.extend(c.iter().zip(d).map(|(x, y)| (*x, y)));
        self.relations.extend(e.iter().zip(f).map(|(x, y)| (*x, y)));
    }
}

fn unpack_relation_node_vals(nqts: &mut QuadtreeSimple, data: &[u8]) {
    for t in IterTags::new(data) {
        match t {
            PbfTag::Data(2, x) => {
                for n in DeltaPackedInt::new(x) {
                    nqts.set(n, Quadtree::new(-1));
                }
            }
            _ => {}
        }
    }
}

pub fn prep_relation_node_vals(relmems: &RelMems) -> Box<QuadtreeSimple> {
    let mut nqts = Box::new(QuadtreeSimple::new());
    for (_, b) in relmems.nodes.iter() {
        nqts.set(*b, Quadtree::new(-1));
    }

    for p in relmems.packed.iter() {
        let f = unpack_file_block(0, &p).expect("?");
        unpack_relation_node_vals(&mut nqts, &f.data());
    }
    nqts
}

impl fmt::Display for RelMems {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RelMems: {} nodes, {} ways, {} rels, {} empty",
            self.nodes.len(),
            self.ways.len(),
            self.relations.len(),
            self.empty_rels.len()
        )
    }
}

pub struct PackWayNodes<T> {
    pending: Vec<Box<WayNodeTile>>,
    split: i64,
    limit: usize,
    outcall: Box<T>,
    first_waytile_pos: Option<u64>,
    relmems: Option<RelMems>,
    pack_rels: bool,
    tm: f64,
}

fn pack_and_clear_pending(tt: &mut Box<WayNodeTile>) -> Vec<u8> {
    tt.sort();
    let p = tt.pack();
    let mut p2 = pack_file_block("WayNodes", &p, true).unwrap();
    tt.clear();
    p2.shrink_to_fit();
    p2
}

impl<T> PackWayNodes<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>,
{
    pub fn new(split: i64, limit: usize, outcall: Box<T>, pack_rels: bool) -> PackWayNodes<T> {
        PackWayNodes {
            pending: Vec::new(),
            split: split,
            limit: limit,
            outcall: outcall,
            first_waytile_pos: None,
            relmems: Some(RelMems::new()),
            pack_rels: pack_rels,
            tm: 0.0,
        }
    }

    fn check_tile(&mut self, t: i64) {
        let ts = t as usize;
        if ts >= self.pending.len() {
            for i in self.pending.len()..ts + 1 {
                //self.pending.push(Box::new(WayNodeTile32::new(self.split, i as i64, self.limit)));
                self.pending
                    .push(Box::new(WayNodeTile::new(i as i64, self.limit)));
            }
        }
    }

    fn add(&mut self, n: i64, w: i64) -> Option<(i64, Vec<u8>)> {
        let t = n / self.split;
        self.check_tile(t);

        let tt = self.pending.get_mut(t as usize).unwrap();

        tt.add(n, w);
        if tt.len() as usize == self.limit {
            return Some((t, pack_and_clear_pending(tt)));
        }

        None
    }

    fn add_all(&mut self, idx: usize, fb: FileBlock) -> Vec<(i64, Vec<u8>)> {
        let mut res = Vec::new();

        let fbd = fb.data();

        if fb.block_type != "OSMData" {
        } else {
            let mb = MinimalBlock::read_parts(idx as i64, fb.pos, &fbd, false, false, true, true)
                .expect("failed to read block");

            if !mb.ways.is_empty() && self.first_waytile_pos.is_none() {
                //message!("\nfound {} ways @ {}", mb.ways.len(), fb.pos);
                self.first_waytile_pos = Some(fb.pos)
            }

            for w in mb.ways {
                for n in DeltaPackedInt::new(&w.refs_data) {
                    match self.add(n, w.id) {
                        Some(bl) => {
                            res.push(bl);
                        }
                        None => {}
                    };
                }
            }
            let rm = self.relmems.as_mut().unwrap();
            for r in mb.relations {
                if r.refs_data.is_empty() {
                    rm.empty_rels.push(r.id);
                } else {
                    for (rf, ty) in
                        DeltaPackedInt::new(&r.refs_data).zip(PackedInt::new(&r.types_data))
                    {
                        match ty {
                            0 => {
                                rm.nodes.push((r.id, rf));
                            }
                            1 => {
                                rm.ways.push((r.id, rf));
                            }
                            2 => {
                                rm.relations.push((r.id, rf));
                            }
                            _ => {}
                        }
                    }
                }
                if self.pack_rels && rm.len() > 10000000 {
                    rm.pack_and_store();
                }
            }
        }
        res
    }

    fn add_remaining(&mut self) -> Vec<(i64, Vec<u8>)> {
        let mut res = Vec::new();
        let p = std::mem::take(&mut self.pending);
        for mut tt in p {
            if tt.len() > 0 {
                res.push((tt.tile_key(), pack_and_clear_pending(&mut tt)));
            }
        }

        let rm = self.relmems.as_mut().unwrap();
        if self.pack_rels && rm.len() > 0 {
            rm.pack_and_store();
        }

        res
    }
}

impl<T> CallFinish for PackWayNodes<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>,
{
    type CallType = (usize, FileBlock);
    //type ReturnType=(RelMems,T::ReturnType);
    type ReturnType = Timings;

    fn call(&mut self, fb: Self::CallType) {
        let tt = ThreadTimer::new();
        let pp = self.add_all(fb.0, fb.1);
        self.tm += tt.since();
        self.outcall.call(pp);
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let tt = ThreadTimer::new();
        let pp = self.add_remaining();
        let x = tt.since();

        self.outcall.call(pp);

        let mut timings = self.outcall.finish()?;
        timings.add("packwaynodes", self.tm);
        timings.add("packwaynodes final", x);

        let r = self.relmems.take().unwrap();
        timings.add_other("relmems", OtherData::RelMems(r));
        match self.first_waytile_pos {
            None => {}
            Some(p) => {
                timings.add_other("first_waytile_pos", OtherData::FirstWayTile(p));
            }
        }
        Ok(timings)
    }
}

fn get_relmems_waynodes(mut tt: Timings) -> (RelMems, WayNodeVals, u64) {
    let mut r = RelMems::new();
    let mut w: Option<WayNodeVals> = None;
    let mut first_waytile_pos = u64::MAX;
    for (_, b) in std::mem::take(&mut tt.others) {
        match b {
            OtherData::RelMems(rx) => r.extend(rx),
            OtherData::PackedWayNodes(wx) => {
                if !w.is_none() {
                    panic!("!!!");
                }
                w = Some(wx);
                /*let a = Arc::try_unwrap(wx);
                match a {
                    Ok(wxx) => w.extend(wxx),
                    Err(_) => {
                        panic!("!!");
                    }
                }*/
            }
            OtherData::FirstWayTile(p) => {
                if p < first_waytile_pos {
                    first_waytile_pos = p;
                }
            }
            _ => {}
        }
    }
    return (r, w.unwrap(), first_waytile_pos);
}

pub fn prep_way_nodes(infn: &str, numchan: usize) -> Result<(RelMems, WayNodeVals, u64)> {
    message!("prep_way_nodes({},{})", infn, numchan);

    let (split, limit) = (1 << 22, 1 << 14);

    let progmsg = format!("prep_way_nodes for {}, numchan={}", infn, numchan);
    let ct = Box::new(CollectTilesStore::new());
    let pwn: CallFinishFileBlocks = match numchan {
        0 => Box::new(PackWayNodes::new(split, limit, ct, true)),

        numchan => {
            let ct_par = CallbackSync::new(ct, numchan);

            let mut pwn_par: Vec<CallFinishFileBlocks> = Vec::new();

            for ctx in ct_par {
                let ctm = Box::new(ReplaceNoneWithTimings::new(ctx));
                let pwn = Box::new(PackWayNodes::new(split, limit, ctm, true));
                pwn_par.push(Box::new(Callback::new(pwn)));
            }

            Box::new(CallbackMerge::new(pwn_par, Box::new(MergeTimings::new())))
        }
    };
    let (tt, _) = read_all_blocks_with_progbar(infn, pwn, &progmsg);
    message!("{}", tt);
    Ok(get_relmems_waynodes(tt))
}
pub fn prep_way_nodes_tempfile(
    infn: &str,
    outfn: &str,
    numchan: usize,
) -> Result<(RelMems, WayNodeVals, u64)> {
    message!("prep_way_nodes_tempfile({},{},{})", infn, outfn, numchan);

    let (split, limit) = (1 << 24, 1 << 16);

    let progmsg = format!("prep_way_nodes_tempfile for {}, numchan={}", infn, numchan);
    let tempfn = format!("{}-waynodestemp", outfn);

    let ww: Box<dyn CallFinish<CallType=Vec<(i64,Vec<u8>)>,ReturnType=(f64,FileLocs)>> = //if numchan == 0 {
        Box::new(WriteFile::new(&tempfn, HeaderType::None));
    //} else {
    //    Box::new(Callback::new(Box::new(WriteFile::new(&tempfn, HeaderType::None))))
    //};

    let ct = Box::new(WriteWayNodeTemp::new(ww, tempfn, 16 * 1024));
    let pwn: CallFinishFileBlocks = match numchan {
        0 => Box::new(PackWayNodes::new(split, limit, ct, true)),

        numchan => {
            let ct_par = CallbackSync::new(ct, numchan);

            let mut pwn_par: Vec<CallFinishFileBlocks> = Vec::new();

            for ctx in ct_par {
                let ctm = Box::new(ReplaceNoneWithTimings::new(ctx));
                let pwn = Box::new(PackWayNodes::new(split, limit, ctm, true));
                pwn_par.push(Box::new(Callback::new(pwn)));
            }

            Box::new(CallbackMerge::new(pwn_par, Box::new(MergeTimings::new())))
        }
    };
    let (tt, _) = read_all_blocks_with_progbar(infn, pwn, &progmsg);
    message!("{}", tt);
    Ok(get_relmems_waynodes(tt))
}
