use crate::callback::{CallFinish, Callback, CallbackMerge, CallbackSync};
use crate::elements::{MinimalBlock, MinimalNode};
use crate::pbfformat::convertblocks::make_convert_minimal_block_parts;
use crate::pbfformat::header_block::HeaderType;
use crate::pbfformat::read_file_block::{read_all_blocks_with_progbar, FileBlock};
use crate::pbfformat::writefile::WriteFile;
use crate::pbfformat::{read_file_block, read_pbf, write_pbf};
use crate::utils::{CallAll, MergeTimings, ReplaceNoneWithTimings, Timer};

use std::fmt;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Result, Seek, SeekFrom};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::calcqts::packwaynodes::WayNodeTile;
use crate::calcqts::write_quadtrees::{WrapWriteFile, WrapWriteFileVec};
use crate::calcqts::{
    CallFinishFileBlocks, FileLocs, NodeWayNodes, OtherData, Timings, WayNodeVals,
};

pub struct NodeWayNodeComb {
    pub id: i64,
    pub lon: i32,
    pub lat: i32,
    pub ways: Vec<i64>,
}

impl NodeWayNodeComb {
    pub fn new(nd: MinimalNode, ways: Vec<i64>) -> NodeWayNodeComb {
        NodeWayNodeComb {
            id: nd.id,
            lon: nd.lon,
            lat: nd.lat,
            ways: ways,
        }
    }
    pub fn from_id(id: i64) -> NodeWayNodeComb {
        NodeWayNodeComb {
            id: id,
            lon: 0,
            lat: 0,
            ways: Vec::new(),
        }
    }
}

impl fmt::Display for NodeWayNodeComb {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:12} {:10} {:10} {:3} ways",
            self.id,
            self.lon,
            self.lat,
            self.ways.len()
        )
    }
}

pub struct NodeWayNodeCombTile {
    pub vals: Vec<NodeWayNodeComb>,
}

impl NodeWayNodeCombTile {
    pub fn new(vals: Vec<NodeWayNodeComb>) -> NodeWayNodeCombTile {
        NodeWayNodeCombTile { vals }
    }

    pub fn pack(&self) -> Vec<u8> {
        let mut res = Vec::new();
        write_pbf::pack_value(&mut res, 1, self.vals.len() as u64);
        write_pbf::pack_data(
            &mut res,
            2,
            &write_pbf::pack_delta_int(self.vals.iter().map(|x| x.id)),
        );
        write_pbf::pack_data(
            &mut res,
            3,
            &write_pbf::pack_delta_int(self.vals.iter().map(|x| x.lon as i64)),
        );
        write_pbf::pack_data(
            &mut res,
            4,
            &write_pbf::pack_delta_int(self.vals.iter().map(|x| x.lat as i64)),
        );
        write_pbf::pack_data(
            &mut res,
            5,
            &write_pbf::pack_int(self.vals.iter().map(|x| x.ways.len() as u64)),
        );
        write_pbf::pack_data(
            &mut res,
            6,
            &write_pbf::pack_delta_int_ref(self.vals.iter().flat_map(|x| x.ways.iter())),
        );

        res
    }

    pub fn unpack(data: &[u8], minw: i64, maxw: i64) -> NodeWayNodeCombTile {
        let mut res = NodeWayNodeCombTile { vals: Vec::new() };

        let mut numw = Vec::new();
        let mut ww = Vec::new();
        for t in read_pbf::IterTags::new(&data, 0) {
            match t {
                read_pbf::PbfTag::Value(1, c) => {
                    res.vals.reserve(c as usize);
                }
                read_pbf::PbfTag::Data(2, x) => {
                    //read_pbf::DeltaPackedInt::new(x).enumerate().map( |(_,x) | { res.vals.push(NodeWayNodeComb::from_id(x));}).collect(); },
                    for i in read_pbf::DeltaPackedInt::new(x) {
                        res.vals.push(NodeWayNodeComb::from_id(i));
                    }
                }
                read_pbf::PbfTag::Data(3, x) => {
                    for (i, ln) in read_pbf::DeltaPackedInt::new(x).enumerate() {
                        res.vals[i].lon = ln as i32;
                    }
                }

                read_pbf::PbfTag::Data(4, x) => {
                    for (i, lt) in read_pbf::DeltaPackedInt::new(x).enumerate() {
                        res.vals[i].lat = lt as i32;
                    }
                }
                read_pbf::PbfTag::Data(5, x) => {
                    numw = read_pbf::read_packed_int(x);
                }
                read_pbf::PbfTag::Data(6, x) => {
                    ww = read_pbf::read_delta_packed_int(x);
                }
                _ => {}
            }
        }

        let mut s = 0;
        if minw == 0 && maxw == 0 {
            for (i, r) in res.vals.iter_mut().enumerate() {
                let n = numw[i] as usize;
                r.ways.extend(ww[s..s + n].iter());
                s += n;
            }
        } else {
            for (i, r) in res.vals.iter_mut().enumerate() {
                let n = numw[i] as usize;
                r.ways.extend(
                    ww[s..s + n]
                        .iter()
                        .filter(|w: &&i64| **w >= minw && (maxw == 0 || **w < maxw)),
                );
                s += n;
            }
        }
        if s != ww.len() {
            panic!("gone wrong");
        }
        res
    }
}

pub fn make_packwaynodescomb<T: CallFinish<CallType = Vec<u8>, ReturnType = Timings>>(
    out: Box<T>,
) -> Box<impl CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>> {
    let conv = Box::new(|n: NodeWayNodeCombTile| {
        read_file_block::pack_file_block("NodeWayNodes", &n.pack(), true).expect("failed to pack")
    });

    Box::new(CallAll::new(out, "packnodewaycomb", conv))
}

fn resort_waynodes((i, dd): (i64, Vec<Vec<u8>>)) -> Vec<(i64, Vec<u8>)> {
    let mut pos: u64 = 0;
    let wnt = read_way_node_tiles_vals(&mut pos, i, &dd, 0, 0).expect("?");
    let mut res = Vec::new();
    for vv in wnt.pack_chunks(4096) {
        res.push((
            0,
            read_file_block::pack_file_block("WayNodes", &vv, true).expect("!"),
        ));
    }
    res
}

pub fn write_waynode_sorted_resort(waynodevals: WayNodeVals, outfn: &str) -> String {
    let waynodesfn = format!("{}-waynodes", outfn);

    let wv = CallbackSync::new(
        Box::new(WrapWriteFileVec::new(WriteFile::new(
            &waynodesfn,
            HeaderType::None,
        ))),
        4,
    );
    let mut vv: Vec<Box<dyn CallFinish<CallType = (i64, Vec<Vec<u8>>), ReturnType = Timings>>> =
        Vec::new();
    for w in wv {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));

        vv.push(Box::new(Callback::new(Box::new(CallAll::new(
            w2,
            "resort",
            Box::new(resort_waynodes),
        )))));
    }

    let mut vvm = Box::new(CallbackMerge::new(vv, Box::new(MergeTimings::new())));

    let ww = Arc::try_unwrap(waynodevals).unwrap();
    let mut pg = read_file_block::ProgBarWrap::new(100);
    pg.set_range(100);
    pg.set_message("write_waynode_sorted_resort");

    let pf = 100.0 / (ww.len() as f64);
    let mut i = 0.0;
    for (a, b) in ww {
        pg.prog(pf * i);
        vvm.call((a, b));
        i += 1.0;
    }
    pg.finish();
    let t = vvm.finish().expect("?");
    println!("{}", t);
    waynodesfn
}

pub fn write_waynode_sorted(waynodevals: WayNodeVals, outfn: &str) -> (String, FileLocs) {
    let waynodesfn = format!("{}-waynodes", outfn);

    let mut wv = WriteFile::new(&waynodesfn, HeaderType::None);

    let ww = Arc::try_unwrap(waynodevals).unwrap();

    for (a, b) in ww {
        for c in b {
            wv.call(vec![(a, c)]);
        }
    }

    let (_, locs) = wv.finish().expect("?");

    (waynodesfn, locs)
}

fn read_way_node_tiles_vals(
    pos: &mut u64,
    tile: i64,
    vals: &Vec<Vec<u8>>,
    minw: i64,
    maxw: i64,
) -> Result<WayNodeTile> {
    let mut res = WayNodeTile::new(tile, 0);

    if vals.is_empty() {
        return Ok(res);
    }
    let nv = vals.len();
    for (i, v) in vals.iter().enumerate() {
        let fb = read_file_block::unpack_file_block(*pos, &v)?;
        if fb.block_type != "WayNodes" {
            return Err(Error::new(
                ErrorKind::Other,
                format!("wrong block type {}", fb.block_type),
            ));
        }

        res.unpack(&fb.data(), minw, maxw)?;
        if i == 0 && minw == 0 && maxw == 0 {
            res.vals.reserve(res.vals.len() * nv);
        }
        *pos += v.len() as u64;
        //drop(v);
    }
    res.sort();
    Ok(res)
}

fn read_way_node_tiles_vals_send(
    vals: WayNodeVals,
    send: mpsc::SyncSender<WayNodeTile>,
    minw: i64,
    maxw: i64,
) {
    let mut pos: u64 = 0;
    for (k, vv) in vals.iter() {
        let wnt = read_way_node_tiles_vals(&mut pos, *k, vv, minw, maxw).unwrap();

        send.send(wnt).expect("send failed");
    }
    drop(send);
}

struct SendAll<T: Sync + Send + 'static> {
    send: Arc<Mutex<mpsc::SyncSender<T>>>,
}

impl<T> CallFinish for SendAll<T>
where
    T: Sync + Send + 'static,
{
    type CallType = T;
    type ReturnType = Timings;
    fn call(&mut self, t: T) {
        self.send.lock().unwrap().send(t).expect("send failed");
    }

    fn finish(&mut self) -> Result<Timings> {
        Ok(Timings::new())
    }
}

fn read_way_node_tiles_file_send(
    fname: String,
    locs: FileLocs,
    send: mpsc::SyncSender<WayNodeTile>,
    minw: i64,
    maxw: i64,
) {
    let fobj = File::open(&fname).expect("?");
    let mut fbuf = BufReader::new(fobj);

    let mut p = 0;

    let send2 = Arc::new(Mutex::new(send));
    let ss = CallbackSync::new(
        Box::new(SendAll {
            send: send2.clone(),
        }),
        4,
    );
    let mut qq = Vec::new();
    for s in ss {
        let s2 = Box::new(ReplaceNoneWithTimings::new(s));
        qq.push(Box::new(Callback::new(Box::new(CallAll::new(
            s2,
            "unpack and merge",
            Box::new(move |(k, pp): (i64, Vec<FileBlock>)| {
                let mut wnt = WayNodeTile::new(k, 0);
                let np = pp.len();
                for (i, p) in pp.iter().enumerate() {
                    wnt.unpack(&p.data(), minw, maxw)
                        .expect(&format!("failed to unpack"));
                    if i == 0 {
                        wnt.vals.reserve(wnt.vals.len() * np)
                    }
                }
                wnt.sort();
                wnt
            }),
        )))));
    }

    if locs.is_empty() {
        for (i, fb) in read_file_block::ReadFileBlocks::new(&mut fbuf).enumerate() {
            qq[i % 4].call((0, vec![fb]));
        }
    } else {
        let mut i = 0;
        for (k, vv) in locs {
            //let mut wnt = WayNodeTile::new(*k, 0);
            if !vv.is_empty() {
                let mut pp = Vec::new();
                for (a, _) in vv {
                    if a != p {
                        fbuf.seek(SeekFrom::Start(a))
                            .expect(&format!("failed to read {} @ {}", fname, a));
                    }
                    let (np, fb) = read_file_block::read_file_block_with_pos(&mut fbuf, a)
                        .expect(&format!("failed to read {} @ {}", fname, a));
                    if fb.block_type != "WayNodes" {
                        panic!("wrong block type {}", fb.block_type);
                    }
                    pp.push(fb);

                    p = np;
                }
                qq[i % 4].call((k, pp));
                i += 1;
            }
        }
    }
    for mut q in qq {
        q.finish().expect("!");
    }
    drop(send2);
}

struct ChannelReadWayNodeFlatIter {
    //jh: thread::JoinHandle<()>,
    recv: Arc<Mutex<mpsc::Receiver<WayNodeTile>>>,
    hadfirst: bool,
    curr: Option<WayNodeTile>,
    idx: i64,
}

impl ChannelReadWayNodeFlatIter {
    pub fn filter_vals(
        waynodevals: WayNodeVals,
        minw: i64,
        maxw: i64,
    ) -> ChannelReadWayNodeFlatIter {
        let (s, r) = mpsc::sync_channel(1);
        let rx = Arc::new(Mutex::new(r));

        /*let jh =*/
        thread::spawn(move || read_way_node_tiles_vals_send(waynodevals, s, minw, maxw));
        ChannelReadWayNodeFlatIter {
            /*jh:jh,*/ recv: rx.clone(),
            hadfirst: false,
            curr: None,
            idx: 0,
        }
    }

    pub fn filter_file(
        fname: &str,
        locs: &FileLocs,
        minw: i64,
        maxw: i64,
    ) -> ChannelReadWayNodeFlatIter {
        let (s, r) = mpsc::sync_channel(1);
        let rx = Arc::new(Mutex::new(r));

        let fname2 = String::from(fname);
        let locs2 = locs.clone();

        /*let jh =*/
        thread::spawn(move || read_way_node_tiles_file_send(fname2, locs2, s, minw, maxw));
        ChannelReadWayNodeFlatIter {
            /*jh:jh,*/ recv: rx.clone(),
            hadfirst: false,
            curr: None,
            idx: 0,
        }
    }

    fn next_wnt(&mut self) {
        match self.recv.lock().unwrap().recv() {
            Ok(wnt) => {
                self.curr = Some(wnt);
                self.idx = 0;
            }
            Err(_) => {
                self.curr = None;
            }
        }
    }
}

impl Iterator for ChannelReadWayNodeFlatIter {
    type Item = (i64, i64);

    fn next(&mut self) -> Option<(i64, i64)> {
        if !self.hadfirst {
            self.next_wnt();
            self.hadfirst = true;
        }

        match &self.curr {
            None => {
                return None;
            }
            Some(wnt) => {
                if self.idx == wnt.len() {
                    self.next_wnt();
                    return self.next();
                }
                let r = wnt.at(self.idx);
                self.idx += 1;
                return Some(r);
            }
        }
    }
}

pub struct CombineNodeWayNodeCB<T, U> {
    waynode: U,
    hadfirst: bool,
    waynode_curr: Option<(i64, i64)>,
    tm: f64,
    combined_cb: Box<T>,
}

impl<T, U> CombineNodeWayNodeCB<T, U>
where
    T: CallFinish<CallType = NodeWayNodeCombTile>,
    U: Iterator<Item = (i64, i64)>,
{
    pub fn new(waynode: U, combined_cb: Box<T>) -> CombineNodeWayNodeCB<T, U> {
        let waynode_curr = None;
        let hadfirst = false;
        let tm = 0.0;
        CombineNodeWayNodeCB {
            waynode,
            hadfirst,
            waynode_curr,
            tm,
            combined_cb,
        }
    }
}

fn combine_nodes_waynodes<'a, T>(
    waynode_iter: &'a mut T,
    waynode_curr: &'a mut Option<(i64, i64)>,
    mb: MinimalBlock,
) -> NodeWayNodeCombTile
where
    T: Iterator<Item = (i64, i64)>,
{
    let mut res = Vec::with_capacity(mb.nodes.len());

    for n in &mb.nodes {
        let ways = || -> Vec<i64> {
            let mut v = Vec::new();

            loop {
                match waynode_curr {
                    None => {
                        return v;
                    }
                    Some((a, b)) => {
                        if *a < n.id {
                            //self.next_waynode();
                            *waynode_curr = waynode_iter.next();
                        } else if *a == n.id {
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
        res.push(NodeWayNodeComb {
            id: n.id,
            lon: n.lon,
            lat: n.lat,
            ways: ways,
        });
    }
    NodeWayNodeCombTile::new(res)
}

impl<T, U> CallFinish for CombineNodeWayNodeCB<T, U>
where
    T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>,
    U: Iterator<Item = (i64, i64)> + Sync + Send + 'static,
{
    type CallType = MinimalBlock;
    type ReturnType = Timings;

    fn call(&mut self, mb: MinimalBlock) {
        let t = Timer::new();
        if !self.hadfirst {
            self.waynode_curr = self.waynode.next();
            self.hadfirst = true;
        }

        let res = combine_nodes_waynodes(&mut self.waynode, &mut self.waynode_curr, mb);
        self.tm += t.since();
        if res.vals.len() > 0 {
            self.combined_cb.call(res);
        }
        return;
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut t = self.combined_cb.finish()?;
        t.add("combinenodewaynode", self.tm);
        Ok(t)
    }
}

pub fn write_nodewaynode_file(nodewaynodes: NodeWayNodes, outfn: &str) -> NodeWayNodes {
    let waynodesfn = format!("{}-nodewaynodes", outfn);

    let wvs = CallbackSync::new(
        Box::new(WrapWriteFile::new(WriteFile::new(
            &waynodesfn,
            HeaderType::None,
        ))),
        4,
    );
    let mut wvps: Vec<Box<dyn CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>>> =
        Vec::new();
    for w in wvs {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));

        wvps.push(Box::new(Callback::new(make_packwaynodescomb(w2))));
    }

    let wvpm = Box::new(CallbackMerge::new(wvps, Box::new(MergeTimings::new())));

    let t = read_nodewaynodes(nodewaynodes, wvpm, 0, 0, "write_nodewaynodevals", 4);

    let mut nt = 0;
    for (_, b) in &t.others {
        match b {
            OtherData::FileLen(n) => nt += *n,
            _ => {}
        }
    }
    println!("write_nodewaynodevals: {}, {} bytes", t, nt);
    NodeWayNodes::Combined(waynodesfn)
}

struct UnpackNodeWayNodeCombTile<T> {
    out: Box<T>,
    minw: i64,
    maxw: i64,
    tm: f64,
}
impl<T> UnpackNodeWayNodeCombTile<T>
where
    T: CallFinish<CallType = NodeWayNodeCombTile>,
{
    pub fn new(out: Box<T>, minw: i64, maxw: i64) -> UnpackNodeWayNodeCombTile<T> {
        let tm = 0.0;
        UnpackNodeWayNodeCombTile {
            out,
            minw,
            maxw,
            tm,
        }
    }
}

impl<T> CallFinish for UnpackNodeWayNodeCombTile<T>
where
    T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>,
{
    type CallType = (usize, FileBlock);
    type ReturnType = Timings;

    fn call(&mut self, fb: (usize, FileBlock)) {
        let t = Timer::new();
        if fb.1.block_type == "NodeWayNodes" {
            let nn = NodeWayNodeCombTile::unpack(&fb.1.data(), self.minw, self.maxw);
            self.out.call(nn);
        } else {
            self.out.call(NodeWayNodeCombTile::new(Vec::new()));
        }
        self.tm += t.since();
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut t = self.out.finish()?;
        t.add("unpack nodewaynodes", self.tm);
        Ok(t)
    }
}

fn read_waynodeways_combined<
    T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>,
>(
    waynodesfn: &str,
    eqt: Box<T>,
    minw: i64,
    maxw: i64,
    msg: &str,
    numchan: usize,
) -> Timings {
    if numchan == 0 {
        let convert = Box::new(UnpackNodeWayNodeCombTile::new(eqt, minw, maxw));
        read_all_blocks_with_progbar(waynodesfn, convert, msg).0
    } else {
        let wbs = CallbackSync::new(eqt, 4);

        let mut conv: Vec<CallFinishFileBlocks> = Vec::new();
        for wb in wbs {
            let wb2 = Box::new(ReplaceNoneWithTimings::new(wb));
            conv.push(Box::new(Callback::new(Box::new(
                UnpackNodeWayNodeCombTile::new(wb2, minw, maxw),
            ))));
        }

        let conv_merge = Box::new(CallbackMerge::new(conv, Box::new(MergeTimings::new())));
        read_all_blocks_with_progbar(waynodesfn, conv_merge, msg).0
    }
}

fn read_waynodeways_inmem<T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>>(
    infn: &str,
    waynodevals: WayNodeVals,
    eqt: Box<T>,
    minw: i64,
    maxw: i64,
    msg: &str,
    numchan: usize,
) -> Timings {
    if numchan == 0 {
        let wn_iter = ChannelReadWayNodeFlatIter::filter_vals(waynodevals.clone(), minw, maxw);
        let combine = Box::new(CombineNodeWayNodeCB::new(wn_iter, eqt));
        let convert = make_convert_minimal_block_parts(false, true, false, false, combine);
        read_all_blocks_with_progbar(infn, convert, msg).0
    } else {
        let wbs = Box::new(Callback::new(eqt));

        let wn_iter = ChannelReadWayNodeFlatIter::filter_vals(waynodevals.clone(), minw, maxw);

        let combines = CallbackSync::new(Box::new(CombineNodeWayNodeCB::new(wn_iter, wbs)), 4);

        let mut converts: Vec<CallFinishFileBlocks> = Vec::new();
        for c in combines {
            let c2 = Box::new(ReplaceNoneWithTimings::new(c));
            converts.push(Box::new(Callback::new(make_convert_minimal_block_parts(
                false, true, false, false, c2,
            ))));
        }
        let conv_merge = Box::new(CallbackMerge::new(converts, Box::new(MergeTimings::new())));
        read_all_blocks_with_progbar(infn, conv_merge, msg).0
    }
}

fn read_waynodeways_seperate<
    T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>,
>(
    infn: &str,
    waynodefn: &str,
    waynodelocs: &FileLocs,
    eqt: Box<T>,
    minw: i64,
    maxw: i64,
    msg: &str,
    numchan: usize,
) -> Timings {
    if numchan == 0 {
        let wn_iter = ChannelReadWayNodeFlatIter::filter_file(waynodefn, waynodelocs, minw, maxw);
        let combine = Box::new(CombineNodeWayNodeCB::new(wn_iter, eqt));
        let convert = make_convert_minimal_block_parts(false, true, false, false, combine);
        read_all_blocks_with_progbar(infn, convert, msg).0
    } else {
        let wbs = Box::new(Callback::new(eqt));

        let wn_iter = ChannelReadWayNodeFlatIter::filter_file(waynodefn, waynodelocs, minw, maxw);

        let combines =
            CallbackSync::new(Box::new(CombineNodeWayNodeCB::new(wn_iter, wbs)), numchan);

        let mut converts: Vec<CallFinishFileBlocks> = Vec::new();
        for c in combines {
            let c2 = Box::new(ReplaceNoneWithTimings::new(c));
            converts.push(Box::new(Callback::new(make_convert_minimal_block_parts(
                false, true, false, false, c2,
            ))));
        }

        let conv_merge = Box::new(CallbackMerge::new(converts, Box::new(MergeTimings::new())));
        read_all_blocks_with_progbar(infn, conv_merge, msg).0
    }
}

pub fn read_nodewaynodes<T: CallFinish<CallType = NodeWayNodeCombTile, ReturnType = Timings>>(
    nodewaynodes: NodeWayNodes,
    eqt: Box<T>,
    minw: i64,
    maxw: i64,
    msg: &str,
    numchan: usize,
) -> Timings {
    match nodewaynodes {
        NodeWayNodes::Combined(waynodesfn) => {
            read_waynodeways_combined(&waynodesfn, eqt, minw, maxw, msg, numchan)
        }
        NodeWayNodes::InMem(infn, waynodevals) => {
            read_waynodeways_inmem(&infn, waynodevals, eqt, minw, maxw, msg, numchan)
        }
        NodeWayNodes::Seperate(infn, waynodefn, waynodelocs) => read_waynodeways_seperate(
            &infn,
            &waynodefn,
            &waynodelocs,
            eqt,
            minw,
            maxw,
            msg,
            numchan,
        ),
    }
}
