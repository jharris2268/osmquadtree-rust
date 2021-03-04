use crate::callback::{CallFinish, Callback, CallbackMerge, CallbackSync};
use crate::elements::{MinimalBlock, MinimalNode};
use crate::pbfformat::{
    file_length, make_convert_minimal_block_parts, pack_file_block,
    read_all_blocks_parallel_with_progbar, read_all_blocks_with_progbar_stop,
    read_file_block_with_pos, unpack_file_block, FileBlock, HeaderType, ProgBarWrap,
    ReadFileBlocks, WriteFile,
};

use crate::utils::{CallAll, MergeTimings, ReplaceNoneWithTimings, Timer};

use std::fmt;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Result, Seek, SeekFrom};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

use crate::calcqts::packwaynodes::WayNodeTile;
use crate::calcqts::write_quadtrees::WrapWriteFileVec;
use crate::calcqts::{
    CallFinishFileBlocks, FileLocs, NodeWayNodes, /*OtherData,*/ Timings, WayNodeVals,
};

pub struct NodeWayNodeComb {
    pub id: i64,
    pub lon: i32,
    pub lat: i32,
    pub ways: Vec<i64>,
}

impl NodeWayNodeComb {
    pub fn new(nd: &MinimalNode, ways: Vec<i64>) -> NodeWayNodeComb {
        NodeWayNodeComb {
            id: nd.id,
            lon: nd.lon,
            lat: nd.lat,
            ways: ways,
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
/*
pub struct NodeWayNodeCombTile {
    pub vals: Vec<NodeWayNodeComb>,
}

impl NodeWayNodeCombTile {
    pub fn new(vals: Vec<NodeWayNodeComb>) -> NodeWayNodeCombTile {
        NodeWayNodeCombTile { vals }
    }


}
*/
fn resort_waynodes((i, dd): (usize, Vec<FileBlock>)) -> Vec<(i64, Vec<u8>)> {
    let mut pos: u64 = 0;
    let wnt = read_way_node_tiles_vals_fb(&mut pos, i as u64, &dd, 0, 0).expect("?");

    let mut res = Vec::new();
    for vv in wnt.pack_chunks(256 * 1024) {
        res.push((0, pack_file_block("WayNodes", &vv, true).expect("!")));
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
    let mut vv: Vec<Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>> =
        Vec::new();
    for w in wv {
        let w2 = Box::new(ReplaceNoneWithTimings::new(w));

        vv.push(Box::new(Callback::new(Box::new(CallAll::new(
            w2,
            "resort",
            Box::new(resort_waynodes),
        )))));
    }

    let vvm = Box::new(CallbackMerge::new(vv, Box::new(MergeTimings::new())));

    let t = read_waynodevals(waynodevals, vvm).expect("?");

    println!("{}", t);
    waynodesfn
}

fn read_waynodevals<T: CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>(
    waynodevals: WayNodeVals,
    mut vvm: Box<T>,
) -> Result<Timings> {
    match waynodevals {
        WayNodeVals::PackedInMem(ww) => {
            let mut pg = ProgBarWrap::new(100);
            pg.set_range(100);
            pg.set_message("write_waynode_sorted_resort");

            let pf = 100.0 / (ww.len() as f64);
            let mut i = 0.0;

            let mut curr = 0;
            let mut xx = Vec::new();
            for (a, b) in ww {
                pg.prog(pf * i);
                if a != curr {
                    if !xx.is_empty() {
                        vvm.call((curr as usize, xx));
                    }
                    curr = a;
                    xx = Vec::new();
                }
                let fb = unpack_file_block(0, &b)?;
                xx.push(fb);
                i += 1.0;
            }
            if !xx.is_empty() {
                vvm.call((curr as usize, xx));
            }
            pg.finish();
            vvm.finish()
        }
        WayNodeVals::TempFile(fname, locs) => {
            let fobj = File::open(&fname)?;
            let mut fbuf = vec![BufReader::new(fobj)];

            let mut ll = Vec::new();

            for (a, b) in locs {
                let mut xx = Vec::new();
                for (c, _) in b {
                    xx.push((0usize, c));
                }

                ll.push((a, xx))
            }

            Ok(read_all_blocks_parallel_with_progbar(
                &mut fbuf,
                &ll,
                vvm,
                "read waynodevals",
                file_length(&fname),
            ))
        }
    }
}

fn read_way_node_tiles_vals_fb(
    _pos: &mut u64,
    tile: u64,
    vals: &Vec<FileBlock>,
    minw: i64,
    maxw: i64,
) -> Result<WayNodeTile> {
    let mut res = WayNodeTile::new(tile as i64, 0);

    if vals.is_empty() {
        return Ok(res);
    }
    let nv = vals.len();
    for (i, fb) in vals.iter().enumerate() {
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
    }
    res.sort();
    Ok(res)
}

fn read_way_node_tiles_vals_rf(
    pos: &mut u64,
    tile: i64,
    vals: &Vec<&[u8]>,
    minw: i64,
    maxw: i64,
) -> Result<WayNodeTile> {
    let mut res = WayNodeTile::new(tile, 0);

    if vals.is_empty() {
        return Ok(res);
    }
    let nv = vals.len();
    for (i, v) in vals.iter().enumerate() {
        let fb = unpack_file_block(*pos, v)?;
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
    vals: Arc<WayNodeVals>,
    send: mpsc::SyncSender<WayNodeTile>,
    minw: i64,
    maxw: i64,
) {
    match &*vals {
        WayNodeVals::PackedInMem(vals) => {
            let mut pos: u64 = 0;
            let mut curr = -1;
            let mut xx = Vec::new();
            for (k, vv) in vals.iter() {
                if *k != curr {
                    if !xx.is_empty() {
                        let wnt =
                            read_way_node_tiles_vals_rf(&mut pos, curr, &xx, minw, maxw).unwrap();
                        send.send(wnt).expect("send failed");
                    }
                    xx.clear();
                    curr = *k;
                }
                xx.push(&vv);
            }
            if !xx.is_empty() {
                let wnt = read_way_node_tiles_vals_rf(&mut pos, curr, &xx, minw, maxw).unwrap();
                send.send(wnt).expect("send failed");
            }

            drop(send);
        }
        WayNodeVals::TempFile(fname, locs) => {
            read_way_node_tiles_file_send(fname.clone(), locs.clone(), send, minw, maxw);
        }
    }
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
                if pp.len() == 1 {
                    wnt.unpack(&pp[0].data(), minw, maxw)
                        .expect(&format!("failed to unpack"));
                    return wnt;
                }

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
        for (i, fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
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
                    let (np, fb) = read_file_block_with_pos(&mut fbuf, a)
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
        waynodevals: Arc<WayNodeVals>,
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
    T: CallFinish<CallType = Vec<NodeWayNodeComb>>,
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
) -> Vec<NodeWayNodeComb>
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
        res.push(NodeWayNodeComb::new(&n, ways));
    }
    res
}

impl<T, U> CallFinish for CombineNodeWayNodeCB<T, U>
where
    T: CallFinish<CallType = Vec<NodeWayNodeComb>, ReturnType = Timings>,
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
        if !res.is_empty() {
            self.combined_cb.call(res);
        }
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut t = self.combined_cb.finish()?;
        t.add("combinenodewaynode", self.tm);
        Ok(t)
    }
}
/*
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
*/
fn read_waynodeways_inmem<T: CallFinish<CallType = Vec<NodeWayNodeComb>, ReturnType = Timings>>(
    infn: &str,
    stop_after: u64,
    waynodevals: Arc<WayNodeVals>,
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
        read_all_blocks_with_progbar_stop(infn, stop_after, convert, msg).0
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
        read_all_blocks_with_progbar_stop(infn, stop_after, conv_merge, msg).0
    }
}

fn read_waynodeways_seperate<
    T: CallFinish<CallType = Vec<NodeWayNodeComb>, ReturnType = Timings>,
>(
    infn: &str,
    stop_after: u64,
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
        read_all_blocks_with_progbar_stop(infn, stop_after, convert, msg).0
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
        read_all_blocks_with_progbar_stop(infn, stop_after, conv_merge, msg).0
    }
}

pub fn read_nodewaynodes<T: CallFinish<CallType = Vec<NodeWayNodeComb>, ReturnType = Timings>>(
    nodewaynodes: NodeWayNodes,
    eqt: Box<T>,
    minw: i64,
    maxw: i64,
    msg: &str,
    numchan: usize,
) -> Timings {
    match nodewaynodes {
        //NodeWayNodes::Combined(waynodesfn) => {
        //    read_waynodeways_combined(&waynodesfn, eqt, minw, maxw, msg, numchan)
        //}
        NodeWayNodes::InMem(infn, waynodevals, stop_after) => read_waynodeways_inmem(
            &infn,
            stop_after,
            waynodevals,
            eqt,
            minw,
            maxw,
            msg,
            numchan,
        ),
        NodeWayNodes::Seperate(infn, waynodefn, waynodelocs, stop_after) => {
            read_waynodeways_seperate(
                &infn,
                stop_after,
                &waynodefn,
                &waynodelocs,
                eqt,
                minw,
                maxw,
                msg,
                numchan,
            )
        }
    }
}
