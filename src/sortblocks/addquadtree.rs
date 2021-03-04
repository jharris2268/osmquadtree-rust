use std::fs::File;
use std::io;
use std::io::{BufReader, Error, ErrorKind};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::elements::PrimitiveBlock;
use crate::pbfformat::{FileBlock, ReadFileBlocks};

use crate::callback::CallFinish;
use crate::elements::{ElementType, Quadtree, QuadtreeBlock};

use crate::utils::{CallAll, ThreadTimer};

use crate::sortblocks::Timings;

fn read_quadtreeblock_tiles_send(
    qtsfn: String,
    send: mpsc::SyncSender<Vec<(ElementType, i64, Quadtree)>>,
) {
    let mut fobj = BufReader::new(File::open(qtsfn).expect("failed to open file"));

    for fb in ReadFileBlocks::new(&mut fobj) {
        if fb.block_type == "OSMData" {
            let bl = QuadtreeBlock::unpack(0, 0, &fb.data()).expect("failed to read block");
            let mut cc = Vec::with_capacity(bl.nodes.len() + bl.ways.len() + bl.relations.len());
            cc.extend(bl.nodes.iter().map(|(a, b)| (ElementType::Node, *a, *b)));
            cc.extend(bl.ways.iter().map(|(a, b)| (ElementType::Way, *a, *b)));
            cc.extend(
                bl.relations
                    .iter()
                    .map(|(a, b)| (ElementType::Relation, *a, *b)),
            );
            send.send(cc).expect("send failed");
        }
    }

    drop(send)
}

pub struct ChannelQuadtreeBlockFlatIter {
    //jh: thread::JoinHandle<()>,
    recv: Arc<Mutex<mpsc::Receiver<Vec<(ElementType, i64, Quadtree)>>>>,
    hadfirst: bool,
    curr: Option<Vec<(ElementType, i64, Quadtree)>>,
    idx: usize,
}

impl ChannelQuadtreeBlockFlatIter {
    pub fn new(qtsfn: &str) -> ChannelQuadtreeBlockFlatIter {
        let (s, r) = mpsc::sync_channel(1);
        let rx = Arc::new(Mutex::new(r));
        let qtsfn = qtsfn.to_owned();

        /*let jh =*/
        thread::spawn(move || read_quadtreeblock_tiles_send(qtsfn, s));
        ChannelQuadtreeBlockFlatIter {
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

impl Iterator for ChannelQuadtreeBlockFlatIter {
    type Item = (ElementType, i64, Quadtree);

    fn next(&mut self) -> Option<(ElementType, i64, Quadtree)> {
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
                let r = wnt[self.idx].clone();
                self.idx += 1;

                return Some(r);
            }
        }
    }
}

pub struct AddQuadtree<T> {
    qts: Box<ChannelQuadtreeBlockFlatIter>,
    curr: Option<(ElementType, i64, Quadtree)>,
    tot: f64,
    out: Box<T>,
}

impl<T> AddQuadtree<T>
where
    T: CallFinish,
{
    pub fn new(qtsfn: &str, out: Box<T>) -> AddQuadtree<T> {
        let mut qts = Box::new(ChannelQuadtreeBlockFlatIter::new(qtsfn));
        let curr = qts.next();
        let tot = 0.0;
        AddQuadtree {
            qts,
            curr,
            out,
            tot,
        }
    }
}

impl<T> CallFinish for AddQuadtree<T>
where
    T: CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>,
{
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;

    fn call(&mut self, mut bl: PrimitiveBlock) {
        let ct = ThreadTimer::new();
        for n in bl.nodes.iter_mut() {
            match &self.curr {
                None => {
                    panic!("ran out of qts");
                }
                Some((ty, id, qt)) => {
                    if *ty == ElementType::Node && *id == n.id {
                        n.quadtree = *qt;
                        self.curr = self.qts.next();
                    } else {
                        panic!("out of sync");
                    }
                }
            }
        }

        for w in bl.ways.iter_mut() {
            match &self.curr {
                None => {
                    panic!("ran out of qts");
                }
                Some((ty, id, qt)) => {
                    if *ty == ElementType::Way && *id == w.id {
                        w.quadtree = *qt;
                        self.curr = self.qts.next();
                    } else {
                        panic!("out of sync");
                    }
                }
            }
        }

        for r in bl.relations.iter_mut() {
            match &self.curr {
                None => {
                    panic!("ran out of qts");
                }
                Some((ty, id, qt)) => {
                    if *ty == ElementType::Relation {
                        if *id > r.id {
                            r.quadtree = Quadtree::new(0);
                        } else if *id == r.id {
                            r.quadtree = *qt;
                            self.curr = self.qts.next();
                        } else {
                            panic!("out of sync");
                        }
                    }
                }
            }
        }
        self.tot += ct.since();
        self.out.call(bl);
    }

    fn finish(&mut self) -> io::Result<Self::ReturnType> {
        match self.curr {
            None => {
                let mut x = self.out.finish()?;
                x.add("add quadtrees", self.tot);
                return Ok(x);
            }
            Some(_) => Err(Error::new(ErrorKind::Other, "qts remaining...")),
        }
    }
}

pub fn make_unpackprimblock<T: CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>>(
    out: Box<T>,
) -> Box<impl CallFinish<CallType = (usize, FileBlock), ReturnType = Timings>> {
    let conv = Box::new(|(i, fb): (usize, FileBlock)| {
        if fb.block_type == "OSMData" {
            PrimitiveBlock::read(i as i64, fb.pos, &fb.data(), false, false)
                .expect("failed to read")
        } else {
            PrimitiveBlock::new(i as i64, fb.pos)
        }
    });
    return Box::new(CallAll::new(out, "unpack", conv));
}
