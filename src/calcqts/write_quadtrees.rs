use crate::callback::{CallFinish, Callback, CallbackSync};
use crate::elements::{Quadtree, QuadtreeBlock};
use crate::pbfformat::header_block::HeaderType;
use crate::pbfformat::writefile::WriteFile;
use crate::utils::ReplaceNoneWithTimings;

use crate::calcqts::{OtherData, Timings};
use crate::pbfformat::read_file_block;

use std::io::Result;

pub struct WrapWriteFile {
    writefile: WriteFile,
}
impl WrapWriteFile {
    pub fn new(writefile: WriteFile) -> WrapWriteFile {
        WrapWriteFile { writefile }
    }
}
impl CallFinish for WrapWriteFile {
    type CallType = Vec<u8>;
    type ReturnType = Timings;

    fn call(&mut self, x: Vec<u8>) {
        self.writefile.call(vec![(0, x)]);
    }
    fn finish(&mut self) -> Result<Timings> {
        let (t, _) = self.writefile.finish()?;
        let mut tm = Timings::new();
        tm.add("writefile", t);
        Ok(tm)
    }
}

impl WrapWriteFileVec {
    pub fn new(writefile: WriteFile) -> WrapWriteFileVec {
        WrapWriteFileVec { writefile }
    }
}

pub struct WrapWriteFileVec {
    writefile: WriteFile,
}

impl CallFinish for WrapWriteFileVec {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, x: Vec<(i64, Vec<u8>)>) {
        self.writefile.call(x);
    }
    fn finish(&mut self) -> Result<Timings> {
        let (t, _) = self.writefile.finish()?;
        let mut tm = Timings::new();
        tm.add("writefile", t);
        Ok(tm)
    }
}

struct WriteQuadTreePack<T> {
    out: Box<T>,
}
impl<T> WriteQuadTreePack<T>
where
    T: CallFinish<CallType = Vec<u8>> + Sync + Send + 'static,
{
    pub fn new(out: Box<T>) -> WriteQuadTreePack<T> {
        WriteQuadTreePack { out }
    }
}

impl<T> CallFinish for WriteQuadTreePack<T>
where
    T: CallFinish<CallType = Vec<u8>> + Sync + Send + 'static,
{
    type CallType = Box<QuadtreeBlock>;
    type ReturnType = T::ReturnType;

    fn call(&mut self, t: Self::CallType) {
        let mut t = t;
        let p = t.pack().expect("failed to pack");
        let b = read_file_block::pack_file_block("OSMData", &p, true).expect("failed to pack");

        self.out.call(b);
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        self.out.finish()
    }
}

pub struct WriteQuadTree {
    packs: Vec<Box<Callback<Box<QuadtreeBlock>, Timings>>>,
    numwritten: usize,
    
}

impl WriteQuadTree {
    pub fn new(outfn: &str) -> WriteQuadTree {
        let outs = CallbackSync::new(
            Box::new(WrapWriteFile {
                writefile: WriteFile::new(outfn, HeaderType::NoLocs),
            }),
            4,
        );
        let mut packs = Vec::new();
        for o in outs {
            let o2 = Box::new(ReplaceNoneWithTimings::new(o));
            packs.push(Box::new(Callback::new(Box::new(WriteQuadTreePack::new(
                o2,
            )))));
        }

        let numwritten = 0;
        //let byteswritten = 0;
        
        WriteQuadTree {
            packs,
            numwritten,
        }
    }
}

impl CallFinish for WriteQuadTree {
    type CallType = Box<QuadtreeBlock>;
    type ReturnType = Timings;

    fn call(&mut self, t: Self::CallType) {
        
        let i = self.numwritten % 4;
        self.numwritten += 1;
        
        self.packs[i].call(t);
    }
    fn finish(&mut self) -> Result<Timings> {
        let mut r = Timings::new();
        let mut byteswritten = 0;
        for p in self.packs.iter_mut() {
            r.combine(p.finish().expect("finish failed"));
        }
        for (_, b) in &r.others {
            match b {
                OtherData::FileLen(f) => byteswritten += f,
                _ => {}
            }
        }

        //let x = self.out.finish()?;
        println!(
            "{} written, [{} bytes]",
            self.numwritten,
            byteswritten
        );

        Ok(r)
    }
}

pub struct PackQuadtrees {
    out: Box<dyn CallFinish<CallType = Box<QuadtreeBlock>, ReturnType = Timings>>,
    limit: usize,
    curr: Box<QuadtreeBlock>,
}

impl PackQuadtrees {
    pub fn new(
        out: Box<dyn CallFinish<CallType = Box<QuadtreeBlock>, ReturnType = Timings>>,
        limit: usize,
    ) -> PackQuadtrees {
        let curr = Box::new(QuadtreeBlock::with_capacity(limit));
        PackQuadtrees { out, limit, curr }
    }

    pub fn add_node(&mut self, n: i64, q: Quadtree) {
        self.curr.add_node(n, q);
        self.check_pack_and_write();
    }

    pub fn add_way(&mut self, n: i64, q: Quadtree) {
        self.curr.add_way(n, q);
        self.check_pack_and_write();
    }
    pub fn add_relation(&mut self, n: i64, q: Quadtree) {
        self.curr.add_relation(n, q);
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
        let t = std::mem::replace(
            &mut self.curr,
            Box::new(QuadtreeBlock::with_capacity(self.limit)),
        );

        self.out.call(t);
    }
}
