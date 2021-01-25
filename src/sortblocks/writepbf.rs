extern crate serde_json;

use std::fs::File;
use std::io;
use std::io::Write;

use crate::callback::CallFinish;
use crate::elements::{PrimitiveBlock, Quadtree,Bbox};
use crate::pbfformat::header_block::{make_header_block_stored_locs, HeaderType};
use crate::pbfformat::read_file_block::pack_file_block;
use crate::pbfformat::writefile;
use crate::utils::{CallAll, ThreadTimer};

use crate::sortblocks::{OtherData, Timings};

pub struct WriteFile {
    writefile: writefile::WriteFile,
}

impl WriteFile {
    pub fn new(outfn: &str, header_type: HeaderType) -> WriteFile {
        WriteFile {
            writefile: writefile::WriteFile::new(outfn, header_type),
        }
    }
    pub fn with_bbox(outfn: &str, header_type: HeaderType, bbox: Option<&Bbox>) -> WriteFile {
        WriteFile {
            writefile: writefile::WriteFile::with_bbox(outfn, header_type, bbox),
        }
    }
}

impl CallFinish for WriteFile {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, x: Vec<(i64, Vec<u8>)>) {
        self.writefile.call(x);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let (tm, ls) = self.writefile.finish()?;

        let mut o = Timings::new();
        o.add("write", tm);
        if !ls.is_empty() {
            o.add_other("locations", OtherData::FileLocs(ls));
        }

        Ok(o)
    }
}

pub struct WriteFileInternalLocs {
    fname: String,
    ischange: bool,
    data: Vec<(Quadtree, Vec<u8>)>,
}

impl WriteFileInternalLocs {
    pub fn new(fname: &str, ischange: bool) -> WriteFileInternalLocs {
        WriteFileInternalLocs {
            fname: String::from(fname),
            ischange: ischange,
            data: Vec::new(),
        }
    }
}

impl CallFinish for WriteFileInternalLocs {
    type CallType = (Quadtree, Vec<u8>);
    type ReturnType = Timings;

    fn call(&mut self, q_d: Self::CallType) {
        self.data.push(q_d);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let tx = ThreadTimer::new();
        self.data.sort_by_key(|p| p.0);
        let mut locs = Vec::with_capacity(self.data.len());
        for (a, b) in &self.data {
            locs.push((a.clone(), b.len() as u64));
        }
        let mut outf = File::create(&self.fname).expect("failed to create");
        let hb = pack_file_block(
            "OSMHeader",
            &make_header_block_stored_locs(self.ischange, locs),
            true,
        )
        .expect("?");
        let mut pos = hb.len() as u64;
        outf.write_all(&hb).expect("?");

        let mut ls = Vec::with_capacity(self.data.len());
        for (a, b) in &self.data {
            ls.push((a.as_int(), vec![(b.len() as u64, pos)]));
            pos += b.len() as u64;
            outf.write_all(&b).expect("?");
        }

        let mut tm = Timings::new();
        tm.add("writefileinternallocs", tx.since());
        tm.add_other("locations", OtherData::FileLocs(ls));
        Ok(tm)
    }
}

pub fn make_packprimblock<T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings> + ?Sized>(
    out: Box<T>,
    includeqts: bool,
) -> Box<impl CallFinish<CallType = PrimitiveBlock, ReturnType = Timings>> {
    let conv = Box::new(move |bl: PrimitiveBlock| {
        if bl.len() == 0 {
            return vec![];
        }
        let xx = bl.pack(includeqts, false).expect("failed to pack");
        let ob = pack_file_block("OSMData", &xx, true).expect("failed to pack fb");
        let i = if includeqts { bl.quadtree.as_int() } else { bl.index as i64};
        vec![(i, ob)]
    });
    return Box::new(CallAll::new(out, "pack", conv));
}

pub fn make_packprimblock_many<
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings> + ?Sized,
>(
    out: Box<T>,
    includeqts: bool,
) -> Box<impl CallFinish<CallType = Vec<PrimitiveBlock>, ReturnType = Timings>> {
    let conv = Box::new(move |bls: Vec<PrimitiveBlock>| {
        let mut res = Vec::new();
        for bl in bls {
            if bl.len() > 0 {
                let xx = bl.pack(includeqts, false).expect("failed to pack");
                let ob = pack_file_block("OSMData", &xx, true).expect("failed to pack fb");
                let i = if includeqts { bl.quadtree.as_int() } else { bl.index as i64};
                res.push((i, ob));
            }
        }
        res
    });
    return Box::new(CallAll::new(out, "pack", conv));
}
