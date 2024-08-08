extern crate serde_json;

use std::fs::File;

use std::io::Write;

use channelled_callbacks::{CallFinish, CallAll, Result as ccResult};
use crate::elements::{Bbox, Block, PrimitiveBlock, Quadtree};
use crate::pbfformat::{pack_file_block, CompressionType};
use crate::pbfformat::{make_header_block_stored_locs, HeaderType};

use crate::utils::{ThreadTimer, Error};

use crate::sortblocks::{OtherData, Timings};

pub struct WriteFile {
    writefile: crate::pbfformat::WriteFile,
}

impl WriteFile {
    pub fn new(outfn: &str, header_type: HeaderType) -> WriteFile {
        WriteFile {
            writefile: crate::pbfformat::WriteFile::new(outfn, header_type),
        }
    }
    pub fn with_bbox(outfn: &str, header_type: HeaderType, bbox: Option<&Bbox>) -> WriteFile {
        WriteFile {
            writefile: crate::pbfformat::WriteFile::with_bbox(outfn, header_type, bbox),
        }
    }
    pub fn with_compression_type(outfn: &str, header_type: HeaderType, bbox: Option<&Bbox>, compression_type: CompressionType) -> WriteFile {
        WriteFile {
            writefile: crate::pbfformat::WriteFile::with_compression_type(outfn, header_type, bbox, compression_type),
        }
    }
}

impl CallFinish for WriteFile {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;
    type ErrorType = Error;
    fn call(&mut self, x: Vec<(i64, Vec<u8>)>) {
        self.writefile.call(x);
    }

    fn finish(&mut self) -> ccResult<Timings, Error> {
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
    compression_type: CompressionType,
}

impl WriteFileInternalLocs {
    pub fn new(fname: &str, ischange: bool) -> WriteFileInternalLocs {
        WriteFileInternalLocs {
            fname: String::from(fname),
            ischange: ischange,
            data: Vec::new(),
            compression_type: CompressionType::Zlib,
        }
    }
}

impl CallFinish for WriteFileInternalLocs {
    type CallType = (Quadtree, Vec<u8>);
    type ReturnType = Timings;
    type ErrorType = Error;
    fn call(&mut self, q_d: Self::CallType) {
        self.data.push(q_d);
    }

    fn finish(&mut self) -> ccResult<Timings, Error> {
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
            &self.compression_type,
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

pub fn make_packprimblock_qtindex<
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings, ErrorType=Error> + ?Sized,
>(
    out: Box<T>,
    includeqts: bool,
    compression_type: CompressionType
) -> Box<impl CallFinish<CallType = PrimitiveBlock, ReturnType = Timings, ErrorType=Error>> {
    
    let conv = Box::new(move |bl: PrimitiveBlock| {
        if bl.len() == 0 {
            return vec![];
        }
        let xx = bl.pack(includeqts, false).expect("failed to pack");
        let ob = pack_file_block("OSMData", &xx, &compression_type).expect("failed to pack fb");

        vec![(bl.quadtree.as_int(), ob)]
    });
    return Box::new(CallAll::new(out, "pack", conv));
}
pub fn make_packprimblock_zeroindex<
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings, ErrorType=Error> + ?Sized,
>(
    out: Box<T>,
    includeqts: bool,
    compression_type: CompressionType
) -> Box<impl CallFinish<CallType = PrimitiveBlock, ReturnType = Timings, ErrorType=Error>> {

    
    let conv = Box::new(move |bl: PrimitiveBlock| {
        if bl.len() == 0 {
            return vec![];
        }
        let xx = bl.pack(includeqts, false).expect("failed to pack");
        let ob = pack_file_block("OSMData", &xx, &compression_type).expect("failed to pack fb");

        vec![(0, ob)]
    });
    return Box::new(CallAll::new(out, "pack", conv));
}

pub fn make_packprimblock_many<
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings, ErrorType=Error> + ?Sized,
>(
    out: Box<T>,
    includeqts: bool,
    compression_type: CompressionType
) -> Box<impl CallFinish<CallType = Vec<(i64, PrimitiveBlock)>, ReturnType = Timings, ErrorType=Error>> {
    
    let conv = Box::new(move |bls: Vec<(i64, PrimitiveBlock)>| {
        let mut res = Vec::new();
        for (i, bl) in bls {
            if bl.len() > 0 {
                let xx = bl.pack(includeqts, false).expect("failed to pack");
                let ob = pack_file_block("OSMData", &xx, &compression_type).expect("failed to pack fb");

                res.push((i, ob));
            }
        }
        res
    });
    return Box::new(CallAll::new(out, "pack", conv));
}
