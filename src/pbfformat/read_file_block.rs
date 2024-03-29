use std::fs::File;
use std::io;
use std::io::{BufReader, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};

//extern crate flate2;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use channelled_callbacks::CallFinish;
use simple_protocolbuffers as spb;

use crate::utils::Timer;
use crate::logging::{ProgressPercent,ProgressBytes};
use crate::{progress_bytes};
//use indicatif::{ProgressBar, ProgressStyle};

//extern crate lzma_rs;
//extern crate lz4_flex;
extern crate lzma;
use crate::brotli_compression::{compress_brotli, decompress_brotli};

const LZMA_PRESET: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    Uncompressed,
    Zlib,
    Brotli,
    Lzma,
    ZlibLevel(u32),
    BrotliLevel(u32),
    LzmaLevel(u32),
    //Lz4
}


pub fn read_file_data<R: Read>(file: &mut R, nbytes: u64) -> io::Result<Vec<u8>> {
    let mut res = vec![0u8; nbytes as usize];
    file.read_exact(&mut res)?;
    Ok(res)
}

#[derive(Debug)]
pub struct FileBlock {
    pub pos: u64,
    pub len: u64,
    pub block_type: String,
    pub data_raw: Vec<u8>,
    pub data_len: u64,
    pub compression_type: CompressionType,
}
impl FileBlock {
    pub fn new() -> FileBlock {
        FileBlock {
            pos: 0,
            len: 0,
            block_type: String::new(),
            data_raw: Vec::new(),
            data_len: 0,
            compression_type: CompressionType::Uncompressed,
        }
    }

    pub fn data(&self) -> Vec<u8> {

        match self.compression_type {
            CompressionType::Uncompressed => self.data_raw.clone(),
            CompressionType::Zlib | CompressionType::ZlibLevel(_) => self.read_data_zlib(),
            CompressionType::Brotli | CompressionType::BrotliLevel(_)=> self.read_data_brotli(),
            CompressionType::Lzma | CompressionType::LzmaLevel(_)=> self.read_data_lzma(),
            //CompressionType::Lz4 => self.read_data_lz4(),
        }
    }
    
    fn read_data_zlib(&self) -> Vec<u8> {
        let mut comp = Vec::new();
        let mut xx = ZlibDecoder::new(&self.data_raw[..]);
        
        xx.read_to_end(&mut comp).expect(&format!("failed to decompress data at pos {}", self.pos));
        comp
    }

    fn read_data_brotli(&self) -> Vec<u8> {

        decompress_brotli(&self.data_raw[..]).expect(&format!("failed to decompress data at pos {}", self.pos))
    }

    fn read_data_lzma(&self) -> Vec<u8> {
        //let mut bf = std::io::BufReader::new(&self.data_raw[..]);
        lzma::decompress(&self.data_raw[..]).unwrap()
        
    }
    /*
    fn read_data_lz4(&self) -> Vec<u8> {
        lz4_flex::decompress_size_prepended(&self.data_raw[..]).unwrap()//, self.data_len as usize).unwrap()
    }*/
        
}

pub fn file_position<F: Seek>(file: &mut F) -> io::Result<u64> {
    file.seek(SeekFrom::Current(0))
}
pub fn read_file_block<F: Seek + Read>(file: &mut F) -> io::Result<FileBlock> {
    let pos = file_position(file)?;
    match read_file_block_with_pos(file, pos) {
        Ok((_, y)) => Ok(y),
        Err(e) => Err(Error::new(ErrorKind::Other, format!("{:?} at {}", e, pos))),
    }
    /*let (_, y) = read_file_block_with_pos(file, pos)?;
    Ok(y)*/
}

pub fn read_file_block_with_pos<F: Read>(
    file: &mut F,
    mut pos: u64,
) -> io::Result<(u64, FileBlock)> {
    let mut fb = FileBlock::new();
    fb.pos = pos;

    let a = match read_file_data(file, 4) {
        Ok(data) => data,
        Err(err) => return Err(err),
    };

    pos += 4;

    let (l, _) = spb::read_uint32(&a, 0)?;

    let b = read_file_data(file, l)?;
    pos += l;

    let bb = spb::IterTags::new(&b);
    
    let mut ln = 0;
    for tg in bb {
        match tg {
            spb::PbfTag::Value(3, v) => ln = v,
            spb::PbfTag::Data(1, d) => fb.block_type = std::str::from_utf8(&d).unwrap().to_string(),
            _ => { return Err(Error::new(ErrorKind::Other, format!("?? wrong tag @ {} {:?}", pos, tg))); },
        }
    }
    fb.len = 4 + l + ln;

    let c = read_file_data(file, ln).unwrap();
    pos += ln;

    for tg in spb::IterTags::new(&c) {
        match tg {
            spb::PbfTag::Data(1, d) => fb.data_raw = d.to_vec(),
            spb::PbfTag::Value(2, v) => {fb.data_len = v; }//fb.is_comp = true,
            spb::PbfTag::Data(3, d) => {
                fb.compression_type = CompressionType::Zlib;
                fb.data_raw = d.to_vec();
            },
            spb::PbfTag::Data(4, d) => {
                fb.compression_type = CompressionType::Lzma;
                fb.data_raw = d.to_vec();
            },
            /*spb::PbfTag::Data(6, d) => {
                fb.compression_type = CompressionType::Lz4;
                fb.data_raw = d.to_vec();
            },*/
            spb::PbfTag::Data(8, d) => {
                fb.compression_type = CompressionType::Brotli;
                fb.data_raw = d.to_vec();
            },
            _ => { return Err(Error::new(ErrorKind::Other, format!("?? wrong tag @ {} {:?}", pos, tg))); },
        }
    }

    Ok((pos, fb))
}

pub fn unpack_file_block(pos: u64, data: &[u8]) -> io::Result<FileBlock> {
    let s = read_file_block_with_pos(&mut Cursor::new(data), pos)?;
    Ok(s.1)
}

pub fn pack_file_block(blockname: &str, data: &[u8], compression_type: &CompressionType) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    match compression_type {
        CompressionType::Zlib => {
            let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
            e.write_all(&data[..])?;

            let comp = e.finish()?;
            body.reserve(comp.len() + 25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 3, &comp[..]);
        },
        CompressionType::ZlibLevel(level) => {
            let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::new(*level));
            e.write_all(&data[..])?;

            let comp = e.finish()?;
            body.reserve(comp.len() + 25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 3, &comp[..]);
        },
        CompressionType::Uncompressed => {
            body.reserve(data.len() + 5);
            spb::pack_data(&mut body, 1, &data[..]);
        },
        CompressionType::Brotli => {

            let comp = compress_brotli(data, 6)?;
            
            body.reserve(comp.len()+25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 8, &comp[..]);
        },
        CompressionType::BrotliLevel(level) => {

            let comp = compress_brotli(data, *level)?;
            
            body.reserve(comp.len()+25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 8, &comp[..]);
        },
        CompressionType::Lzma => {
            let comp = match lzma::compress(data, LZMA_PRESET) {
                Ok(r) => r,
                Err(e) => {return Err(Error::new(ErrorKind::Other, format!("{:?}", e))); },
            };
            body.reserve(comp.len()+25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 4, &comp[..]);
        },
        CompressionType::LzmaLevel(level) => {
            let comp = match lzma::compress(data, *level) {
                Ok(r) => r,
                Err(e) => {return Err(Error::new(ErrorKind::Other, format!("{:?}", e))); },
            };
            /*

            let mut bf = std::io::BufReader::new(data);
            let mut comp: Vec<u8> = Vec::new();
            lzma_rs::lzma_compress(&mut bf, &mut comp)?;*/
            body.reserve(comp.len()+25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 4, &comp[..]);
        },
        /*
        CompressionType::Lz4 => {
            let comp = lz4_flex::compress_prepend_size(&data[..]);
            
            body.reserve(comp.len()+25);
            spb::pack_value(&mut body, 2, data.len() as u64);
            spb::pack_data(&mut body, 6, &comp[..]);
        },*/
    }

    let mut head = Vec::with_capacity(25);
    spb::pack_data(&mut head, 1, blockname.as_bytes());
    spb::pack_value(&mut head, 3, body.len() as u64);

    let mut result = Vec::with_capacity(4 + head.len() + body.len());
    spb::write_uint32(&mut result, head.len() as u32);
    result.extend(head);
    result.extend(body);

    Ok(result)
        
}

pub struct ReadFileBlocks<'a, R: Read> {
    file: &'a mut R,
    p: u64,
    stop_at: u64,
}

impl<R> ReadFileBlocks<'_, R>
where
    R: Read + Seek,
{
    pub fn new(file: &mut R) -> ReadFileBlocks<R> {
        let p = file_position(file).expect("!");
        ReadFileBlocks {
            file: file,
            p: p,
            stop_at: u64::MAX,
        }
    }
}
impl<R> ReadFileBlocks<'_, R>
where
    R: Read,
{
    pub fn new_at_start(file: &mut R) -> ReadFileBlocks<R> {
        ReadFileBlocks {
            file: file,
            p: 0,
            stop_at: u64::MAX,
        }
    }

    pub fn new_at_start_with_stop(file: &mut R, stop_at: u64) -> ReadFileBlocks<R> {
        ReadFileBlocks {
            file: file,
            p: 0,
            stop_at: stop_at,
        }
    }
}

impl<R> Iterator for ReadFileBlocks<'_, R>
where
    R: Read,
{
    type Item = FileBlock;

    fn next(&mut self) -> Option<Self::Item> {
        if self.p > self.stop_at {
            return None;
        }
        match read_file_block_with_pos(self.file, self.p) {
            Ok((p, fb)) => {
                self.p = p;
                Some(fb)
            }
            Err(err) => {
                match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        //at end of file
                    }
                    _ => {
                        panic!("failed to read {}", err);
                    }
                }
                None
            }
        }
    }
}

pub struct ReadFileBlocksOwn {
    bf: BufReader<File>,
    p: u64
}

impl ReadFileBlocksOwn {
    pub fn new(fname: &str) -> std::io::Result<ReadFileBlocksOwn> {
        let fs = File::open(fname)?;
        let bf = BufReader::new(fs);
        Ok(ReadFileBlocksOwn{bf:bf,p:0})
    }
}

impl Iterator for ReadFileBlocksOwn {
    type Item = FileBlock;
    fn next(&mut self) -> Option<Self::Item> {
        match read_file_block_with_pos(&mut self.bf, self.p) {
            Ok((p, fb)) => {
                self.p = p;
                Some(fb)
            }
            Err(err) => {
                match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        //at end of file
                    }
                    _ => {
                        panic!("failed to read {}", err);
                    }
                }
                None
            }
        }
    }
}

pub fn read_all_blocks<T, U>(fname: &str, mut pp: Box<T>) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let tx=Timer::new();
    let f = File::open(fname).expect("fail");
    let mut fbuf = BufReader::new(f);
    for (i, fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
        pp.call((i, fb));
    }
    (pp.finish().expect("finish failed"), tx.since())
}

pub fn file_length(fname: &str) -> u64 {
    std::fs::metadata(fname)
        .expect(&format!("failed to open {}", fname))
        .len()
}

pub fn read_all_blocks_prog<R: Read, T, U>(
    fobj: &mut R,
    flen: u64,
    mut pp: Box<T>,
    pb: &Box<dyn ProgressPercent>,
    start_percent: f64,
    end_percent: f64
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let ct = Timer::new();

    let pf = (end_percent-start_percent) / (flen as f64);

    for (i, fb) in ReadFileBlocks::new_at_start(fobj).enumerate() {
        pb.progress_percent((fb.pos as f64) * pf + start_percent);

        pp.call((i, fb));
    }
    pb.progress_percent(end_percent);
    let r = pp.finish().expect("finish failed");

    
    (r, ct.since())
}

pub fn read_all_blocks_prog_fpos<R: Read, T, U>(
    fobj: &mut R,
    mut pp: Box<T>,
    pg: Box<dyn ProgressBytes>
    
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let ct = Timer::new();

    for (i, fb) in ReadFileBlocks::new_at_start(fobj).enumerate() {
        
        pg.progress_bytes(fb.pos);
        pp.call((i, fb));
    }
    
    pg.finish();
    (pp.finish().expect("finish failed"), ct.since())
}

pub fn read_all_blocks_prog_fpos_stop<R: Read, T, U>(
    fobj: &mut R,
    stop_at: u64,
    mut pp: Box<T>,
    pb: Box<dyn ProgressBytes>,
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let ct = Timer::new();
    for (i, fb) in ReadFileBlocks::new_at_start_with_stop(fobj, stop_at).enumerate() {
        pb.progress_bytes(fb.pos);
        pp.call((i, fb));
    }
    pb.finish();
    (pp.finish().expect("finish failed"), ct.since())
}

pub fn read_all_blocks_with_progbar<T, U>(fname: &str, pp: Box<T>, msg: &str) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let fl = file_length(fname);
    //let pb = ProgBarWrap::new_filebytes(fl);
    //pb.set_message(msg);
    let pg = progress_bytes!(msg, fl);

    let fobj = File::open(fname).expect("failed to open file");
    let mut fbuf = BufReader::new(fobj);

    read_all_blocks_prog_fpos(&mut fbuf, pp, pg)
}

pub fn read_all_blocks_vec_with_progbar<T, U>(fname: &str, mut pp: Box<T>, msg: &str) -> (U, f64)
where
    T: CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let fl = file_length(fname);
    //let pb = ProgBarWrap::new_filebytes(fl);
    //pb.set_message(msg);
    let pg = progress_bytes!(msg, fl);

    let fobj = File::open(fname).expect("failed to open file");
    let mut fbuf = BufReader::new(fobj);

    let ct = Timer::new();

    for (i, fb) in ReadFileBlocks::new_at_start(&mut fbuf).enumerate() {
        
        pg.progress_bytes(fb.pos);
        pp.call((i, vec![fb]));
    }
    
    pg.finish();
    (pp.finish().expect("finish failed"), ct.since())
}


pub fn read_all_blocks_with_progbar_stop<T, U>(
    fname: &str,
    stop_after: u64,
    pp: Box<T>,
    msg: &str,
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
{
    let pb = progress_bytes!(msg, stop_after);
    

    let fobj = File::open(fname).expect("failed to open file");
    let mut fbuf = BufReader::new(fobj);

    read_all_blocks_prog_fpos_stop(&mut fbuf, stop_after, pp, pb)
}


pub fn read_all_blocks_locs_prog<R, T, U>(
    fobj: &mut R,
    fname: &str,
    locs: Vec<u64>,
    mut pp: Box<T>,
    pb: &Box<dyn ProgressPercent>,
    start_percent: f64,
    end_percent: f64
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, FileBlock), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
    R: Read + Seek,
{
    let ct = Timer::new();

    let pf = (end_percent-start_percent) / (locs.len() as f64);

    for (i, l) in locs.iter().enumerate() {
        fobj.seek(SeekFrom::Start(*l))
            .expect(&format!("failed to read {} @ {}", fname, *l));
        let (_, fb) = read_file_block_with_pos(fobj, *l)
            .expect(&format!("failed to read {} @ {}", fname, *l));

        pb.progress_percent(((i + 1) as f64) * pf + start_percent);

        pp.call((i, fb));
    }
    pb.progress_percent(end_percent);
    (pp.finish().expect("finish failed"), ct.since())
}

pub fn read_all_blocks_parallel_prog<T, U, F, Q>(
    fbufs: &mut Vec<F>,
    locs: &Vec<(Q, Vec<(usize, u64)>)>,
    mut pp: Box<T>,
    pb: Box<dyn ProgressPercent>,
) -> (U, f64)
where
    T: CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
    F: Seek + Read,
{
    let ct = Timer::new();

    let mut fposes = Vec::new();
    for f in fbufs.iter_mut() {
        fposes.push(file_position(f).expect("!"));
    }
    let pf = 100.0 / (locs.len() as f64);
    for (j, (_, ll)) in locs.iter().enumerate() {
        let mut fbs = Vec::new();
        for (a, b) in ll {
            if fposes[*a] != *b {
                fbufs[*a]
                    .seek(SeekFrom::Start(*b))
                    .expect(&format!("failed to read {} @ {}", *a, *b));
            }

            let (x, y) = read_file_block_with_pos(&mut fbufs[*a], *b)
                .expect(&format!("failed to read {} @ {}", *a, *b));

            fbs.push(y);
            fposes[*a] = x;
        }

        pb.progress_percent(((j + 1) as f64) * pf);

        pp.call((j, fbs));
    }
    pb.finish();
    (pp.finish().expect("finish failed"), ct.since())
}

pub fn read_all_blocks_parallel_with_progbar<T, U, F, Q>(
    fbufs: &mut Vec<F>,
    locs: &Vec<(Q, Vec<(usize, u64)>)>,
    mut pp: Box<T>,
    msg: &str,
    total_len: u64,
) -> U
where
    T: CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = U> + ?Sized,
    U: Send + Sync + 'static,
    F: Seek + Read,
{
    let mut fposes = Vec::new();
    for f in fbufs.iter_mut() {
        fposes.push(file_position(f).expect("!"));
    }

    let pb = progress_bytes!(msg,total_len);
    

    let mut pos = 0;
    for (j, (_, ll)) in locs.iter().enumerate() {
        let mut fbs = Vec::new();

        for (a, b) in ll {
            if fposes[*a] != *b {
                fbufs[*a]
                    .seek(SeekFrom::Start(*b))
                    .expect(&format!("failed to read {} @ {}", *a, *b));
            }

            let (x, y) = read_file_block_with_pos(&mut fbufs[*a], *b)
                .expect(&format!("failed to read {} @ {}", *a, *b));

            fbs.push(y);
            fposes[*a] = x;
            pos += x - *b;
        }

        pb.progress_bytes(pos);

        pp.call((j, fbs));
    }
    pb.finish();
    pp.finish().expect("finish failed")
}
