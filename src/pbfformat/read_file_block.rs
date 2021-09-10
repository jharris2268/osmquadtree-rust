use std::fs::File;
use std::io;
use std::io::{BufReader, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};

//extern crate flate2;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;

use channelled_callbacks::CallFinish;
use simple_protocolbuffers as spb;

use crate::utils::Checktime;
use crate::logging::{ProgressPercent,ProgressBytes};
use crate::{progress_bytes};
//use indicatif::{ProgressBar, ProgressStyle};

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
    pub is_comp: bool,
}
impl FileBlock {
    pub fn new() -> FileBlock {
        FileBlock {
            pos: 0,
            len: 0,
            block_type: String::new(),
            data_raw: Vec::new(),
            is_comp: false,
        }
    }

    pub fn data(&self) -> Vec<u8> {
        if !self.is_comp {
            return self.data_raw.clone();
        }

        let mut comp = Vec::new();
        let mut xx = ZlibDecoder::new(&self.data_raw[..]);
        
        xx.read_to_end(&mut comp).expect(&format!("failed to decompress data at pos {}", self.pos));
        comp
    }
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
    //println!("{:?}", bb);
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
            spb::PbfTag::Value(2, _) => fb.is_comp = true,
            spb::PbfTag::Data(3, d) => fb.data_raw = d.to_vec(),
            _ => { return Err(Error::new(ErrorKind::Other, format!("?? wrong tag @ {} {:?}", pos, tg))); },
        }
    }

    Ok((pos, fb))
}

pub fn unpack_file_block(pos: u64, data: &[u8]) -> io::Result<FileBlock> {
    let s = read_file_block_with_pos(&mut Cursor::new(data), pos)?;
    Ok(s.1)
}

pub fn pack_file_block(blockname: &str, data: &[u8], compress: bool) -> io::Result<Vec<u8>> {
    let mut body = Vec::new();
    if compress {
        let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
        e.write_all(&data[..])?;

        let comp = e.finish()?;
        body.reserve(comp.len() + 25);
        spb::pack_value(&mut body, 2, data.len() as u64);
        spb::pack_data(&mut body, 3, &comp[..]);
    } else {
        body.reserve(data.len() + 5);
        spb::pack_data(&mut body, 1, &data[..]);
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
    let mut ct = Checktime::new();

    let pf = 100.0
        / (std::fs::metadata(fname)
            .expect(&format!("failed to open {}", fname))
            .len() as f64);
    let f = File::open(fname).expect("fail");
    let mut fbuf = BufReader::new(f);
    for (i, fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
        match ct.checktime() {
            Some(d) => {
                print!(
                    "\r{:8.3}s: {:6.1}% {:9.1}mb block {:10}",
                    d,
                    (fb.pos as f64) * pf,
                    (fb.pos as f64) / 1024.0 / 1024.0,
                    i
                );
                io::stdout().flush().expect("");
            }
            None => {}
        }
        pp.call((i, fb));
    }
    (pp.finish().expect("finish failed"), ct.gettime())
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
    let ct = Checktime::new();

    let pf = (end_percent-start_percent) / (flen as f64);

    for (i, fb) in ReadFileBlocks::new_at_start(fobj).enumerate() {
        pb.progress_percent((fb.pos as f64) * pf + start_percent);

        pp.call((i, fb));
    }
    pb.progress_percent(end_percent);
    let r = pp.finish().expect("finish failed");

    
    (r, ct.gettime())
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
    let ct = Checktime::new();

    for (i, fb) in ReadFileBlocks::new_at_start(fobj).enumerate() {
        
        pg.progress_bytes(fb.pos);
        pp.call((i, fb));
    }
    
    pg.finish();
    (pp.finish().expect("finish failed"), ct.gettime())
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
    let ct = Checktime::new();
    for (i, fb) in ReadFileBlocks::new_at_start_with_stop(fobj, stop_at).enumerate() {
        pb.progress_bytes(fb.pos);
        pp.call((i, fb));
    }
    pb.finish();
    (pp.finish().expect("finish failed"), ct.gettime())
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
/*
pub struct ProgBarWrap {
    start: u64,
    end: u64,
    asb: bool,
    pb: ProgressBar,
}

impl ProgBarWrap {
    pub fn new(total: u64) -> ProgBarWrap {
        let pb = ProgressBar::new(total);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:100.cyan/blue}] {percent:>4}% ({eta_precise}) {msg}")
            .progress_chars("#>-"));

        ProgBarWrap {
            start: 0,
            end: 0,
            pb: pb,
            asb: false,
        }
    }

    pub fn new_filebytes(filelen: u64) -> ProgBarWrap {
        let pb = ProgressBar::new(filelen);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:100.cyan/blue}] {bytes} / {total_bytes} ({eta_precise}) {msg}")
            .progress_chars("#>-"));

        ProgBarWrap {
            start: 0,
            end: 0,
            pb: pb,
            asb: true,
        }
    }

    pub fn set_range(&mut self, x: u64) {
        self.start = self.end;
        self.end = self.start + x;
    }
    pub fn set_message(&self, msg: &str) {
        self.pb.set_message(msg);
    }
    pub fn prog(&self, val: f64) {
        if self.asb {
            self.pb.set_position(val as u64);
        } else {
            let v = ((self.end - self.start) as f64) * val / 100.0;
            self.pb.set_position(v as u64 + self.start);
        }
    }
    pub fn finish(&self) {
        self.pb.finish();
    }
}
*/
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
    let ct = Checktime::new();

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
    (pp.finish().expect("finish failed"), ct.gettime())
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
    let ct = Checktime::new();

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
    (pp.finish().expect("finish failed"), ct.gettime())
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
