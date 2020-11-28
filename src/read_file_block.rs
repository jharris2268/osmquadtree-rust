use std::fs::File;
use std::io;
use std::io::{BufReader,Read,Write,ErrorKind};

use std::io::Seek;
use std::io::SeekFrom;
//use std::collections::HashMap;

extern crate flate2;
use self::flate2::read::ZlibDecoder;
use self::flate2::write::ZlibEncoder;


use super::read_pbf;
use super::write_pbf;
use super::callback::CallFinish;
use super::utils::{Checktime};
use indicatif::{ProgressBar, ProgressStyle};

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
        FileBlock{pos: 0, len: 0, block_type: String::new(), data_raw:Vec::new(), is_comp:false}
    }
    
    pub fn data(&self) -> Vec<u8> {
        if !self.is_comp { return self.data_raw.clone(); }
        
        let mut comp = Vec::new();
        let mut xx = ZlibDecoder::new(&self.data_raw[..]);
        xx.read_to_end(&mut comp).unwrap();
        comp
    }
    
}


pub fn file_position<F: Seek>(file: &mut F) -> io::Result<u64> {
    file.seek(SeekFrom::Current(0))
}
pub fn read_file_block<F: Seek+Read>(file: &mut F) -> io::Result<FileBlock> {
    let pos = file_position(file)?;
    let (_,y) = read_file_block_with_pos(file, pos)?;
    Ok(y)
}


pub fn read_file_block_with_pos<F: Read>(file: &mut F, mut pos: u64) -> io::Result<(u64,FileBlock)> {

    let mut fb = FileBlock::new();
    fb.pos = pos;
        
    let a = match read_file_data(file, 4) {
        Ok(data) => data,
        Err(err) => return Err(err),
    };
        
    pos += 4;
        
    let (l, _) = read_pbf::read_uint32(&a, 0)?;
    
    let b = read_file_data(file, l).unwrap();
    pos += l;
    
    let bb = read_pbf::IterTags::new(&b, 0);
    //println!("{:?}", bb);
    let mut ln = 0;
    for tg in bb {
        match tg {
            read_pbf::PbfTag::Value(3, v) => ln = v,
            read_pbf::PbfTag::Data(1, d) => fb.block_type = std::str::from_utf8(&d).unwrap().to_string(),
            _ => println!("?? {:?}", tg),
        }
    }
    fb.len = 4+l+ln;
    
    let c = read_file_data(file, ln).unwrap();
    pos += ln;
    
    for tg in read_pbf::IterTags::new(&c, 0) {
        match tg {
            read_pbf::PbfTag::Data(1, d) => fb.data_raw = d.to_vec(),
            read_pbf::PbfTag::Value(2, _) => fb.is_comp = true,
            read_pbf::PbfTag::Data(3, d) => fb.data_raw = d.to_vec(),
            _ => println!("?? {:?}", tg),
        }
    }
    
    Ok((pos,fb))
    
}

pub fn unpack_file_block(pos: u64, data: &[u8]) -> io::Result<FileBlock> {
    
    let s = read_file_block_with_pos(&mut data.as_ref(),pos)?;
    Ok(s.1)

}

pub fn pack_file_block(blockname: &str, data: &[u8], compress: bool) -> io::Result<Vec<u8>> {
    
    let mut body = Vec::new();
    if compress {
        let mut e = ZlibEncoder::new(Vec::new(), flate2::Compression::new(6));
        e.write_all(&data[..])?;
        
        let comp = e.finish()?;
        body.reserve(comp.len()+25);
        write_pbf::pack_value(&mut body, 2, data.len() as u64);
        write_pbf::pack_data(&mut body, 3, &comp[..]);
    } else {
        body.reserve(data.len()+5);
        write_pbf::pack_data(&mut body, 1, &data[..]);
    }
    
    let mut head = Vec::with_capacity(25);
    write_pbf::pack_data(&mut head, 1, blockname.as_bytes());
    write_pbf::pack_value(&mut head, 3, body.len() as u64);
    
    let mut result = Vec::with_capacity(4+head.len()+body.len());
    write_pbf::write_uint32(&mut result, head.len() as u32);
    result.extend(head);
    result.extend(body);
    
    Ok(result)
}



pub struct ReadFileBlocks<'a, R: Read> {
    file: &'a mut R,
    p: u64
}

impl<R> ReadFileBlocks<'_, R>
    where R: Read+Seek
{
    pub fn new(file: &mut R) -> ReadFileBlocks<R> {
        let p = file_position(file).expect("!");
        ReadFileBlocks{file, p}
    }
}



impl<R> Iterator for ReadFileBlocks<'_, R>
    where R: Read
{
    type Item = FileBlock;
    
    fn next(&mut self) -> Option<Self::Item> {
        
                
        match read_file_block_with_pos(self.file,self.p) {
            Ok((p,fb)) => { self.p = p; Some(fb) },
            Err(err) => {
                match err.kind() {
                    ErrorKind::UnexpectedEof => {
                        //at end of file
                    },
                    _ => {
                        println!("failed to read {}", err);
                    }
                }
                None
            },
            
        }
    }
}




pub fn read_all_blocks<T,U>(fname: &str, mut pp: Box<T>) -> (U, f64)
    where   T: CallFinish<CallType=(usize,FileBlock), ReturnType=U>,
            U: Send+Sync+'static
{
    let mut ct=Checktime::new();
    
    let pf = 100.0 / (std::fs::metadata(fname).expect(&format!("failed to open {}", fname)).len() as f64);
    let f = File::open(fname).expect("fail");
    let mut fbuf = BufReader::new(f);
    for (i,fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
        match ct.checktime() {
            Some(d) => {
                print!("\r{:8.3}s: {:6.1}% {:9.1}mb block {:10}", d, (fb.pos as f64)*pf, (fb.pos as f64)/1024.0/1024.0, i);
                io::stdout().flush().expect("");
            },
            None => {}
        }
        pp.call((i,fb));
    }
    (pp.finish().expect("finish failed"), ct.gettime())
}      

pub fn read_all_blocks_prog<T,U>(fname: &str, mut pp: Box<T>, prog: &ProgBarWrap) -> (U, f64)
    where   T: CallFinish<CallType=(usize,FileBlock), ReturnType=U>,
            U: Send+Sync+'static
{
    let ct=Checktime::new();
    
    let pf = 100.0 / (std::fs::metadata(fname).expect(&format!("failed to open {}", fname)).len() as f64);
    let f = File::open(fname).expect("fail");
    let mut fbuf = BufReader::new(f);
    for (i,fb) in ReadFileBlocks::new(&mut fbuf).enumerate() {
        /*match ct.checktime() {
            Some(d) => {
                print!("\r{:8.3}s: {:6.1}% {:9.1}mb block {:10}", d, (fb.pos as f64)*pf, (fb.pos as f64)/1024.0/1024.0, i);
                io::stdout().flush().expect("");
            },
            None => {}
        }*/
        prog.prog((fb.pos as f64)*pf);
        pp.call((i,fb));
        
    }
    (pp.finish().expect("finish failed"), ct.gettime())
}    

pub fn read_all_blocks_locs<R,T,U>(fobj: &mut R, fname: &str, locs: Vec<u64>, print_msgs: bool, mut pp: Box<T>) -> (U, f64)
    where   T: CallFinish<CallType=(usize,FileBlock), ReturnType=U>,
            U: Send+Sync+'static,
            R: Read+Seek
{
    let mut ct=Checktime::new();
    let pf = 100.0 / (locs.len() as f64);
    for (i,l) in locs.iter().enumerate() {
        fobj.seek(SeekFrom::Start(*l)).expect(&format!("failed to read {} @ {}", fname, *l));
        let (_,fb) = read_file_block_with_pos(fobj, *l).expect(&format!("failed to read {} @ {}", fname, *l));
        if print_msgs {
            match ct.checktime() {
                Some(d) => {
                    print!("\r{:8.3}s: {:6.1}% {:9.1}mb block {:10}", d, (i as f64)*pf, (fb.pos as f64)/1024.0/1024.0, i);
                    io::stdout().flush().expect("");
                },
                None => {}
            }
        }
        pp.call((i,fb));
    }
    (pp.finish().expect("finish failed"), ct.gettime())
}


pub struct ProgBarWrap {
    start: u64,
    end: u64,
    
    pb: ProgressBar
}

impl ProgBarWrap {
    pub fn new(total: u64) -> ProgBarWrap {
        let pb = ProgressBar::new(total);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {percent:4.1%} ({eta_precise}) {msg}")
            .progress_chars("#>-"));
            
        ProgBarWrap{start:0, end: 0, pb: pb}
    }
    
    pub fn set_range(&mut self, x: u64) {
        self.start=self.end;
        self.end = self.start+x;
    }
    pub fn set_message(&self, msg: &str) {
        self.pb.set_message(msg);
    }
    pub fn prog(&self, val: f64) {
        let v= ((self.end-self.start) as f64)*val/100.0;
        self.pb.set_position(v as u64 + self.start);
    }
    pub fn finish(&self) {
        self.pb.finish();
    }
    
}
    



pub fn read_all_blocks_locs_prog<R,T,U>(fobj: &mut R, fname: &str, locs: Vec<u64>, mut pp: Box<T>, pb: &ProgBarWrap) -> (U, f64)
    where   T: CallFinish<CallType=(usize,FileBlock), ReturnType=U>,
            U: Send+Sync+'static,
            R: Read+Seek
{
    let ct=Checktime::new();
    
    let pf = 100.0 / (locs.len() as f64);
    
    
    for (i,l) in locs.iter().enumerate() {
        
        fobj.seek(SeekFrom::Start(*l)).expect(&format!("failed to read {} @ {}", fname, *l));
        let (_,fb) = read_file_block_with_pos(fobj, *l).expect(&format!("failed to read {} @ {}", fname, *l));
        
        pb.prog((i as f64) * pf);
        
        pp.call((i,fb));
    }
    (pp.finish().expect("finish failed"), ct.gettime())
}


pub fn read_all_blocks_parallel<T,U,F>(mut fbufs: Vec<F>, locs: Vec<(usize,Vec<(usize,u64)>)>,mut pp: Box<T>) -> (U, f64)
    where   T: CallFinish<CallType=(usize,Vec<FileBlock>), ReturnType=U>,
            U: Send+Sync+'static,
            F: Seek+Read
{
    let mut ct=Checktime::new();
    
    let mut fposes = Vec::new();
    for f in fbufs.iter_mut() {
        fposes.push(file_position(f).expect("!"));
    }
    let pf = 100.0 / (locs.len() as f64);
    for (j,(i,ll)) in locs.iter().enumerate() {
        let mut fbs = Vec::new();
        for (a,b) in ll {
            if fposes[*a]!=*b {
                fbufs[*a].seek(SeekFrom::Start(*b)).expect(&format!("failed to read {} @ {}", *a,*b));
            }
            
            let (x,y) = read_file_block_with_pos(&mut fbufs[*a],*b).expect(&format!("failed to read {} @ {}", *a,*b));
            
            fbs.push(y);
            fposes[*a]=x;
        }
        
        match ct.checktime() {
            Some(d) => {
                print!("\r{:8.3}s: {:6.1}% block {:10}", d, (j as f64)*pf, i);
                io::stdout().flush().expect("");
            },
            None => {}
        }
        pp.call((*i,fbs));
    }
    (pp.finish().expect("finish failed"), ct.gettime())
}      
