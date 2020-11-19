use std::fs::File;
use std::io;
use std::io::{BufReader,Read,Write};

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

pub fn read_file_data(file: &mut BufReader<File>, nbytes: u64) -> io::Result<Vec<u8>> {
    
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


pub fn file_position(file: &mut BufReader<File>) -> io::Result<u64> {
    file.seek(SeekFrom::Current(0))
}

pub fn read_file_block(file: &mut BufReader<File>) -> io::Result<FileBlock> {
    
    //let mut fb = Box::new(FileBlock::new());
    let mut fb = FileBlock::new();
    
    fb.pos = file_position(file)?;
    
        
    let a = match read_file_data(file, 4) {
        Ok(data) => data,
        Err(err) => return Err(err),
    };
        
        
        
    let (l, _) = read_pbf::read_uint32(&a, 0)?;
    
    let b = read_file_data(file, l).unwrap();
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
    //let cc = read_pbf::read_all_tags(&c, 0);
    
    //println!("{:?}", cc);   
    //let mut comp = Vec::new();
    //let mut comp_len = 0;
    //for tg in cc {
    for tg in read_pbf::IterTags::new(&c, 0) {
        match tg {
            read_pbf::PbfTag::Data(1, d) => fb.data_raw = d.to_vec(),
            read_pbf::PbfTag::Value(2, _) => fb.is_comp = true,
            read_pbf::PbfTag::Data(3, d) => fb.data_raw = d.to_vec(),
            _ => println!("?? {:?}", tg),
        }
    }
    /*drop(a);
    drop(b);
    drop(c);*/
    
    Ok(fb)
    
}

pub fn unpack_file_block(pos: u64, data: &[u8]) -> io::Result<FileBlock> {
    
    
    let mut fb = FileBlock::new();
    
    fb.pos = pos;
    
        
    let a = &data[0..4];
        
    let (l, _) = read_pbf::read_uint32(a, 0)?;
    
    
    let b = &data[4..4+l as usize];
    
    let bb = read_pbf::IterTags::new(b, 0);
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
    
    let c = &data[4+l as usize .. (4+l+ln) as usize];
    for tg in read_pbf::IterTags::new(c, 0) {
        match tg {
            read_pbf::PbfTag::Data(1, d) => fb.data_raw = d.to_vec(),
            read_pbf::PbfTag::Value(2, _) => fb.is_comp = true,
            read_pbf::PbfTag::Data(3, d) => fb.data_raw = d.to_vec(),
            _ => println!("?? {:?}", tg),
        }
    }
    /*drop(a);
    drop(b);
    drop(c);*/
    
    Ok(fb)
    
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



pub struct ReadFileBlocks<'a> {
    file: &'a mut BufReader<File>
}

impl ReadFileBlocks<'_> {
    pub fn new(file: &mut BufReader<File>) -> ReadFileBlocks {
        ReadFileBlocks{file}
    }
}



impl Iterator for ReadFileBlocks<'_> {
    type Item = FileBlock;
    
    fn next(&mut self) -> Option<Self::Item> {
        
                
        match read_file_block(self.file) {
            Ok(fb) => { Some(fb) }
            Err(err) => {
                println!("failed to read {}", err);
                None
            }
        }
    }
}




pub fn read_all_blocks<T,U>(fname: &str, mut pp: Box<T>) -> (U, f64)
    where   T: CallFinish<CallType=(usize,FileBlock), ReturnType=U>,
            U: Send+Sync+'static
{
    let mut ct=Checktime::new();
    
    let pf = 100.0 / (std::fs::metadata(fname).expect("fail").len() as f64);
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
    

