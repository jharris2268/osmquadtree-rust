use crate::pbfformat::read_file_block::ReadFileBlocksOwn;
use crate::pbfformat::{FileBlock,make_convert_primitive_block,/*file_length,*/read_all_blocks_with_progbar};
use crate::elements::{Element,PrimitiveBlock};
use channelled_callbacks::{Callback,CallbackMerge,CallbackSync,Timings,ReplaceNoneWithTimings,CallFinish,MergeTimings, ReverseCallback};
//use crate::{progress_bytes,logging::ProgressBytes};

use std::io::{Result};

pub fn iter_elements_flat(fname: &str, numchan: usize) -> Result<Box<dyn Iterator<Item = Element>>> {
    
    Ok(Box::new(iter_primitiveblocks(fname, numchan)?.flat_map(|bl| { bl.into_iter() })))
}
/*
struct ConvBlocksCollect {
    blocks: Vec<PrimitiveBlock>
}

impl ConvBlocksCollect {
    fn new() -> ConvBlocksCollect {
        ConvBlocksCollect{blocks: Vec::new()}
    }
}

impl CallFinish for ConvBlocksCollect {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings<Vec<PrimitiveBlock>>;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        self.blocks.push(bl);
    }
    fn finish(&mut self) -> Result<Timings<Vec<PrimitiveBlock>>> {
        let mut tm = Timings::new();
        tm.add_other("data",std::mem::take(&mut self.blocks));
        Ok(tm)
    }
}


struct ConvertPrimitiveBlocksLumps {
    
    ff: ReadFileBlocksOwn,
    numchan: usize,
    idx: usize,
    atend: bool,
    prog: Option<Box<dyn ProgressBytes>>
}

impl ConvertPrimitiveBlocksLumps {
    fn new(fname: &str, numchan: usize, report_progress: bool) -> Result<ConvertPrimitiveBlocksLumps> {
        let mut prog = None;
        if report_progress {
            let fl = file_length(fname);
            prog = Some(progress_bytes!(&format!("read from {}", fname), fl));
        }
        let ff = ReadFileBlocksOwn::new(fname)?;
        Ok(ConvertPrimitiveBlocksLumps{ff:ff,numchan:numchan,idx: 0,atend: false,prog: prog})
    }
}

impl Iterator for ConvertPrimitiveBlocksLumps {
    type Item = Vec<PrimitiveBlock>;
    
    fn next(&mut self) -> Option<Vec<PrimitiveBlock>> {
        if self.atend {
            match &self.prog {
                Some(p) => {p.finish();},
                None => {}
            } 
            return None;
        }
        let collect = CallbackSync::new(Box::new(ConvBlocksCollect::new()), self.numchan);
        let mut convs: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings<Vec<PrimitiveBlock>>>>> = Vec::new();
        for c in collect {
            let c2 = Box::new(ReplaceNoneWithTimings::new(c));
            convs.push(Box::new(Callback::new(make_convert_primitive_block(false, c2))))
        }
        
        let mut conv = Box::new(CallbackMerge::new(convs,Box::new(MergeTimings::new())));
        
        for _ in 0..self.numchan*50 {
            match self.ff.next() {
                Some(fb) => {
                    match &self.prog {
                        Some(p) => {p.progress_bytes(fb.pos);},
                        None => {}
                    }
                    conv.call((self.idx, fb));
                    self.idx+=1;
                },
                None => { self.atend = true; break;}
            }
        }
        
        let mut res = conv.finish().expect("??");
        
        Some(std::mem::take(&mut res.others[0].1))
    }
}*/


    
fn prep_read_all_primitive(fname: String, numchan: usize, cb: Box<dyn CallFinish<CallType=PrimitiveBlock, ReturnType=Timings<()>>>) -> Result<Timings<()>> {
    
    let cbs = CallbackSync::new(cb, numchan);
    let mut convs: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings<()>>>> = Vec::new();
    for c in cbs {
        let c2 = Box::new(ReplaceNoneWithTimings::new(c));
        convs.push(Box::new(Callback::new(make_convert_primitive_block(false, c2))))
    }
    
    let conv = Box::new(CallbackMerge::new(convs,Box::new(MergeTimings::new())));
    
    let (tm,_) = read_all_blocks_with_progbar(&fname, conv, &format!("read from {}",&fname) );
    Ok(tm)
}
    
    

pub fn iter_primitiveblocks(fname: &str, numchan: usize) -> Result<Box<dyn Iterator<Item = PrimitiveBlock>>> {
    
    if numchan != 0 {
        let n=String::from(fname);
        
        return Ok(Box::new(ReverseCallback::new(move |cb| prep_read_all_primitive(n,numchan,cb) )));
            
        
        //let cc = ConvertPrimitiveBlocksLumps::new(fname,numchan,false)?;
        //return Ok(Box::new(cc.into_iter().flatten()));
            
        //return Err(Error::new(ErrorKind::Other, "not implemented"))
    } 
    
    Ok(Box::new(ReadFileBlocksOwn::new(fname)?.enumerate().map( |(i,fb)| { 
        if fb.block_type == "OSMData" {
            PrimitiveBlock::read(i as i64, fb.pos, &fb.data(), false, false).expect("?")
        } else {
            PrimitiveBlock::new(i as i64, 0)
        }
    })))
}

    
