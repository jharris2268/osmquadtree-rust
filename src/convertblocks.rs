

use super::elements::{PrimitiveBlock,apply_change_primitive, combine_block_primitive,IdSet};
use super::elements::{MinimalBlock,apply_change_minimal, combine_block_minimal};
use super::read_file_block::FileBlock;
use super::utils::{CallAll,Timings,ThreadTimer};
use super::callback::CallFinish;
use std::io::Result;
use std::sync::Arc;
use std::marker::PhantomData;

pub fn read_primitive_blocks_combine(idx: i64, mut blocks: Vec<FileBlock>, ids: Option<&IdSet>) -> Result<PrimitiveBlock> {

    
    if blocks.is_empty() {
        return Ok(PrimitiveBlock::new(idx,0));
    }
    
    if blocks.len() == 1 {
        let fb = blocks.pop().unwrap();
        return PrimitiveBlock::read_check_ids(idx, fb.pos, &fb.data(), false, false, ids)
    }
    
    let pos = blocks[0].pos;
    
    let fb = blocks.pop().unwrap();
    let mut curr = PrimitiveBlock::read_check_ids(idx, pos, &fb.data(), true, false, ids)?;
    
    while blocks.len()>1 {
        let fb = blocks.pop().unwrap();
        let nb = PrimitiveBlock::read_check_ids(idx, pos, &fb.data(), true, false, ids)?;
        curr = combine_block_primitive(nb, curr);
        
    }
    
    let fb = blocks.pop().unwrap();
    let main = PrimitiveBlock::read_check_ids(idx, pos, &fb.data(), false, false,ids)?;
    
    Ok(apply_change_primitive(main, curr))
}

fn wrap_read_primitive_blocks_combine(idx_blocks: (usize, Vec<FileBlock>)) -> PrimitiveBlock {
    read_primitive_blocks_combine(idx_blocks.0 as i64, idx_blocks.1, None).expect("failed to read data")
        
}


pub fn make_read_primitive_blocks_combine_call_all<V: Sync+Send+'static, O: CallFinish<CallType=PrimitiveBlock,ReturnType=Timings<V>>>(out: Box<O>) -> Box<impl CallFinish<CallType=(usize, Vec<FileBlock>), ReturnType=Timings<V>>> {
    Box::new(CallAll::new(out, "read_primitive_blocks_combine", Box::new(wrap_read_primitive_blocks_combine)))
}


struct Rpbccai<O,V> {
    out: Box<O>,
    ids: Arc<IdSet>,
    x: PhantomData<V>,
    tm: f64
}
impl<O,V> Rpbccai<O,V> {
    pub fn new(out: Box<O>, ids: Arc<IdSet>) -> Rpbccai<O,V> {
        Rpbccai{out:out,ids:ids,x:PhantomData,tm:0.0}
    }
}


impl<O,V> CallFinish for Rpbccai<O,V>
    where   O: CallFinish<CallType=PrimitiveBlock,ReturnType=Timings<V>>,
            V: Sync+Send+'static
{
    type CallType = (usize, Vec<FileBlock>);
    type ReturnType = Timings<V>;
    
    fn call(&mut self, idx_blocks: (usize, Vec<FileBlock>)) {
        let tx=ThreadTimer::new();
        let b = read_primitive_blocks_combine(idx_blocks.0 as i64, idx_blocks.1, Some(self.ids.as_ref())).expect("?");
        //println!("block {} {} nodes, {} ways, {} relations", b.quadtree.as_string(),b.nodes.len(),b.ways.len(),b.relations.len());
        self.tm+=tx.since();
        self.out.call(b);
    }
    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut tm = self.out.finish()?;
        tm.add("read_primitive_blocks_combine", self.tm);
        Ok(tm)
    }
}



pub fn make_read_primitive_blocks_combine_call_all_idset<V: Sync+Send+'static, O: CallFinish<CallType=PrimitiveBlock,ReturnType=Timings<V>>>(out: Box<O>, idset: Arc<IdSet>) -> Box<impl CallFinish<CallType=(usize, Vec<FileBlock>), ReturnType=Timings<V>>> {
    //Box::new(CallAll::new(out, "read_primitive_blocks_combine_idset", Box::new(move |pp| { wrap_read_primitive_blocks_combine(pp, Some(idset)) })))
    Box::new(Rpbccai::new(out,idset))
}



    

pub fn read_minimal_blocks_combine(idx: i64, mut blocks: Vec<FileBlock>) -> Result<MinimalBlock> {
    
    if blocks.is_empty() {
        return Ok(MinimalBlock::new());
    }
    
    if blocks.len() == 1 {
        let fb = blocks.pop().unwrap();
        return MinimalBlock::read(idx, fb.pos, &fb.data(), false)
    }
    
    let pos = blocks[0].pos;
    
    let fb = blocks.pop().unwrap();
    let mut curr = MinimalBlock::read(idx, pos, &fb.data(), true)?;
    
    while blocks.len()>1 {
        let fb = blocks.pop().unwrap();
        let nb = MinimalBlock::read(idx, pos, &fb.data(), true)?;
        curr = combine_block_minimal(nb, curr);
        
    }
    
    let fb = blocks.pop().unwrap();
    let main = MinimalBlock::read(idx, pos, &fb.data(), false)?;
    
    Ok(apply_change_minimal(main, curr))
}

fn wrap_read_minimal_blocks_combine(idx_blocks: (usize, Vec<FileBlock>)) -> MinimalBlock {
    
    read_minimal_blocks_combine(idx_blocks.0 as i64, idx_blocks.1).expect("failed to read data")
}


pub fn make_read_minimal_blocks_combine_call_all<V: Sync+Send+'static, O: CallFinish<CallType=MinimalBlock,ReturnType=Timings<V>>>(out: Box<O>) -> Box<impl CallFinish<CallType=(usize, Vec<FileBlock>), ReturnType=Timings<V>>> {
    Box::new(CallAll::new(out, "read_minimal_blocks_combine", Box::new(wrap_read_minimal_blocks_combine)))
}


        

pub fn make_convert_minimal_block<T: CallFinish<CallType=MinimalBlock,ReturnType=Timings<U>>,U: Sync+Send+'static>(ischange: bool, out: Box<T>) -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=Timings<U>>> {
    let convert_minimal = move |(i,fb): (usize, FileBlock)| -> MinimalBlock {
        if fb.block_type == "OSMData" {
            MinimalBlock::read(i as i64, fb.pos, &fb.data(),ischange).expect("?")
        } else {
            MinimalBlock::new()
        }
    };
    
    Box::new(CallAll::new(out, "convert minimal", Box::new(convert_minimal)))
    
}

pub fn make_convert_primitive_block<T: CallFinish<CallType=PrimitiveBlock,ReturnType=Timings<U>>,U: Sync+Send+'static>(ischange: bool, out: Box<T>) -> Box<impl CallFinish<CallType=(usize,FileBlock),ReturnType=Timings<U>>> {
    let convert_minimal = move |(i,fb): (usize, FileBlock)| -> PrimitiveBlock {
        if fb.block_type == "OSMData" {
            PrimitiveBlock::read(i as i64, fb.pos, &fb.data(),ischange,false).expect("?")
        } else {
            PrimitiveBlock::new(0,0)
        }
    };
    
    Box::new(CallAll::new(out, "convert primitive", Box::new(convert_minimal)))
    
}
    
    
    
    
    
    
    
    
