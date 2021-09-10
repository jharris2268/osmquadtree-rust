use crate::pbfformat::read_file_block::ReadFileBlocksOwn;
use crate::elements::{Element,PrimitiveBlock};

use std::io::{Result,Error,ErrorKind};

pub fn iter_elements_flat(fname: &str, numchan: usize) -> Result<Box<dyn Iterator<Item = Element>>> {
    
    Ok(Box::new(iter_primitiveblocks(fname, numchan)?.flat_map(|bl| { bl.into_iter() })))
}

pub fn iter_primitiveblocks(fname: &str, numchan: usize) -> Result<Box<dyn Iterator<Item = PrimitiveBlock>>> {
    
    if numchan != 0 {
        return Err(Error::new(ErrorKind::Other, "not implemented"))
    }
    
    Ok(Box::new(ReadFileBlocksOwn::new(fname)?.enumerate().map( |(i,fb)| { 
        if fb.block_type == "OSMData" {
            PrimitiveBlock::read(i as i64, fb.pos, &fb.data(), false, false).expect("?")
        } else {
            PrimitiveBlock::new(i as i64, 0)
        }
    })))
}

    
