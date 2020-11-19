use super::read_pbf;
use super::quadtree;

use std::io::{Result,Error,ErrorKind};

#[derive(Debug)]
pub struct IndexItem {
    pub quadtree: quadtree::Tuple,
    pub is_change: bool,
    pub location: u64,
    pub length: u64,
}


impl IndexItem {
    pub fn new() -> IndexItem {
        IndexItem{quadtree:quadtree::Tuple::new(0,0,0), is_change: false, location: 0, length: 0}
    }
    
    pub fn read(npos: u64, data: &[u8]) -> Result<IndexItem> {
        let mut res = IndexItem::new();
        res.location = npos;
        for x in read_pbf::read_all_tags(&data, 0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => res.quadtree = quadtree::Tuple::read(&d)?,
                read_pbf::PbfTag::Value(2, isc) => res.is_change = isc!=0,
                read_pbf::PbfTag::Value(3, l) => res.length = l,
                read_pbf::PbfTag::Value(4, qt)=> res.quadtree = quadtree::Tuple::from_integer(read_pbf::unzigzag(qt))?,
                _ => return Err(Error::new(ErrorKind::Other,format!("IndexItem unexpected item: {:?}",x))),
            }
        }
        Ok(res)
    }
}

#[derive(Debug)]
pub struct HeaderBlock {
    pub bbox: Vec<i64>,
    pub writer: String,
    pub features: Vec<String>,
    pub index: Vec<IndexItem>,
}

fn read_header_bbox(data: &[u8]) -> Result<Vec<i64>> {
    let mut bbox = Vec::new();
    bbox.resize(4,0);
    let xx = read_pbf::read_all_tags(&data, 0);
    for x in xx {
        match x {
            read_pbf::PbfTag::Value(1, minlon) => bbox[0] = read_pbf::unzigzag(minlon)/1000, //left
            read_pbf::PbfTag::Value(2, minlat) => bbox[2] = read_pbf::unzigzag(minlat)/1000, //right
            read_pbf::PbfTag::Value(3, maxlon) => bbox[3] = read_pbf::unzigzag(maxlon)/1000, //top
            read_pbf::PbfTag::Value(4, maxlat) => bbox[1] = read_pbf::unzigzag(maxlat)/1000, //bottom
            _ => return Err(Error::new(ErrorKind::Other,"unexpected item")),
        }
    }
    
    Ok(bbox)
}
    
impl HeaderBlock {
    pub fn new() -> HeaderBlock {
        HeaderBlock{bbox: Vec::new(), writer: String::new(), features: Vec::new(), index: Vec::new()}
    }
    
    pub fn read(filepos: u64, data: &[u8]) -> Result<HeaderBlock> {
        let mut npos = filepos;
        let mut res = HeaderBlock::new();
        let xx = read_pbf::read_all_tags(&data, 0);
        for x in xx {
            match x {
                read_pbf::PbfTag::Data(1, d) => res.bbox = read_header_bbox(&d)?,
                read_pbf::PbfTag::Data(4, d) => {
                    let f = std::str::from_utf8(d).unwrap().to_string();
                    res.features.push(f);
                },
                read_pbf::PbfTag::Data(16,d) => res.writer = std::str::from_utf8(d).unwrap().to_string(),
                read_pbf::PbfTag::Data(22,d) => {
                    let i = IndexItem::read(npos, &d)?;
                    npos += i.length;
                    res.index.push(i);
                },
                _ => println!("?? {:?}", x),
            }
        }
        Ok(res)
    }
}
                    
                
                
    
