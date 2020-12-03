use super::read_pbf;
use super::write_pbf;
use super::elements::Quadtree;

use std::fs::File;
use std::io::{Result,Error,ErrorKind};
use serde_json;


#[derive(Debug)]
pub struct IndexItem {
    pub quadtree: Quadtree,
    pub is_change: bool,
    pub location: u64,
    pub length: u64,
}


impl IndexItem {
    pub fn new(quadtree: Quadtree, is_change: bool, location: u64, length: u64) -> IndexItem {
        IndexItem{quadtree, is_change, location, length}
    }
    
    pub fn read(npos: u64, data: &[u8]) -> Result<IndexItem> {
        let mut quadtree=Quadtree::empty();
        let mut is_change=false;
        let mut length=0;
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => quadtree = Quadtree::read(&d)?,
                read_pbf::PbfTag::Value(2, isc) => is_change = isc!=0,
                read_pbf::PbfTag::Value(3, l) => length = read_pbf::un_zig_zag(l) as u64,
                read_pbf::PbfTag::Value(4, qt)=> quadtree = Quadtree::new(read_pbf::un_zig_zag(qt)),
                _ => return Err(Error::new(ErrorKind::Other,format!("IndexItem unexpected item: {:?}",x))),
            }
        }
        Ok(IndexItem::new(quadtree,is_change,npos,length))
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
    
    for x in read_pbf::IterTags::new(&data, 0) {
        match x {
            read_pbf::PbfTag::Value(1, minlon) => bbox[0] = read_pbf::un_zig_zag(minlon)/1000, //left
            read_pbf::PbfTag::Value(2, minlat) => bbox[2] = read_pbf::un_zig_zag(minlat)/1000, //right
            read_pbf::PbfTag::Value(3, maxlon) => bbox[3] = read_pbf::un_zig_zag(maxlon)/1000, //top
            read_pbf::PbfTag::Value(4, maxlat) => bbox[1] = read_pbf::un_zig_zag(maxlat)/1000, //bottom
            _ => return Err(Error::new(ErrorKind::Other,"unexpected item")),
        }
    }
    
    Ok(bbox)
}
    

    
impl HeaderBlock {
    pub fn new() -> HeaderBlock {
        HeaderBlock{bbox: Vec::new(), writer: String::new(), features: Vec::new(), index: Vec::new()}
    }
    
    pub fn read(filepos: u64, data: &[u8], fname: &str) -> Result<HeaderBlock> {
        let mut npos = filepos;
        let mut res = HeaderBlock::new();
        
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => res.bbox = read_header_bbox(&d)?,
                read_pbf::PbfTag::Data(4, d) => {
                    let f = std::str::from_utf8(d).unwrap().to_string();
                    res.features.push(f);
                },
                read_pbf::PbfTag::Data(16,d) => res.writer = std::str::from_utf8(d).expect("!").to_string(),
                read_pbf::PbfTag::Data(22,d) => {
                    let i = IndexItem::read(npos, &d)?;
                    npos += i.length;
                    res.index.push(i);
                },
                read_pbf::PbfTag::Data(23,d) => {
                    res.read_file_locs(&format!("{}{}", fname, std::str::from_utf8(d).expect("!")))?;
                },
                _ => println!("?? {:?}", x),
            }
        }
        Ok(res)
    }
    
    fn read_file_locs(&mut self, filelocs_fn: &str) -> Result<()> {
        let fl = File::open(filelocs_fn)?;
        let ff: Vec<(i64,u64,u64)>;
        
        match serde_json::from_reader(&fl) {
            Ok(r) => { ff = r; },
            Err(e) => {return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));}
        }
        for (a,b,c) in ff {
            self.index.push(IndexItem::new(Quadtree::new(a),false,b,c));
        }
        Ok(())
    }
    
}


fn pack_bbox()->Vec<u8> {
    let mut res=Vec::new();
    write_pbf::pack_value(&mut res, 1, write_pbf::zig_zag(-180000000000));
    write_pbf::pack_value(&mut res, 2, write_pbf::zig_zag(180000000000));
    write_pbf::pack_value(&mut res, 3, write_pbf::zig_zag(90000000000));
    write_pbf::pack_value(&mut res, 4, write_pbf::zig_zag(-90000000000));
    res
}

pub fn make_header_block(withlocs: bool) -> Vec<u8> {
    let mut res=Vec::new();
    
    write_pbf::pack_data(&mut res, 1, &pack_bbox());
    write_pbf::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    write_pbf::pack_data(&mut res, 4, b"DenseNodes");
    write_pbf::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    if withlocs {
        write_pbf::pack_data(&mut res, 23, b"-filelocs.json");
    }
    
    res
}

fn pack_index_item(q: &Quadtree, _ischange: bool, l: u64) -> Vec<u8> {
    let mut res=Vec::with_capacity(25);
    //if ischange { write_pbf::pack_value(&mut res, 2, 1); }
    write_pbf::pack_value(&mut res, 2, 0);
    write_pbf::pack_value(&mut res, 3, write_pbf::zig_zag(l as i64));
    write_pbf::pack_value(&mut res, 4, write_pbf::zig_zag(q.as_int()));
    res
}

pub fn make_header_block_stored_locs(ischange: bool, locs: Vec<(Quadtree,u64)>) -> Vec<u8> {
    let mut res=Vec::new();
    
    write_pbf::pack_data(&mut res, 1, &pack_bbox());
    write_pbf::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    write_pbf::pack_data(&mut res, 4, b"DenseNodes");
    write_pbf::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    for (a,b) in &locs {
        write_pbf::pack_data(&mut res, 22, &pack_index_item(a,ischange,*b));
    }
    
    res
}

pub enum HeaderType {
    None,
    NoLocs,
    InternalLocs,
    ExternalLocs
}

                
                
    
 
