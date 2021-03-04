use crate::elements::{Bbox, Quadtree};

use simple_protocolbuffers as spb;

use serde_json;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Result};

#[derive(Debug)]
pub struct IndexItem {
    pub quadtree: Quadtree,
    pub is_change: bool,
    pub location: u64,
    pub length: u64,
}

impl IndexItem {
    pub fn new(quadtree: Quadtree, is_change: bool, location: u64, length: u64) -> IndexItem {
        IndexItem {
            quadtree,
            is_change,
            location,
            length,
        }
    }

    pub fn read(npos: u64, data: &[u8]) -> Result<IndexItem> {
        let mut quadtree = Quadtree::empty();
        let mut is_change = false;
        let mut length = 0;
        for x in spb::IterTags::new(&data) {
            match x {
                spb::PbfTag::Data(1, d) => quadtree = Quadtree::read(&d)?,
                spb::PbfTag::Value(2, isc) => is_change = isc != 0,
                spb::PbfTag::Value(3, l) => length = spb::un_zig_zag(l) as u64,
                spb::PbfTag::Value(4, qt) => quadtree = Quadtree::new(spb::un_zig_zag(qt)),
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("IndexItem unexpected item: {:?}", x),
                    ))
                }
            }
        }
        Ok(IndexItem::new(quadtree, is_change, npos, length))
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
    bbox.resize(4, 0);

    for x in spb::IterTags::new(&data) {
        match x {
            spb::PbfTag::Value(1, minlon) => bbox[0] = spb::un_zig_zag(minlon) / 1000, //left
            spb::PbfTag::Value(2, minlat) => bbox[2] = spb::un_zig_zag(minlat) / 1000, //right
            spb::PbfTag::Value(3, maxlon) => bbox[3] = spb::un_zig_zag(maxlon) / 1000, //top
            spb::PbfTag::Value(4, maxlat) => bbox[1] = spb::un_zig_zag(maxlat) / 1000, //bottom
            _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
        }
    }

    Ok(bbox)
}

impl HeaderBlock {
    pub fn new() -> HeaderBlock {
        HeaderBlock {
            bbox: Vec::new(),
            writer: String::new(),
            features: Vec::new(),
            index: Vec::new(),
        }
    }

    pub fn read(filepos: u64, data: &[u8], fname: &str) -> Result<HeaderBlock> {
        let mut npos = filepos;
        let mut res = HeaderBlock::new();

        for x in spb::IterTags::new(&data) {
            match x {
                spb::PbfTag::Data(1, d) => res.bbox = read_header_bbox(&d)?,
                spb::PbfTag::Data(4, d) => {
                    let f = std::str::from_utf8(d).unwrap().to_string();
                    res.features.push(f);
                }
                spb::PbfTag::Data(16, d) => {
                    res.writer = std::str::from_utf8(d).expect("!").to_string()
                }
                spb::PbfTag::Data(22, d) => {
                    let i = IndexItem::read(npos, &d)?;
                    npos += i.length;
                    res.index.push(i);
                }
                spb::PbfTag::Data(23, d) => {
                    res.read_file_locs(&format!(
                        "{}{}",
                        fname,
                        std::str::from_utf8(d).expect("!")
                    ))?;
                }
                _ => println!("?? {:?}", x),
            }
        }
        Ok(res)
    }

    fn read_file_locs(&mut self, filelocs_fn: &str) -> Result<()> {
        let fl = File::open(filelocs_fn)?;
        let mut flb = BufReader::new(fl);
        let ff: Vec<(i64, u64, u64)>;

        match serde_json::from_reader(&mut flb) {
            Ok(r) => {
                ff = r;
            }
            Err(e) => {
                return Err(Error::new(ErrorKind::Other, format!("{:?}", e)));
            }
        }
        for (a, b, c) in ff {
            self.index
                .push(IndexItem::new(Quadtree::new(a), false, b, c));
        }

        Ok(())
    }
}

fn pack_bbox_planet() -> Vec<u8> {
    let mut res = Vec::new();
    spb::pack_value(&mut res, 1, spb::zig_zag(-180000000000));
    spb::pack_value(&mut res, 2, spb::zig_zag(180000000000));
    spb::pack_value(&mut res, 3, spb::zig_zag(90000000000));
    spb::pack_value(&mut res, 4, spb::zig_zag(-90000000000));
    res
}
fn pack_bbox(bbox: &Bbox) -> Vec<u8> {
    let mut res = Vec::new();
    spb::pack_value(&mut res, 1, spb::zig_zag((bbox.minlon as i64) * 100));
    spb::pack_value(&mut res, 2, spb::zig_zag((bbox.maxlon as i64) * 100));
    spb::pack_value(&mut res, 3, spb::zig_zag((bbox.maxlat as i64) * 100));
    spb::pack_value(&mut res, 4, spb::zig_zag((bbox.minlat as i64) * 100));
    res
}

pub fn make_header_block(withlocs: bool, bbox: Option<&Bbox>) -> Vec<u8> {
    let mut res = Vec::new();

    match bbox {
        Some(bbox) => {
            spb::pack_data(&mut res, 1, &pack_bbox(bbox));
        }
        None => {
            spb::pack_data(&mut res, 1, &pack_bbox_planet());
        }
    }
    spb::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    spb::pack_data(&mut res, 4, b"DenseNodes");
    spb::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    if withlocs {
        spb::pack_data(&mut res, 23, b"-filelocs.json");
    }

    res
}

fn pack_index_item(q: &Quadtree, _ischange: bool, l: u64) -> Vec<u8> {
    let mut res = Vec::with_capacity(25);
    //if ischange { spb::pack_value(&mut res, 2, 1); }
    spb::pack_value(&mut res, 2, 0);
    spb::pack_value(&mut res, 3, spb::zig_zag(l as i64));
    spb::pack_value(&mut res, 4, spb::zig_zag(q.as_int()));
    res
}

pub fn make_header_block_stored_locs(ischange: bool, locs: Vec<(Quadtree, u64)>) -> Vec<u8> {
    let mut res = Vec::new();

    spb::pack_data(&mut res, 1, &pack_bbox_planet());
    spb::pack_data(&mut res, 4, b"OsmSchema-V0.6");
    spb::pack_data(&mut res, 4, b"DenseNodes");
    spb::pack_data(&mut res, 16, b"osmquadtree-cpp"); //b"osmquadtree-rust"
    for (a, b) in &locs {
        spb::pack_data(&mut res, 22, &pack_index_item(a, ischange, *b));
    }

    res
}

pub enum HeaderType {
    None,
    NoLocs,
    InternalLocs,
    ExternalLocs,
}
