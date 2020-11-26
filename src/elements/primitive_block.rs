mod osmquadtree {
    pub use super::super::super::*;
}

use osmquadtree::read_pbf;
use osmquadtree::write_pbf;

use super::quadtree;

pub use super::node::Node;
pub use super::way::Way;
pub use super::relation::{Relation,ElementType,Member};
pub use super::tags::Tag;
pub use super::common::Changetype;
pub use super::info::Info;
pub use super::idset::IdSet;

use super::dense::Dense;
use super::common::{PackStringTable,get_changetype};

use std::io::{Error,Result,ErrorKind};


#[derive(Debug)]
pub struct PrimitiveBlock {
    pub index: i64,
    pub location: u64,
    pub quadtree: quadtree::Quadtree,
    pub start_date: i64, 
    pub end_date: i64, 
    pub nodes: Vec<Node>,
    pub ways: Vec<Way>,
    pub relations: Vec<Relation>,
}


fn read_stringtable(data: &[u8]) -> Result<Vec<String>> {
    let mut res = Vec::new();
    for x in read_pbf::IterTags::new(&data,0) {
        match x {
            read_pbf::PbfTag::Data(1, d) => {
                let s = std::str::from_utf8(d).unwrap().to_string();
                res.push(s);
            },
            
            _ => return Err(Error::new(ErrorKind::Other,"unexpected item")),
        }
    
        
    }
    Ok(res)
}


impl PrimitiveBlock {
    pub fn new(index: i64, location: u64) -> PrimitiveBlock {
        PrimitiveBlock{index:index, location:location,
            quadtree: quadtree::Quadtree::new(0),
            start_date: 0, end_date: 0,
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
            }
    }
    
    pub fn sort(&mut self) {
        self.nodes.sort();
        self.ways.sort();
        self.relations.sort();
    }
    
    pub fn len(&self) -> usize {
        self.nodes.len()+self.ways.len()+self.relations.len()
    }
    
    pub fn read(index: i64, location: u64, data: &[u8], ischange: bool, minimal: bool) -> Result<PrimitiveBlock> {
        Self::read_check_ids(index, location, data, ischange, minimal, None)
    }
    
    pub fn read_check_ids(index: i64, location: u64, data: &[u8], ischange: bool, minimal: bool, idset: Option<&IdSet>) -> Result<PrimitiveBlock> {
        
        let mut res = PrimitiveBlock::new(index,location);
        
        
        let mut strings = Vec::new();
        let mut groups = Vec::new();
        for x in read_pbf::IterTags::new(&data,0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => {
                    if !minimal { strings = read_stringtable(&d)?
                    }
                },
                read_pbf::PbfTag::Data(2, d) => groups.push(d),
                
                read_pbf::PbfTag::Value(32, qt) => res.quadtree = quadtree::Quadtree::new(read_pbf::un_zig_zag(qt)),
                read_pbf::PbfTag::Value(33, sd) => res.start_date = sd as i64,
                read_pbf::PbfTag::Value(34, ed) => res.end_date = ed as i64,
                
                _ => return Err(Error::new(ErrorKind::Other,"unexpected item")),
            }
        }
        
        
        for g in groups {
            let ct = PrimitiveBlock::find_changetype(&g, ischange);
            res.read_group(&strings, ct, &g, minimal, idset)?;
            drop(g);
        }
        drop(strings);
        
        Ok(res)
    }
    
    fn find_changetype(data: &[u8], ischange: bool) -> Changetype {
        if !ischange { return Changetype::Normal; }
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Value(10,ct) => {return get_changetype(ct);},
                _ => {},
            }
        }
        Changetype::Normal
    }
    
    fn read_group(&mut self, strings: &Vec<String>, changetype: Changetype, data: &[u8], minimal: bool, idset: Option<&IdSet>) -> Result<u64> {
        let mut count=0;
        for x in read_pbf::IterTags::new(&data,0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => count += self.read_node(strings, changetype, &d, minimal, idset)?,
                read_pbf::PbfTag::Data(2, d) => count += self.read_dense(strings, changetype, &d, minimal, idset)?,
                read_pbf::PbfTag::Data(3, d) => count += self.read_way(strings, changetype, &d, minimal, idset)?,
                read_pbf::PbfTag::Data(4, d) => count += self.read_relation(strings, changetype, &d, minimal, idset)?,
                read_pbf::PbfTag::Value(10,_) => {},
                _ => return Err(Error::new(ErrorKind::Other,"unexpected item")),
            }
        }
        Ok(count)
    }
    
    fn read_node(&mut self, strings: &Vec<String>, changetype: Changetype, data: &[u8], minimal: bool, idset: Option<&IdSet>) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Node,get_id(&data)) {
                    return Ok(0);
                }
            },
            None => {}
        }
        let n = Node::read(changetype, &strings, &data, minimal)?;
        self.nodes.push(n);
        Ok(1)
        
    }
    fn read_way(&mut self, strings: &Vec<String>, changetype: Changetype, data: &[u8], minimal: bool, idset: Option<&IdSet>) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Way,get_id(&data)) {
                    return Ok(0);
                }
            },
            None => {}
        }
        let w = Way::read(changetype, &strings, &data, minimal)?;
        self.ways.push(w);
        Ok(1)
    }
    fn read_relation(&mut self, strings: &Vec<String>, changetype: Changetype, data: &[u8], minimal: bool, idset: Option<&IdSet>) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Relation,get_id(&data)) {
                    return Ok(0);
                }
            },
            None => {}
        }
        let r = Relation::read(changetype, &strings, &data, minimal)?;
        self.relations.push(r);
        Ok(1)
    }
    fn read_dense(&mut self, strings: &Vec<String>, changetype: Changetype, data: &[u8], minimal: bool, idset: Option<&IdSet>) -> Result<u64> {
        let nn = Dense::read(changetype,&strings, &data, minimal, idset)?;
        let nl = nn.len() as u64;
        for n in nn {
            self.nodes.push(n);
        }
        
        Ok(nl)
        
    }
    
    pub fn pack(&self, include_qts: bool, as_change: bool) -> Result<Vec<u8>> {
        if as_change {
            return Err(Error::new(ErrorKind::Other,"not impl"));
        }
    
        let mut pack_strings = Box::new(PackStringTable::new());
        
        
        let mut groups = Vec::new();
        if self.nodes.len()>0 {
            
            groups.push(self.pack_nodes(&mut pack_strings, include_qts, as_change)?);
        }
        if self.ways.len()>0 {
            groups.push(self.pack_ways(&mut pack_strings, include_qts, as_change)?);
        }
        if self.relations.len()>0 {
            groups.push(self.pack_relations(&mut pack_strings, include_qts, as_change)?);
        }

        let pp = pack_strings.pack();
        let mut outl = write_pbf::data_length(1, pp.len());
        for g in &groups { outl += write_pbf::data_length(2, g.len()); }
        
        
        if include_qts {                
            outl += write_pbf::value_length(32, write_pbf::zig_zag(self.quadtree.as_int()));
            outl += write_pbf::value_length(33, self.start_date as u64);
            outl += write_pbf::value_length(34, self.end_date as u64);
        }
        
        let mut res = Vec::with_capacity(outl);
        write_pbf::pack_data(&mut res, 1, &pp);
        for g in groups {
            write_pbf::pack_data(&mut res, 2, &g);
        }
        if include_qts {
            write_pbf::pack_value(&mut res, 32, write_pbf::zig_zag(self.quadtree.as_int()));
            if self.start_date != 0 {
                write_pbf::pack_value(&mut res, 33, self.start_date as u64);
            }
            if self.end_date != 0 {
                write_pbf::pack_value(&mut res, 34, self.end_date as u64);
            }
        }
        Ok(res)
    }    
        
            
    fn pack_nodes(&self, prep_strings: &mut Box<PackStringTable>, include_qts: bool, _as_change: bool) -> Result<Vec<u8>> {
        let mut res=Vec::new();
        write_pbf::pack_data(&mut res, 2, &Dense::pack(&self.nodes, prep_strings, include_qts)?);
        return Ok(res);
    }
    
    fn pack_ways(&self, prep_strings: &mut Box<PackStringTable>, include_qts: bool, _as_change: bool) -> Result<Vec<u8>> {
        let mut res=Vec::new();
        for w in &self.ways {
            write_pbf::pack_data(&mut res, 3, &w.pack(prep_strings, include_qts)?);
        }
        return Ok(res);
    }
    
    fn pack_relations(&self, prep_strings: &mut Box<PackStringTable>, include_qts: bool, _as_change: bool) -> Result<Vec<u8>> {
        let mut res=Vec::new();
        for r in &self.relations {
            write_pbf::pack_data(&mut res, 4, &r.pack(prep_strings, include_qts)?);
        }
        return Ok(res);
    }
    
}

fn get_id(data: &[u8]) -> i64 {
    match read_pbf::read_tag(data, 0) {
        (read_pbf::PbfTag::Value(1,i),_) => {i as i64},
        _ => 0
    }
}
        
                
