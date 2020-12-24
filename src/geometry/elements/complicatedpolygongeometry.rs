use crate::elements::{Info,Tag,Quadtree};
use crate::geometry::LonLat;

use std::io::{Error,ErrorKind,Result};
use std::fmt;

fn collect_refs(parts: &Vec<RingPart>) -> Result<Vec<&i64>> {
    let mut res = Vec::new();
    for p in parts {
        if p.is_reversed {
            let mut ii = p.refs.iter().rev();
            
            if !res.is_empty() {
                let f = ii.next().unwrap();
                if res[res.len()-1] != f {
                    return Err(Error::new(ErrorKind::Other,"not a ring"));
                }
            }
            res.extend(ii);
        } else {
            
            let mut ii = p.refs.iter();
        
        
            if !res.is_empty() {
                let f = ii.next().unwrap();
                if res[res.len()-1] != f {
                    return Err(Error::new(ErrorKind::Other,"not a ring"));
                }
            }
            res.extend(ii);
        }
    }
    if res[0] != res[res.len()-1] {
        return Err(Error::new(ErrorKind::Other,"not a ring"));
    }
    
    Ok(res)
}

fn collect_lonlats(parts: &Vec<RingPart>) -> Result<Vec<&LonLat>> {
    let mut res = Vec::new();
    for p in parts {
        
        if p.is_reversed {
            let mut ii = p.lonlats.iter().rev();
            
            if !res.is_empty() {
                let f = ii.next().unwrap();
                if res[res.len()-1] != f {
                    return Err(Error::new(ErrorKind::Other,"not a ring"));
                }
            }
            res.extend(ii);
        } else {
            
            let mut ii = p.lonlats.iter();
        
        
            if !res.is_empty() {
                let f = ii.next().unwrap();
                if res[res.len()-1] != f {
                    return Err(Error::new(ErrorKind::Other,"not a ring"));
                }
            }
            res.extend(ii);
        }
    }
    if res[0] != res[res.len()-1] {
        return Err(Error::new(ErrorKind::Other,"not a ring"));
    }
    
    Ok(res)
}

#[derive(Debug)]
pub struct PolygonPart {
    pub exterior: Vec<RingPart>,
    pub interiors: Vec<Vec<RingPart>>
}


pub struct RingPart {
    pub orig_id: i64,
    pub is_reversed: bool,
    pub refs: Vec<i64>,
    pub lonlats: Vec<LonLat>
}
impl fmt::Debug for RingPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Point")
         .field("orig_id", &self.orig_id)
         .field("is_reversed", &self.is_reversed)
         .field("np", &self.refs.len())
         .field("first", &self.refs[0])
         .field("last", &self.refs[self.refs.len()-1])
         .finish()
    }
}

impl RingPart {
    pub fn new(orig_id: i64, is_reversed: bool, refs: Vec<i64>, lonlats: Vec<LonLat>) -> RingPart {
        RingPart{orig_id, is_reversed, refs, lonlats}
    }
}

pub struct ComplicatedPolygonGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub parts: Vec<PolygonPart>,
    pub z_order: i64,
    pub area: f64,
    pub minzoom: i64,
    pub quadtree: Quadtree
}
