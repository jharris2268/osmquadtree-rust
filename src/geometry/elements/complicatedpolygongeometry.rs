use crate::elements::{Info,Tag,Quadtree};
use crate::geometry::LonLat;

use std::io::{Error,ErrorKind,Result};
use std::fmt;

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

#[derive(Debug)]
pub struct Ring {
    pub parts: Vec<RingPart>,
    pub area: f64
}

impl Ring {
    pub fn new() -> Ring {
        Ring{parts: Vec::new(), area: 0.0}
    }
    
    pub fn reverse(&mut self) {
        self.parts.reverse();
        for p in self.parts.iter_mut() {
            p.is_reversed = !p.is_reversed;
        }
        self.area *= -1.0;
    }
    
    pub fn first_last(&self) -> (i64, i64) {
        let p = &self.parts[0];
        let f = if p.is_reversed {
            p.refs[p.refs.len()-1]
        } else {
            p.refs[0]
        };
        
        let q = &self.parts[self.parts.len()-1];
        let t = if q.is_reversed {
            q.refs[0]
        } else {
            q.refs[q.refs.len()-1]
        };
        (f,t)
    }
    
    pub fn is_ring(&self) -> bool {
        let (f,t) = self.first_last();
        f==t
    }
    
    pub fn refs<'a>(&'a self) -> Result<Vec<&'a i64>> {
        let mut res = Vec::new();
        for p in &self.parts {
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
    pub fn lonlats<'a>(&'a self) -> Result<Vec<&'a LonLat>> {
        let mut res = Vec::new();
        for p in &self.parts {
        
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
    
    
    
}

fn merge_rings(rings: &mut Vec<Ring>) -> (bool,Option<Ring>) {
    if rings.len() == 0 { return (false,None); }
    if rings.len() == 1 {
        if rings[0].is_ring() {
            let zz = rings.remove(0);
            return (true, Some(zz));
        }
        return (false,None);
    }
        
    for i in 0 .. rings.len()-1 {
        let (f,t) = rings[i].first_last();
        if f==t {
            let zz = rings.remove(i);
            return (true, Some(zz));
        }
        for j in i+1 .. rings.len() {
            let (g,u) = rings[j].first_last();
            
            if t == g {
                let zz = rings.remove(j);
                rings[i].parts.extend(zz.parts);
                if rings[i].is_ring() {
                    let zz = rings.remove(i);
                    return (true, Some(zz));
                }
                return (true,None);
            } else if t == u {
                let mut zz = rings.remove(j);
                zz.reverse();
                rings[i].parts.extend(zz.parts);
                if rings[i].is_ring() {
                    let zz = rings.remove(i);
                    return (true, Some(zz));
                }
                return (true,None);
            } else if f==u {
                let mut zz = rings.remove(j);
                zz.reverse();
                rings[i].reverse();
                rings[i].parts.extend(zz.parts);
                return (true,None);
            } else if f == g {
                let zz = rings.remove(j);
                rings[i].reverse();
                rings[i].parts.extend(zz.parts);
                
                return (true,None);
            }
        }
    }
    return (false,None);
}
                


pub fn collect_rings(ww: Vec<RingPart>) -> Result<(Vec<Ring>,Vec<RingPart>)> {
    //let nw=ww.len();
    let mut parts = Vec::new();
    for w in ww {
        let mut r=Ring::new();
        r.parts.push(w);
        parts.push(r);
    }
    
    let mut res = Vec::new();
    loop {
        let (f,r) = merge_rings(&mut parts);
        match r {
            None => {},
            Some(r) => { res.push(r); }
        }
        if !f {
            break;
        }
    }
    
    let mut rem=Vec::new();
    for p in parts {
        for q in p.parts {
            rem.push(q);
        }
    }
    
    //println!("found {} rings from {} ways, {} left", res.len(), nw, rem.len());
    //Err(Error::new(ErrorKind::Other,"not implemented"))
    Ok((res,rem))
}





#[derive(Debug)]
pub struct PolygonPart {
    pub exterior: Ring,
    pub interiors: Vec<Ring>,
    
    pub area: f64
}

impl PolygonPart {
    pub fn new(mut ext: Ring) -> PolygonPart {
        
        if ext.area<0.0 {
            ext.reverse();
        }
        let a=ext.area;
        PolygonPart{exterior:ext, interiors: Vec::new(), area: a}
    }
    
    pub fn add_interior(&mut self, mut p: Ring) {
        if p.area > 0.0 {
            p.reverse();
            
        }
        self.area += p.area;
        self.interiors.push(p);
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
