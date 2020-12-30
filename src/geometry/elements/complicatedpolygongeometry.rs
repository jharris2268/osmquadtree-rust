use crate::elements::{Info,Tag,Quadtree,Relation,Bbox};
use crate::elements::traits::*;
use crate::geometry::LonLat;
use crate::geometry::position::calc_ring_area_and_bbox;
use crate::geometry::elements::pointgeometry::pack_tags;
use crate::geometry::elements::simplepolygongeometry::{read_lonlats,pack_bounds};
use crate::geometry::elements::GeoJsonable;
use std::io::{Error,ErrorKind,Result};
use std::fmt;
use serde::Serialize;
use serde_json::{json,Value,Map};
extern crate geo;

#[derive(Serialize)]
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

#[derive(Debug,Serialize)]
pub struct Ring {
    pub parts: Vec<RingPart>,
    pub area: f64,
    pub bbox: Bbox,
}

impl Ring {
    pub fn new() -> Ring {
        Ring{parts: Vec::new(), area: 0.0, bbox: Bbox::empty()}
    }
    
    pub fn calc_area_bbox(&mut self) -> Result<()> {
        let x = calc_ring_area_and_bbox(&self.lonlats()?);
        self.area=x.0;
        self.bbox=x.1;
        Ok(())
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
       
    pub fn lonlats_iter<'a>(&'a self) -> RingLonLatsIter<'a> {
        RingLonLatsIter::new(self)
    }
    
    pub fn to_geo(&self, transform: bool) -> geo::LineString<f64> {
        geo::LineString(self.lonlats_iter().map(|l| { l.to_xy(transform) }).collect())
    }
}

pub struct RingLonLatsIter<'a> {
    ring: &'a Ring,
    part_idx: usize,
    coord_idx: usize
}

impl<'a> RingLonLatsIter<'a> {
    pub fn new(ring: &'a Ring) -> RingLonLatsIter<'a> {
        RingLonLatsIter{ring: ring, part_idx: 0, coord_idx: 0}
    }
    
    fn curr(&self) -> Option<&'a LonLat> {
        if self.part_idx >= self.ring.parts.len() {
            return None;
        }
        
        let p = &self.ring.parts[self.part_idx];
        
        if p.is_reversed {
            Some(&p.lonlats[p.lonlats.len() - 1 - self.coord_idx])
        } else {
            Some(&p.lonlats[self.coord_idx])
        }
    }
    
    fn next(&mut self) {
        if self.part_idx >= self.ring.parts.len() {
            return;
        }
        self.coord_idx += 1;
        while self.coord_idx == self.ring.parts[self.part_idx].lonlats.len() {
            self.part_idx += 1;
            if self.part_idx >= self.ring.parts.len() {
                return;
            }
            self.coord_idx=0;
        }
    }
        
}

impl<'a> Iterator for RingLonLatsIter<'a> {
    type Item = &'a LonLat;
    
    fn next(&mut self) -> Option<&'a LonLat> {
        match self.curr() {
            None => None,
            Some(r) => {
                self.next();
                Some(r)
            }
        }
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





#[derive(Debug,Serialize)]
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
    
    pub fn prep_coordinates(&self) -> Result<Vec<Vec<(f64,f64)>>> {
        let mut rings = Vec::new();
        
        rings.push(read_lonlats(&self.exterior.lonlats()?,false));
        for ii in &self.interiors {
            rings.push(read_lonlats(&ii.lonlats()?,false));
        }
        
        Ok(rings)
    }
    
}




#[derive(Debug,Serialize)]
pub struct ComplicatedPolygonGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub parts: Vec<PolygonPart>,
    pub z_order: Option<i64>,
    pub layer: Option<i64>,
    pub area: f64,
    pub minzoom: Option<i64>,
    pub quadtree: Quadtree
}

impl ComplicatedPolygonGeometry {
    pub fn new(relation: &Relation, tags: Vec<Tag>, z_order: Option<i64>, layer: Option<i64>, parts: Vec<PolygonPart>) -> ComplicatedPolygonGeometry {
        let mut area=0.0;
        for p in &parts {
            area+=p.area;
        }
        
        ComplicatedPolygonGeometry{id: relation.id, info: relation.info.clone(), tags: tags, parts: parts,
                z_order: z_order, layer: layer, area: area, minzoom: None, quadtree: relation.quadtree}
    }
    
    pub fn to_geo(&self, transform: bool) -> geo::MultiPolygon<f64> {
        
        let mut polys = Vec::new();
        for p in &self.parts {
            //let ext = p.exterior.lonlats().unwrap().iter().map(|l| { l.to_xy(transform) }).collect();
            //let ext = p.exterior.lonlats_iter().map(|l| { l.to_xy(transform) }).collect();
            let ext = p.exterior.to_geo(transform);
            let mut ints = Vec::new();
            for ii in &p.interiors {
                //ints.push(ii.lonlats().unwrap().iter().map(|l| { l.to_xy(transform) }).collect());
                //ints.push(ii.lonlats_iter().map(|l| { l.to_xy(transform) }).collect());
                ints.push(ii.to_geo(transform));
            }
            polys.push(geo::Polygon::new(ext, ints));
        }
        geo::MultiPolygon(polys)
    }
    
    pub fn bounds(&self) -> Bbox {
        let mut res=Bbox::empty();
        for p in &self.parts {
            for l in &p.exterior.lonlats().unwrap() {
                res.expand(l.lon,l.lat);
            }
        }
        res
    }
    
    fn to_geometry_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        if self.parts.len()==1 {
            res.insert(String::from("type"), json!("Polygon"));
            res.insert(String::from("coordinates"), json!(self.parts[0].prep_coordinates()?));
            
        } else {
            res.insert(String::from("type"), json!("MultiPolygon"));
            let mut cc = Vec::new();
            for p in &self.parts {
                cc.push(p.prep_coordinates()?);
            }
            res.insert(String::from("coordinates"), json!(cc));
        }
        Ok(json!(res))
    }
}

impl GeoJsonable for ComplicatedPolygonGeometry {
    
    fn to_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        res.insert(String::from("type"), json!("Feature"));
        res.insert(String::from("id"), json!(self.id));
        res.insert(String::from("quadtree"), json!(self.quadtree.as_tuple().xyz()));
        res.insert(String::from("properties"), pack_tags(&self.tags)?);
        res.insert(String::from("geometry"), self.to_geometry_geojson()?);
        res.insert(String::from("way_area"), json!(f64::round(self.area*10.0)/10.0));
        
        match self.layer {
            None => {},
            Some(l) => { res.insert(String::from("layer"), json!(l)); }
        }
        match self.z_order {
            None => {},
            Some(l) => { res.insert(String::from("z_order"), json!(l)); }
        }
        match self.minzoom {
            None => {},
            Some(l) => { res.insert(String::from("minzoom"), json!(l)); }
        }
        res.insert(String::from("bbox"), pack_bounds(&self.bounds()));
                
        Ok(json!(res))
    }
    
}

impl WithId for ComplicatedPolygonGeometry {
    fn get_id(&self) -> i64 {
        self.id
    }
}
    
impl WithTags for ComplicatedPolygonGeometry {
    fn get_tags<'a>(&'a self) -> &'a [Tag] {
        &self.tags
    }
}
    
impl WithInfo for ComplicatedPolygonGeometry {
    fn get_info<'a>(&'a self) -> &Option<Info> {
        &self.info
    }
}


impl WithQuadtree for ComplicatedPolygonGeometry {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}
   
