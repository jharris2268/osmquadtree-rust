use crate::elements::{Info,Tag,Quadtree,Way,Bbox};
use crate::geometry::LonLat;
use crate::geometry::elements::pointgeometry::pack_tags;

use serde::Serialize;
use serde_json::{json,Value,Map};
use std::borrow::Borrow;
pub fn transform_lonlats<T: Borrow<LonLat>>(lonlats: &Vec<T>, is_reversed: bool) -> Vec<(f64,f64)> {
    let mut res = Vec::with_capacity(lonlats.len());
    for l in lonlats {
        let p = l.borrow().forward();
        res.push((p.x,p.y));
    }
    if is_reversed {
        res.reverse();
    }
    res
}
pub fn pack_bounds(bounds: &Bbox) -> Value {
    let a = LonLat::new(bounds.minlon, bounds.minlat).forward();
    let b = LonLat::new(bounds.maxlon, bounds.maxlat).forward();
    json!((a.x,a.y,b.x,b.y))
}



#[derive(Serialize)]
pub struct SimplePolygonGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,
    pub lonlats: Vec<LonLat>,
    pub area: f64,
    pub reversed: bool,
    pub z_order: Option<i64>,
    pub layer: Option<i64>,
    pub minzoom: Option<i64>,
    pub quadtree: Quadtree
    
}

impl SimplePolygonGeometry {
    pub fn from_way(w: Way, lonlats: Vec<LonLat>, tgs: Vec<Tag>, area: f64, layer: Option<i64>, z_order: Option<i64>, reversed: bool) -> SimplePolygonGeometry {
        SimplePolygonGeometry{id: w.id, info: w.info, tags: tgs, refs: w.refs, lonlats: lonlats, quadtree: w.quadtree, area: area, layer: layer, z_order: z_order, minzoom: None, reversed: reversed}
    }
    
    pub fn bounds(&self) -> Bbox {
        let mut res=Bbox::empty();
        for l in &self.lonlats {
            res.expand(l.lon, l.lat);
        }
        res
    }
    
    
    fn to_geometry_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        
        res.insert(String::from("type"), json!("Polygon"));
        res.insert(String::from("coordinates"), json!(vec![transform_lonlats(&self.lonlats, self.reversed)]));
        Ok(json!(res))
    }
        
    pub fn to_geojson(&self) -> std::io::Result<Value> {
        
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
        res.insert(String::from("bounds"), pack_bounds(&self.bounds()));
                
        Ok(json!(res))
    }
}
