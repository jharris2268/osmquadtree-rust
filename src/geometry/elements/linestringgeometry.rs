use crate::elements::{Info,Tag,Quadtree,Way,Bbox};
use crate::geometry::LonLat;
use crate::geometry::elements::pointgeometry::pack_tags;
use crate::geometry::elements::simplepolygongeometry::{transform_lonlats,pack_bounds};
use serde::Serialize;
use serde_json::{json,Value,Map};

#[derive(Serialize)]
pub struct LinestringGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,
    pub lonlats: Vec<LonLat>,
    pub length: f64,
    pub z_order: Option<i64>,
    pub layer: Option<i64>,
    pub minzoom: Option<i64>,
    pub quadtree: Quadtree
}

impl LinestringGeometry {
    pub fn from_way(w: Way, lonlats: Vec<LonLat>, tgs: Vec<Tag>, length: f64, layer: Option<i64>, z_order: Option<i64>) -> LinestringGeometry {
        LinestringGeometry{id: w.id, info: w.info, tags: tgs, refs: w.refs, lonlats: lonlats, quadtree: w.quadtree, length: length, layer: layer, z_order: z_order, minzoom: None}
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
        
        res.insert(String::from("type"), json!("LineString"));
        res.insert(String::from("coordinates"), json!(transform_lonlats(&self.lonlats, false)));
        Ok(json!(res))
    }
        
    pub fn to_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        res.insert(String::from("type"), json!("Feature"));
        res.insert(String::from("id"), json!(self.id));
        res.insert(String::from("quadtree"), json!(self.quadtree.as_tuple().xyz()));
        res.insert(String::from("properties"), pack_tags(&self.tags)?);
        res.insert(String::from("geometry"), self.to_geometry_geojson()?);
        res.insert(String::from("way_length"), json!(f64::round(self.length*10.0)/10.0));
        
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
