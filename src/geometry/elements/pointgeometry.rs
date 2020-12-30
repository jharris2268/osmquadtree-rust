use crate::geometry::elements::GeoJsonable;
use crate::elements::{Info,Tag,Quadtree,Node};
//use crate::elements::quadtree::coordinate_as_float;
use crate::geometry::LonLat;

extern crate geo;
extern crate geojson;

use serde::Serialize;
use serde_json::{json,Value,Map};

pub fn pack_tags(tgs: &[Tag]) -> std::io::Result<Value> {
    let mut res = Map::new();
    for t in tgs {
        res.insert(t.key.clone(), json!(t.val));
    }
    Ok(json!(res))
}


#[derive(Serialize)]
pub struct PointGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub lonlat: LonLat,
    pub layer: Option<i64>,
    pub minzoom: Option<i64>,
    pub quadtree: Quadtree
}

impl PointGeometry {
    pub fn from_node(n: Node, tgs: Vec<Tag>, layer: Option<i64>) -> PointGeometry {
        PointGeometry{id: n.id, info: n.info, tags: tgs, lonlat: LonLat::new(n.lon,n.lat), quadtree: n.quadtree, layer: layer, minzoom: None}
    }
    
    pub fn to_geo(&self, transform: bool) -> geo::Point<f64> {
        geo::Point(self.lonlat.to_xy(transform))
    }
    
    fn to_geometry_geojson(&self) -> std::io::Result<Value> {
        let geom = geojson::Value::from(&self.to_geo(false));
        
        Ok(Value::from(&geom))
        /*
        
        let mut res = Map::new();
        //let p = self.lonlat.forward();
        res.insert(String::from("type"), json!("Point"));
        res.insert(String::from("coordinates"), json!((coordinate_as_float(self.lonlat.lon),coordinate_as_float(self.lonlat.lat))));
        Ok(json!(res))*/
    }
}

impl GeoJsonable for PointGeometry {        
    fn to_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        res.insert(String::from("type"), json!("Feature"));
        res.insert(String::from("id"), json!(self.id));
        res.insert(String::from("quadtree"), json!(self.quadtree.as_tuple().xyz()));
        res.insert(String::from("properties"), pack_tags(&self.tags)?);
        res.insert(String::from("geometry"), self.to_geometry_geojson()?);
        
        match self.layer {
            None => {},
            Some(l) => { res.insert(String::from("layer"), json!(l)); }
        }
        match self.minzoom {
            None => {},
            Some(l) => { res.insert(String::from("minzoom"), json!(l)); }
        }
        
        let p = self.lonlat.forward();
        res.insert(String::from("bbox"), json!(vec![p.x,p.y,p.x,p.y]));
                
        Ok(json!(res))
    }
}

