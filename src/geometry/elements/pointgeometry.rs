
use crate::elements::{Info,Tag,Quadtree,Node};
use crate::geometry::LonLat;

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
    
    
    fn to_geometry_geojson(&self) -> std::io::Result<Value> {
        
        let mut res = Map::new();
        let p = self.lonlat.forward();
        res.insert(String::from("type"), json!("Point"));
        res.insert(String::from("coordinates"), json!(vec![p.x,p.y]));
        Ok(json!(res))
    }
        
    pub fn to_geojson(&self) -> std::io::Result<Value> {
        
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
        res.insert(String::from("bounds"), json!(vec![p.x,p.y,p.x,p.y]));
                
        Ok(json!(res))
    }
}

