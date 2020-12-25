use crate::elements::{Info,Tag,Quadtree,Way,Bbox};
use crate::geometry::LonLat;

use serde::Serialize;

#[derive(Serialize)]
pub struct SimplePolygonGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,
    pub lonlats: Vec<LonLat>,
    pub area: f64,
    pub z_order: i64,
    pub layer: i64,
    pub minzoom: i64,
    pub quadtree: Quadtree
}

impl SimplePolygonGeometry {
    pub fn from_way(w: Way, lonlats: Vec<LonLat>, tgs: Vec<Tag>, area: f64, layer: i64, z_order: i64) -> SimplePolygonGeometry {
        SimplePolygonGeometry{id: w.id, info: w.info, tags: tgs, refs: w.refs, lonlats: lonlats, quadtree: w.quadtree, area: area, layer: layer, z_order: z_order, minzoom: 0}
    }
    
    pub fn bounds(&self) -> Bbox {
        let mut res=Bbox::empty();
        for l in &self.lonlats {
            res.expand(l.lon, l.lat);
        }
        res
    }
}
