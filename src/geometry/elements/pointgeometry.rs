
use crate::elements::{Info,Tag,Quadtree,Node};
use crate::geometry::LonLat;

use serde::Serialize;

#[derive(Serialize)]
pub struct PointGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub lonlat: LonLat,
    pub layer: i64,
    pub minzoom: i64,
    pub quadtree: Quadtree
}

impl PointGeometry {
    pub fn from_node(n: Node, tgs: Vec<Tag>, layer: i64) -> PointGeometry {
        PointGeometry{id: n.id, info: n.info, tags: tgs, lonlat: LonLat::new(n.lon,n.lat), quadtree: n.quadtree, layer: layer, minzoom: 0}
    }
}

