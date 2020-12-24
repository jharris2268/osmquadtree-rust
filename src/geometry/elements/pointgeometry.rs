
use crate::elements::{Info,Tag,Quadtree,Node};
use crate::geometry::LonLat;

pub struct PointGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub lonlat: LonLat,
    pub minzoom: i64,
    pub quadtree: Quadtree
}

impl PointGeometry {
    pub fn from_node(n: Node) -> PointGeometry {
        PointGeometry{id: n.id, info: n.info, tags: n.tags, lonlat: LonLat::new(n.lon,n.lat), quadtree: n.quadtree, minzoom: 0}
    }
}

