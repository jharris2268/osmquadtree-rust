
use crate::elements::{Info,Tag,Quadtree,Node};
use crate::geometry::LonLat;

pub struct PointGeometry {
    id: i64,
    info: Option<Info>,
    tags: Vec<Tag>,
    lonlat: LonLat,
    quadtree: Quadtree
}

impl PointGeometry {
    pub fn from_node(n: Node) -> PointGeometry {
        PointGeometry{id: n.id, info: n.info, tags: n.tags, lonlat: LonLat::new(n.lon,n.lat), quadtree: n.quadtree}
    }
}

