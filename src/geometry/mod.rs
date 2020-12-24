mod waywithnodes;
mod pack_geometry;
mod process_geometry;
mod style;
mod default_style;
mod elements;
mod geometry_block;
mod multipolygons;
mod position;

use crate::elements::{Quadtree,Node,Way,Relation};
pub use crate::geometry::waywithnodes::{CollectWayNodes};
pub use crate::geometry::position::{LonLat,XY};

pub use crate::geometry::process_geometry::process_geometry;
pub use crate::geometry::style::GeometryStyle;
pub use crate::geometry::geometry_block::{GeometryBlock,Object,};
pub use crate::geometry::elements::{PointGeometry,ComplicatedPolygonGeometry,RingPart,Ring,PolygonPart};





pub struct WorkingBlock {
    geometry_block: GeometryBlock,
    
    pending_nodes: Vec<Node>,
    pending_ways: Vec<(Way,Vec<LonLat>,Vec<String>)>,
    pending_relations: Vec<Relation>,
    
    
}
impl WorkingBlock {
    pub fn new(index: i64, quadtree: Quadtree, end_date: i64) -> WorkingBlock {
        WorkingBlock{geometry_block: GeometryBlock::new(index,quadtree,end_date), pending_nodes: Vec::new(), pending_ways: Vec::new(), pending_relations: Vec::new()}
    }
}




pub enum OtherData {
    Errors(Vec<(Object, String)>),
    Messages(Vec<String>)
}

pub type Timings = crate::utils::Timings<OtherData>;
