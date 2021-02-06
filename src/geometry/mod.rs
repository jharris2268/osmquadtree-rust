mod waywithnodes;
mod pack_geometry;
mod process_geometry;
mod style;
mod default_style;
mod elements;
mod geometry_block;
mod multipolygons;
mod position;
mod addparenttag;
mod relationtags;
mod default_minzoom_values;
mod minzoom;
pub mod postgresql;
mod wkb;

use crate::elements::{Quadtree,Node,Way,Relation,Element};
pub use crate::geometry::waywithnodes::{CollectWayNodes};
pub use crate::geometry::position::{LonLat,XY,get_srid};

pub use crate::geometry::process_geometry::{process_geometry,OutputType};
pub use crate::geometry::style::GeometryStyle;
pub use crate::geometry::geometry_block::{GeometryBlock};
pub use crate::geometry::elements::{PointGeometry,ComplicatedPolygonGeometry,RingPart,Ring,PolygonPart,LinestringGeometry,SimplePolygonGeometry};

use std::collections::BTreeMap;



pub struct WorkingBlock {
    geometry_block: GeometryBlock,
    
    pending_nodes: Vec<Node>,
    pending_ways: Vec<(Way,Vec<LonLat>)>,
    pending_relations: Vec<Relation>,
    
    
}
impl WorkingBlock {
    pub fn new(index: i64, quadtree: Quadtree, end_date: i64) -> WorkingBlock {
        WorkingBlock{geometry_block: GeometryBlock::new(index,quadtree,end_date), pending_nodes: Vec::new(), pending_ways: Vec::new(), pending_relations: Vec::new()}
    }
}




pub enum OtherData {
    Errors(Vec<(Element, String)>),
    Messages(Vec<String>),
    GeometryBlocks(BTreeMap<Quadtree,GeometryBlock>)
}

pub type Timings = crate::utils::Timings<OtherData>;

pub type CallFinishGeometryBlock = Box<dyn crate::callback::CallFinish<CallType=GeometryBlock, ReturnType=Timings>>;
