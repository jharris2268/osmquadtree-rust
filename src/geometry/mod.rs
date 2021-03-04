mod addparenttag;
mod default_minzoom_values;
mod default_style;
mod elements;
mod geometry_block;
mod minzoom;
mod multipolygons;
mod pack_geometry;
mod position;
pub mod postgresql;
mod process_geometry;
mod relationtags;
mod style;
mod waywithnodes;
mod wkb;

use crate::elements::{Element, Node, Quadtree, Relation, Way};
pub use crate::geometry::position::{get_srid, LonLat, XY};
pub use crate::geometry::waywithnodes::CollectWayNodes;

pub use crate::geometry::elements::{
    ComplicatedPolygonGeometry, LinestringGeometry, PointGeometry, PolygonPart, Ring, RingPart,
    SimplePolygonGeometry,
};
pub use crate::geometry::geometry_block::GeometryBlock;
pub use crate::geometry::process_geometry::{process_geometry, OutputType};
pub use crate::geometry::style::GeometryStyle;

use std::collections::BTreeMap;

pub struct WorkingBlock {
    geometry_block: GeometryBlock,

    pending_nodes: Vec<Node>,
    pending_ways: Vec<(Way, Vec<LonLat>)>,
    pending_relations: Vec<Relation>,
}
impl WorkingBlock {
    pub fn new(index: i64, quadtree: Quadtree, end_date: i64) -> WorkingBlock {
        WorkingBlock {
            geometry_block: GeometryBlock::new(index, quadtree, end_date),
            pending_nodes: Vec::new(),
            pending_ways: Vec::new(),
            pending_relations: Vec::new(),
        }
    }
}

pub enum OtherData {
    Errors(Vec<(Element, String)>),
    Messages(Vec<String>),
    GeometryBlocks(BTreeMap<Quadtree, GeometryBlock>),
}

pub type Timings = crate::utils::Timings<OtherData>;

pub type CallFinishGeometryBlock =
    Box<dyn crate::callback::CallFinish<CallType = GeometryBlock, ReturnType = Timings>>;
