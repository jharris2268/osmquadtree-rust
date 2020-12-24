use crate::geometry::{PointGeometry,ComplicatedPolygonGeometry};
use crate::elements::{Quadtree,Node,Way,Relation};
use crate::geometry::pack_geometry::{pack_geometry_block, unpack_geometry_block};

use std::io::Result;
use std::fmt;

type LinestringGeometry = ();
type SimplePolygonGeometry = ();
//pub type ComplicatedPolygonGeometry = ();


pub enum Object {
    Node(Node),
    Way(Way),
    Relation(Relation),
    
    PointGeometry(PointGeometry),
    LinestringGeometry(LinestringGeometry),
    SimplePolygonGeometry(SimplePolygonGeometry),
    ComplicatedPolygonGeometry(ComplicatedPolygonGeometry),
}

pub struct GeometryBlock {
    pub index: i64,
    pub quadtree: Quadtree,
    pub end_date: i64,
    
    pub points: Vec<PointGeometry>,
    pub linestrings: Vec<LinestringGeometry>,
    pub simple_polygons: Vec<SimplePolygonGeometry>,
    pub complicated_polygons: Vec<ComplicatedPolygonGeometry>,
    
}

impl GeometryBlock {
    pub fn new(index: i64, quadtree: Quadtree, end_date: i64) -> GeometryBlock {
        GeometryBlock{index: index,quadtree: quadtree,end_date: end_date, points:Vec::new(), linestrings:Vec::new(), simple_polygons:Vec::new(), complicated_polygons:Vec::new()}
    }
    
    pub fn unpack(index: i64, data: &[u8]) -> Result<GeometryBlock> {
        unpack_geometry_block(index, data)
    }
    
    pub fn pack(&self) -> Result<Vec<u8>> {
        pack_geometry_block(self)
    }
    
    pub fn len(&self) -> usize {
        self.points.len()+self.linestrings.len()+self.simple_polygons.len()+self.complicated_polygons.len()
    }
}
impl fmt::Display for GeometryBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GeometryBlock[{} [{}] with {} points, {} linestrings, {} simple polygons, {} complicated polgons]", 
            self.index,  self.quadtree, 
            self.points.len(), self.linestrings.len(),
            self.simple_polygons.len(), self.complicated_polygons.len())
    }
}
