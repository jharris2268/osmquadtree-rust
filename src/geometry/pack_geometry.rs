use crate::geometry::{GeometryBlock,/*PointGeometry,LinestringGeometry, SimplePolygonGeometry, ComplicatedPolygonGeometry*/};

use std::io::{Result,Error,ErrorKind};

pub fn pack_geometry_block(_gb: &GeometryBlock) -> Result<Vec<u8>> {
    Err(Error::new(ErrorKind::Other, "not impl"))
}

pub fn unpack_geometry_block(_idx: i64, _data: &[u8] ) -> Result<GeometryBlock> {
    Err(Error::new(ErrorKind::Other, "not impl"))
}
