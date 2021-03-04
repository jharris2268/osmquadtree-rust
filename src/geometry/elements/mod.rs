mod complicatedpolygongeometry;
mod linestringgeometry;
mod pointgeometry;
mod simplepolygongeometry;

pub use complicatedpolygongeometry::{
    collect_rings, ComplicatedPolygonGeometry, PolygonPart, Ring, RingPart,
};
pub use linestringgeometry::LinestringGeometry;
pub use pointgeometry::PointGeometry;
pub use simplepolygongeometry::SimplePolygonGeometry;

pub trait GeoJsonable {
    fn to_geojson(&self) -> std::io::Result<serde_json::Value>;
}
