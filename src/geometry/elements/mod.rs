mod pointgeometry;
mod complicatedpolygongeometry;
mod linestringgeometry;
mod simplepolygongeometry;

pub use pointgeometry::PointGeometry;
pub use complicatedpolygongeometry::{ComplicatedPolygonGeometry,RingPart,Ring,PolygonPart,collect_rings};
pub use linestringgeometry::LinestringGeometry;
pub use simplepolygongeometry::SimplePolygonGeometry;

pub trait GeoJsonable {
    fn to_geojson(&self) -> std::io::Result<serde_json::Value>;
}