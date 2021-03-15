use crate::elements::{Bbox, Info, Quadtree, Tag, Way};
use crate::geometry::elements::pointgeometry::pack_tags;
use crate::geometry::elements::simplepolygongeometry::{pack_bounds, read_lonlats};
use crate::geometry::elements::GeoJsonable;
use crate::geometry::wkb::{prep_wkb, write_ring};
use crate::geometry::LonLat;
use serde::Serialize;
use serde_json::{json, Map, Value};

extern crate geo;

#[derive(Debug, Serialize)]
pub struct LinestringGeometry {
    pub id: i64,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,
    pub lonlats: Vec<LonLat>,
    pub length: f64,
    pub z_order: Option<i64>,
    pub layer: Option<i64>,
    pub minzoom: Option<i64>,
    pub quadtree: Quadtree,
}

impl LinestringGeometry {
    pub fn empty() -> LinestringGeometry {
        LinestringGeometry{id: 0, info: None, tags: Vec::new(), refs: Vec::new(), lonlats: Vec::new(),
            length: 0.0, layer: None, z_order: None, minzoom: None, quadtree: Quadtree::empty()}
    }
    
    pub fn from_way(
        w: Way,
        lonlats: Vec<LonLat>,
        tgs: Vec<Tag>,
        length: f64,
        layer: Option<i64>,
        z_order: Option<i64>,
    ) -> LinestringGeometry {
        LinestringGeometry {
            id: w.id,
            info: w.info,
            tags: tgs,
            refs: w.refs,
            lonlats: lonlats,
            quadtree: w.quadtree,
            length: length,
            layer: layer,
            z_order: z_order,
            minzoom: None,
        }
    }

    pub fn to_geo(&self, transform: bool) -> geo::LineString<f64> {
        geo::LineString(self.lonlats.iter().map(|l| l.to_xy(transform)).collect())
    }

    pub fn to_wkb(&self, transform: bool, with_srid: bool) -> std::io::Result<Vec<u8>> {
        let mut res = prep_wkb(with_srid, transform, 2, 4 + 16 * self.lonlats.len())?;
        write_ring(
            &mut res,
            self.lonlats.len(),
            self.lonlats.iter().map(|l| l.to_xy(transform)),
        )?;
        Ok(res)
    }

    pub fn bounds(&self) -> Bbox {
        let mut res = Bbox::empty();
        for l in &self.lonlats {
            res.expand(l.lon, l.lat);
        }
        res
    }

    fn to_geometry_geojson(&self) -> std::io::Result<Value> {
        let mut res = Map::new();

        res.insert(String::from("type"), json!("LineString"));
        res.insert(
            String::from("coordinates"),
            json!(read_lonlats(&self.lonlats, false)),
        );
        Ok(json!(res))
    }
}

impl GeoJsonable for LinestringGeometry {
    fn to_geojson(&self) -> std::io::Result<Value> {
        let mut res = Map::new();
        res.insert(String::from("type"), json!("Feature"));
        res.insert(String::from("id"), json!(self.id));
        res.insert(
            String::from("quadtree"),
            json!(self.quadtree.as_tuple().xyz()),
        );
        res.insert(String::from("properties"), pack_tags(&self.tags)?);
        res.insert(String::from("geometry"), self.to_geometry_geojson()?);
        res.insert(
            String::from("way_length"),
            json!(f64::round(self.length * 10.0) / 10.0),
        );

        match self.layer {
            None => {}
            Some(l) => {
                res.insert(String::from("layer"), json!(l));
            }
        }
        match self.z_order {
            None => {}
            Some(l) => {
                res.insert(String::from("z_order"), json!(l));
            }
        }
        match self.minzoom {
            None => {}
            Some(l) => {
                res.insert(String::from("minzoom"), json!(l));
            }
        }
        res.insert(String::from("bbox"), pack_bounds(&self.bounds()));

        Ok(json!(res))
    }
}

use crate::elements::{WithId, WithInfo, WithQuadtree, WithTags,SetCommon};
impl WithId for LinestringGeometry {
    fn get_id(&self) -> i64 {
        self.id
    }
}

impl WithInfo for LinestringGeometry {
    fn get_info<'a>(&'a self) -> &Option<Info> {
        &self.info
    }
}

impl WithTags for LinestringGeometry {
    fn get_tags<'a>(&'a self) -> &'a [Tag] {
        &self.tags
    }
}

impl WithQuadtree for LinestringGeometry {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}
impl SetCommon for LinestringGeometry {
    fn set_id(&mut self, id: i64) {
        self.id = id;
    }
    fn set_info(&mut self, info: Info) {
        self.info = Some(info);
    }
    fn set_tags(&mut self, tags: Vec<Tag>) {
        self.tags = tags;
    }
    fn set_quadtree(&mut self, quadtree: Quadtree) {
        self.quadtree = quadtree;
    }
}
