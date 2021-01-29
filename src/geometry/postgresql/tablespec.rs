use crate::geometry::GeometryStyle;    
use serde::Serialize;

#[derive(Eq,PartialEq,Debug,Clone,Serialize)]
pub enum ColumnType {
    Text,
    BigInteger,
    //Integer,
    Double,
    Hstore,
    //Json,
    //TextArray,
    Geometry,
    PointGeometry,
    LineGeometry,
    PolygonGeometry
}

#[allow(dead_code)]
fn type_str(ct: &ColumnType) -> &str {
    
    match ct {
        ColumnType::BigInteger => "bigint",
        ColumnType::Text => "text",
        ColumnType::Double => "float",
        ColumnType::Hstore => "hstore",
        ColumnType::Geometry => "geometry(Geometry, 3857)",
        ColumnType::PointGeometry => "geometry(Point, 3857)",
        ColumnType::LineGeometry => "geometry(Linestring, 3857)",
        ColumnType::PolygonGeometry => "geometry(Polygon, 3857)",
    }
}
    

#[derive(Eq,PartialEq,Debug,Clone,Serialize)]
pub enum ColumnSource {
    OsmId,
    //Part,
    ObjectQuadtree,
    BlockQuadtree,
    Tag,
    OtherTags,
    Layer,
    ZOrder,
    MinZoom,
    Length,
    Area,
    Geometry,
    RepresentativePointGeometry,
    BoundaryLineGeometry
}

#[derive(Debug,Serialize)]
pub struct TableSpec {
    pub name: String,
    pub columns: Vec<(String, ColumnSource, ColumnType)>,
}
impl TableSpec {
    pub fn new(name: &str, columns: Vec<(String, ColumnSource, ColumnType)>) -> TableSpec {
        TableSpec{name: String::from(name),columns: columns}
    }
}

pub fn make_createtable(spec: &TableSpec, prfx: &str) -> std::io::Result<String> {

    let mut cols = Vec::new();
    for (n,_,t) in &spec.columns {
        cols.push(format!("{} {}", n, type_str(t)));
    }
    
    Ok(format!("CREATE TABLE {}{} ({})", prfx, spec.name, cols.join(", ")))
}

  

fn make_point_spec(with_quadtree: bool, tag_cols: &Vec<String>, with_other_tags: bool, with_minzoom: bool) -> Vec<(String, ColumnSource, ColumnType)> {
    
    let mut res = Vec::new();
    res.push((String::from("osm_id"), ColumnSource::OsmId, ColumnType::BigInteger));
    if with_quadtree {
        res.push((String::from("quadtree"), ColumnSource::ObjectQuadtree, ColumnType::BigInteger));
        res.push((String::from("tile"), ColumnSource::BlockQuadtree, ColumnType::BigInteger));
    }
    
    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }
    
        
    
    if with_other_tags {
        res.push((String::from("tags"), ColumnSource::OtherTags, ColumnType::Hstore));
    }
    res.push((String::from("layer"), ColumnSource::Layer, ColumnType::BigInteger));
    if with_minzoom {
        res.push((String::from("minzoom"), ColumnSource::MinZoom, ColumnType::BigInteger));
    }
    res.push((String::from("way"), ColumnSource::Geometry, ColumnType::PointGeometry));
    
    res
}

fn make_linestring_spec(with_quadtree: bool, tag_cols: &Vec<String>, with_other_tags: bool, with_minzoom: bool, with_length: bool) -> Vec<(String, ColumnSource, ColumnType)> {
    
    let mut res = Vec::new();
    res.push((String::from("osm_id"), ColumnSource::OsmId, ColumnType::BigInteger));
    if with_quadtree {
        res.push((String::from("quadtree"), ColumnSource::ObjectQuadtree, ColumnType::BigInteger));
        res.push((String::from("tile"), ColumnSource::BlockQuadtree, ColumnType::BigInteger));
    }
    
    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }
    
    if with_other_tags {
        res.push((String::from("tags"), ColumnSource::OtherTags, ColumnType::Hstore));
    }
    res.push((String::from("layer"), ColumnSource::Layer, ColumnType::BigInteger));
    res.push((String::from("z_order"), ColumnSource::ZOrder, ColumnType::BigInteger));
    if with_length {
        res.push((String::from("length"), ColumnSource::Length, ColumnType::Double));
    }
    
    if with_minzoom {
        res.push((String::from("minzoom"), ColumnSource::MinZoom, ColumnType::BigInteger));
    }
    res.push((String::from("way"), ColumnSource::Geometry, ColumnType::LineGeometry));
    
    res
}


fn make_polygon_spec(with_quadtree: bool, tag_cols: &Vec<String>, with_other_tags: bool, with_point_geom: bool, with_boundary_geom: bool, with_minzoom: bool) -> Vec<(String, ColumnSource, ColumnType)> {
    
    let mut res = Vec::new();
    res.push((String::from("osm_id"), ColumnSource::OsmId, ColumnType::BigInteger));
    if with_quadtree {
        res.push((String::from("quadtree"), ColumnSource::ObjectQuadtree, ColumnType::BigInteger));
        res.push((String::from("tile"), ColumnSource::BlockQuadtree, ColumnType::BigInteger));
    }
    
    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }
    
    if with_other_tags {
        res.push((String::from("tags"), ColumnSource::OtherTags, ColumnType::Hstore));
    }
    res.push((String::from("layer"), ColumnSource::Layer, ColumnType::BigInteger));
    res.push((String::from("z_order"), ColumnSource::ZOrder, ColumnType::BigInteger));
    
    res.push((String::from("way_area"), ColumnSource::Area, ColumnType::Double));
    
    if with_minzoom {
        res.push((String::from("minzoom"), ColumnSource::MinZoom, ColumnType::BigInteger));
    }
    
    res.push((String::from("way"), ColumnSource::Geometry, ColumnType::Geometry));
    if with_point_geom {
        res.push((String::from("way_point"), ColumnSource::RepresentativePointGeometry, ColumnType::PointGeometry));
    }
    if with_boundary_geom {
        res.push((String::from("way_exterior"), ColumnSource::BoundaryLineGeometry, ColumnType::Geometry));
    }
    
    res
}

const DEFAULT_EXTRA_NODE_COLS: &str = r#"["access","addr:housename","addr:housenumber","addr:interpolation","admin_level","bicycle","covered","foot","horse","name","oneway","ref","religion","surface"]"#; //"layer"
const DEFAULT_EXTRA_WAY_COLS: &str = r#"["addr:housenumber", "admin_level", "bicycle", "name", "tracktype", "addr:interpolation", "addr:housename", "horse", "surface", "access", "religion", "oneway", "foot", "covered", "ref"]"#; //"layer"

pub fn make_table_spec(style: &GeometryStyle, extended: bool) -> Vec<TableSpec> {
    let mut res = Vec::new();
    
    let mut point_tag_cols = Vec::new();
    let mut line_tag_cols = Vec::new();
    
    for k in &style.feature_keys {
        point_tag_cols.push(k.clone());
        line_tag_cols.push(k.clone());
    }
    
    match &style.other_keys {
        None => {
            let enc: Vec<String> = serde_json::from_str(&DEFAULT_EXTRA_NODE_COLS).expect("!!");
            for k in &enc {
                point_tag_cols.push(k.clone());
            }
                
            let ewc: Vec<String> = serde_json::from_str(&DEFAULT_EXTRA_WAY_COLS).expect("!!");
            for k in &ewc {
                line_tag_cols.push(k.clone());
            }
        },
        Some(oo) => {
            for k in oo {
                point_tag_cols.push(k.clone());
                line_tag_cols.push(k.clone());
            }
        }
    }
    
    point_tag_cols.sort();
    line_tag_cols.sort();
    
    let poly_tag_cols = line_tag_cols.clone();
    
    if extended {
        for (l,_) in &style.parent_tags {
            point_tag_cols.push(l.clone());
        }
        
        for l in &style.relation_tag_spec {
            line_tag_cols.push(l.target_key.clone());
        }
    }
    
    
    res.push(TableSpec::new("point", make_point_spec(extended, &point_tag_cols, true, extended)));
    res.push(TableSpec::new("line", make_linestring_spec(extended, &line_tag_cols, true, extended, extended)));
    res.push(TableSpec::new("polygon", make_polygon_spec(extended, &poly_tag_cols, true, extended, false, extended)));
    if extended {
        res.push(TableSpec::new("highway", make_linestring_spec(true, &line_tag_cols, true, true, true)));
        res.push(TableSpec::new("building", make_polygon_spec(true, &line_tag_cols, true, true, false, true)));
        res.push(TableSpec::new("boundary", make_polygon_spec(true, &poly_tag_cols, true, true, true, true)));
    }
    
    res
}
    
    
    
    
    
