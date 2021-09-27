use crate::geometry::GeometryStyle;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use crate::elements::zoom;

#[derive(Eq, PartialEq, Debug, Clone, Serialize, Deserialize)]
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
    PolygonGeometry,
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

fn is_geom_columntype(ct: &ColumnType) -> bool {
    match ct {
        ColumnType::BigInteger => false,
        ColumnType::Text => false,
        ColumnType::Double => false,
        ColumnType::Hstore => false,
        ColumnType::Geometry => true,
        ColumnType::PointGeometry => true,
        ColumnType::LineGeometry => true,
        ColumnType::PolygonGeometry => true,
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Serialize)]
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
    BoundaryLineGeometry,
}

#[derive(Debug, Serialize)]
pub struct TableSpec {
    pub name: String,
    pub columns: Vec<(String, ColumnSource, ColumnType)>,
}
impl TableSpec {
    pub fn new(name: &str, columns: Vec<(String, ColumnSource, ColumnType)>) -> TableSpec {
        TableSpec {
            name: String::from(name),
            columns: columns,
        }
    }
}

pub fn prepare_tables(
    prfx: Option<&str>,
    spec: &Vec<TableSpec>,
    extended: bool,
    planet_osm_views: bool,
    lowzoom: &Option<Vec<(String,i64,bool)>>
) -> std::io::Result<(Vec<String>, Vec<String>, Vec<String>)> {
    let table_queries: BTreeMap<String, Vec<(TableQueryType, String)>> =
        serde_json::from_str(&TABLE_QUERIES)?;

    let mut before = Vec::new();
    let mut after = Vec::new();
    let mut copy = Vec::new();
    for t in spec {
        let tname = match prfx {
            Some(prfx) => format!("{}{}", prfx, &t.name),
            None => t.name.clone(),
        };

        before.push(format!("DROP TABLE IF EXISTS {} CASCADE", &tname));
        before.push(make_createtable(t, prfx)?);
        before.push(format!(
            "ALTER TABLE {} SET (autovacuum_enabled=false)",
            &tname
        ));

        copy.push(format!("COPY {} FROM STDIN WITH (FORMAT binary)", &tname));

        let table_col_names = make_column_list(t, true)?;
        match table_queries.get(&t.name) {
            None => {}
            Some(xx) => {
                for (a, b) in xx {
                    if use_query(a, extended) {
                        let c = b.replace("%RR%", &table_col_names);
                        
                        after.push(match prfx {
                            Some(prfx) => c.replace("%ZZ%", &prfx),
                            None => c,
                        });
                    }
                }
            }
        }
    }
    if planet_osm_views {
        
        let planet_osm_queries: Vec<(TableQueryType, String)> =
            serde_json::from_str(&PLANET_OSM_QUERIES)?;
        
        for (a, b) in planet_osm_queries {
            if use_query(&a, extended) {
                after.push(match prfx {
                    Some(prfx) => b.replace("%ZZ%", &prfx),
                    None => b,
                });
            }
        }
        

    }
    match lowzoom {
        None => {},
        Some(lowzoom) => {
            for (new_prfx, lz, astable) in lowzoom {
                let pp = format!("{}{}", prfx.unwrap_or("%ZZ%"), new_prfx);
                if *astable {
                    after.extend(make_tables_lowzoom(spec, prfx, &pp, *lz, Some(zoom(*lz as f64)))?);
                } else {
                    after.extend(make_views_lowzoom(spec, prfx, &pp, *lz)?);
                }
            }
        }
        
    }
    Ok((before, copy, after))
}

#[derive(Debug, Deserialize)]
enum TableQueryType {
    All,
    Option,
    Osm2pgsql,
    Extended,
}

fn use_query(t: &TableQueryType, e: bool) -> bool {
    match t {
        TableQueryType::All => true,
        TableQueryType::Option => true,
        TableQueryType::Extended => e,
        TableQueryType::Osm2pgsql => !e,
    }
}

const TABLE_QUERIES: &str = r#"
{
    "point": [
        ["All","CREATE INDEX %ZZ%point_way ON %ZZ%point USING gist(way)"],
        ["Option","CREATE INDEX %ZZ%point_name ON %ZZ%point USING gin(name gin_trgm_ops)"],
        ["Option","CREATE INDEX %ZZ%point_id ON %ZZ%point USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%point"],
        ["All","ALTER TABLE %ZZ%point SET (autovacuum_enabled=true)"],
        ["All", "CREATE VIEW %ZZ%json_point AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%point pp"]
    ],
    "line": [
        ["All","CREATE INDEX %ZZ%line_way ON %ZZ%line USING gist(way)"],
        ["Osm2pgsql","CREATE INDEX %ZZ%line_way_roadslz ON %ZZ%line USING gist(way) WHERE (\n    highway in ('motorway','motorway_link','trunk','trunk_link','primary','primary_link','secondary')\n    or (railway in ('rail','light_rail','narrow_gauge','funicular') and (service IS NULL OR service NOT IN ('spur', 'siding', 'yard')))\n)"],
        ["Option","CREATE INDEX %ZZ%line_name ON %ZZ%line USING gin(name gin_trgm_ops)"],
        ["Option","CREATE INDEX %ZZ%line_id ON %ZZ%line USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%line"],
        ["All","ALTER TABLE %ZZ%line SET (autovacuum_enabled=true)"],
        ["All", "CREATE VIEW %ZZ%json_line AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%line pp"]
    ],
    "highway": [
        ["All","CREATE INDEX %ZZ%highway_way ON %ZZ%highway USING gist(way)"],
        ["Extended","CREATE INDEX %ZZ%highway_way_roadslz ON %ZZ%highway USING gist(way) WHERE (\n    highway in ('motorway','motorway_link','trunk','trunk_link','primary','primary_link','secondary')\n    or (railway in ('rail','light_rail','narrow_gauge','funicular') and (service IS NULL OR service NOT IN ('spur', 'siding', 'yard')))\n)"],
        ["Option","CREATE INDEX %ZZ%highway_name ON %ZZ%highway USING gin(name gin_trgm_ops)"],
        ["Option","CREATE INDEX %ZZ%highway_id ON %ZZ%highway USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%highway"],
        ["All","ALTER TABLE %ZZ%highway SET (autovacuum_enabled=true)"],
        ["All", "CREATE VIEW %ZZ%json_highway AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%highway pp"]
    ],
    "polygon": [
        ["All","CREATE INDEX %ZZ%polygon_way ON %ZZ%polygon USING gist(way)"],
        ["Extended","CREATE INDEX %ZZ%polygon_way_point ON %ZZ%polygon USING gist(way_point)"],
        ["Option","CREATE INDEX %ZZ%polygon_name ON %ZZ%polygon USING gin(name gin_trgm_ops)"],
        ["Option","CREATE INDEX %ZZ%polygon_id ON %ZZ%polygon USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%polygon"],
        ["All","ALTER TABLE %ZZ%polygon SET (autovacuum_enabled=true)"],
        ["Osm2pgsql", "CREATE VIEW %ZZ%json_polygon AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%polygon pp"],
        ["Extended", "CREATE VIEW %ZZ%json_polygon AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way' - 'way_point') AS properties,way,way_point FROM %ZZ%polygon pp"],
        ["Extended", "CREATE VIEW %ZZ%polygon_way_point AS SELECT %RR%, way_point as way from %ZZ%polygon"]
    ],
    "building": [
        ["All","CREATE INDEX %ZZ%building_way ON %ZZ%building USING gist(way)"],
        ["All","CREATE INDEX %ZZ%building_way_point ON %ZZ%building USING gist(way_point)"],
        ["Option","CREATE INDEX %ZZ%building_id ON %ZZ%building USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%building"],
        ["All","ALTER TABLE %ZZ%building SET (autovacuum_enabled=true)"],
        ["Osm2pgsql", "CREATE VIEW %ZZ%json_building AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%building pp"],
        ["Extended", "CREATE VIEW %ZZ%json_building AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way' - 'way_point') AS properties,way,way_point FROM %ZZ%building pp"]
    ],
    "boundary": [
        ["All","CREATE INDEX %ZZ%boundary_way ON %ZZ%boundary USING gist(way)"],
        ["All","CREATE INDEX %ZZ%boundary_way_exterior ON %ZZ%boundary USING gist(way_exterior)"],
        ["All","CREATE INDEX %ZZ%boundary_way_point ON %ZZ%boundary USING gist(way_point)"],
        ["Option","CREATE INDEX %ZZ%boundary_name ON %ZZ%boundary USING gin(name gin_trgm_ops)"],
        ["Option","CREATE INDEX %ZZ%boundary_id ON %ZZ%boundary USING btree(osm_id)"],
        ["All","VACUUM ANALYZE %ZZ%boundary"],
        ["All","ALTER TABLE %ZZ%boundary SET (autovacuum_enabled=true)"],
        ["Osm2pgsql", "CREATE VIEW %ZZ%json_boundary AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way') AS properties,way FROM %ZZ%boundary pp"],
        ["Extended", "CREATE VIEW %ZZ%json_boundary AS SELECT osm_id,jsonb_strip_nulls(row_to_json(pp)::jsonb - 'osm_id' - 'way' - 'way_point' - 'way_exterior') AS properties,way,way_point,way_exterior FROM %ZZ%boundary pp"],
        ["Extended", "CREATE VIEW %ZZ%boundary_exterior AS SELECT %RR%, way_exterior as way from %ZZ%boundary"]
    ]
}
"#;

const PLANET_OSM_QUERIES: &str = r#"[
["All","drop view if exists planet_osm_point"],
["All","drop view if exists planet_osm_line"],
["All","drop view if exists planet_osm_polygon"],
["All","drop table if exists planet_osm_roads"],
["Extended", "drop view if exists planet_osm_highway"],
["Extended", "drop view if exists planet_osm_building"],
["Extended","drop view if exists planet_osm_boundary"],
["Extended","drop view if exists planet_osm_polygon_point"],
["All","create view planet_osm_point as (select * from %ZZ%point)"],

["Extended","create view planet_osm_line as (select * from %ZZ%line union all select * from %ZZ%highway)"],
["Osm2pgsql","create view planet_osm_line as select * from %ZZ%line"],

["Extended","create table planet_osm_roads as (SELECT osm_id,tile,quadtree,name,ref,admin_level,highway,railway,boundary,service,tunnel,bridge,z_order,covered,surface, minzoom, way FROM %ZZ%highway WHERE highway in ('secondary','secondary_link','primary','primary_link','trunk','trunk_link','motorway','motorway_link') OR railway is not null UNION ALL SELECT osm_id,tile,quadtree,name,null as ref, admin_level,null as highway, null as railway, boundary, null as service, null as tunnel,null as bridge, 0  as z_order,null as covered,null as surface,minzoom, way_exterior as way FROM %ZZ%boundary WHERE osm_id<0 and boundary='administrative')"],
["Osm2pgsql","create table planet_osm_roads as (SELECT osm_id,tile,quadtree,name,ref,admin_level,highway,railway,boundary,service,tunnel,bridge,z_order,covered,surface, minzoom, way FROM %ZZ%line WHERE highway in ('secondary','secondary_link','primary','primary_link','trunk','trunk_link','motorway','motorway_link') OR railway is not null UNION ALL SELECT osm_id,tile,quadtree,name,null as ref, admin_level,null as highway, null as railway, boundary, null as service, null as tunnel,null as bridge, 0  as z_order,null as covered,null as surface,minzoom, way FROM %ZZ%polygon WHERE osm_id<0 and boundary='administrative')"],

["All","create index planet_osm_roads_way_admin on planet_osm_roads using gist(way) where (osm_id < 0 and boundary='administrative')"],
["All","create index planet_osm_roads_way_highway on planet_osm_roads using gist(way) where (highway in ('secondary','secondary_link','primary','primary_link','trunk','trunk_link','motorway','motorway_link') OR railway is not null)"],
["All","vacuum analyze planet_osm_roads"],
["Extended","create view planet_osm_highway as (select * from %ZZ%highway)"],
["Extended","create view planet_osm_building as (select * from %ZZ%building)"],
["Extended","create view planet_osm_boundary as (select * from %ZZ%boundary)"],
["Extended","create view planet_osm_polygon_point as select * from %ZZ%polygon_point"]
]
"#;


fn make_column_list(spec: &TableSpec, exclude_geom: bool) -> std::io::Result<String> {
    let mut cols = Vec::new();
    for (n, _, t) in &spec.columns {
        if !(exclude_geom && is_geom_columntype(t)) {
            cols.push(format!("\"{}\"",n));
        }
    }
    Ok(cols.join(", "))
}

fn make_tables_lowzoom(spec: &Vec<TableSpec>, prfx: Option<&str>, new_prfx: &str, minzoom: i64, simplify: Option<f64>) -> std::io::Result<Vec<String>> {
    
    let mut res = Vec::new();
    let prfx = match prfx {
        Some(p) => p,
        None => "%ZZ%"
    };
    
    for t in spec {
        let orig_tab = format!("{}{}", prfx, t.name);
        let new_tab = format!("{}{}", new_prfx, t.name);
        res.push(format!("DROP TABLE {} IF EXISTS CASCADE", new_tab));
        
        let cols = 
            match simplify {
                None => String::from("*"),
                Some(s) => {
        
                    let mut cols = Vec::new();
                    
                    for (n,_,ty) in &t.columns {
                        cols.push(
                            match ty {
                                ColumnType::PolygonGeometry | ColumnType::Geometry =>
                                    format!("ST_SIMPLIFY({}, {}) as {}", n, s, n),
                                _ => n.clone()
                            }
                        );
                    }
                    cols.join(", ")
                }
            };
        
        res.push(format!("CREATE TABLE {} AS SELECT {} FROM {} WHERE minzoom <= {}", new_tab, cols, orig_tab, minzoom));
        
        for (n,_,ty) in &t.columns {
            if is_geom_columntype(ty) {
                res.push(format!("CREATE INDEX {}_{} ON {} USING gist({}) WHERE {} IS NOT NULL", new_tab, n, new_tab, n, n));
            }
        }
        
        res.push(format!("CREATE INDEX {}_osm_id ON {} USING btree(osm_id)", new_tab, new_tab));
    }
    
    Ok(res)
}

fn make_views_lowzoom(spec: &Vec<TableSpec>, prfx: Option<&str>, new_prfx: &str, minzoom: i64) -> std::io::Result<Vec<String>> {
    
    let mut res = Vec::new();
    let prfx = match prfx {
        Some(p) => p,
        None => "%ZZ%"
    };
    
    for t in spec {
        let orig_tab = format!("{}{}", prfx, t.name);
        let new_tab = format!("{}{}", new_prfx, t.name);
        res.push(format!("DROP VIEW {} IF EXISTS CASCADE", new_tab));
        
        res.push(format!("CREATE VIEW {} AS SELECT * FROM {} WHERE minzoom <= {}", new_tab, orig_tab, minzoom));
        
        for (n,_,ty) in &t.columns {
            if is_geom_columntype(ty) {
                res.push(format!("CREATE INDEX {}_{} ON {} USING gist({}) WHERE {} IS NOT NULL", new_tab, n, new_tab, n, n));
            }
        }
        
        res.push(format!("CREATE INDEX {}_osm_id ON {} USING btree(osm_id)", new_tab, new_tab));
    }
    
    Ok(res)
}

pub fn make_createtable(spec: &TableSpec, prfx: Option<&str>) -> std::io::Result<String> {
    let mut cols = Vec::new();
    for (n, _, t) in &spec.columns {
        cols.push(format!("\"{}\" {}", n, type_str(t)));
    }

    let p = match prfx {
        None => "%ZZ%",
        Some(p) => p,
    };
    Ok(format!(
        "CREATE TABLE {}{} ({})",
        p,
        spec.name,
        cols.join(", ")
    ))
}

fn make_point_spec(
    with_quadtree: bool,
    tag_cols: &Vec<String>,
    with_other_tags: bool,
    with_minzoom: bool,
) -> Vec<(String, ColumnSource, ColumnType)> {
    let mut res = Vec::new();
    res.push((
        String::from("osm_id"),
        ColumnSource::OsmId,
        ColumnType::BigInteger,
    ));
    if with_quadtree {
        res.push((
            String::from("quadtree"),
            ColumnSource::ObjectQuadtree,
            ColumnType::BigInteger,
        ));
        res.push((
            String::from("tile"),
            ColumnSource::BlockQuadtree,
            ColumnType::BigInteger,
        ));
    }

    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }

    if with_other_tags {
        res.push((
            String::from("tags"),
            ColumnSource::OtherTags,
            ColumnType::Hstore,
        ));
    }
    res.push((
        String::from("layer"),
        ColumnSource::Layer,
        ColumnType::BigInteger,
    ));
    if with_minzoom {
        res.push((
            String::from("minzoom"),
            ColumnSource::MinZoom,
            ColumnType::BigInteger,
        ));
    }
    res.push((
        String::from("way"),
        ColumnSource::Geometry,
        ColumnType::PointGeometry,
    ));

    res
}

fn make_linestring_spec(
    with_quadtree: bool,
    tag_cols: &Vec<String>,
    with_other_tags: bool,
    with_minzoom: bool,
    with_length: bool,
) -> Vec<(String, ColumnSource, ColumnType)> {
    let mut res = Vec::new();
    res.push((
        String::from("osm_id"),
        ColumnSource::OsmId,
        ColumnType::BigInteger,
    ));
    if with_quadtree {
        res.push((
            String::from("quadtree"),
            ColumnSource::ObjectQuadtree,
            ColumnType::BigInteger,
        ));
        res.push((
            String::from("tile"),
            ColumnSource::BlockQuadtree,
            ColumnType::BigInteger,
        ));
    }

    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }

    if with_other_tags {
        res.push((
            String::from("tags"),
            ColumnSource::OtherTags,
            ColumnType::Hstore,
        ));
    }
    res.push((
        String::from("layer"),
        ColumnSource::Layer,
        ColumnType::BigInteger,
    ));
    res.push((
        String::from("z_order"),
        ColumnSource::ZOrder,
        ColumnType::BigInteger,
    ));
    if with_length {
        res.push((
            String::from("length"),
            ColumnSource::Length,
            ColumnType::Double,
        ));
    }

    if with_minzoom {
        res.push((
            String::from("minzoom"),
            ColumnSource::MinZoom,
            ColumnType::BigInteger,
        ));
    }
    res.push((
        String::from("way"),
        ColumnSource::Geometry,
        ColumnType::LineGeometry,
    ));

    res
}

fn make_polygon_spec(
    with_quadtree: bool,
    tag_cols: &Vec<String>,
    with_other_tags: bool,
    with_point_geom: bool,
    with_boundary_geom: bool,
    with_minzoom: bool,
) -> Vec<(String, ColumnSource, ColumnType)> {
    let mut res = Vec::new();
    res.push((
        String::from("osm_id"),
        ColumnSource::OsmId,
        ColumnType::BigInteger,
    ));
    if with_quadtree {
        res.push((
            String::from("quadtree"),
            ColumnSource::ObjectQuadtree,
            ColumnType::BigInteger,
        ));
        res.push((
            String::from("tile"),
            ColumnSource::BlockQuadtree,
            ColumnType::BigInteger,
        ));
    }

    for t in tag_cols {
        res.push((t.clone(), ColumnSource::Tag, ColumnType::Text));
    }

    if with_other_tags {
        res.push((
            String::from("tags"),
            ColumnSource::OtherTags,
            ColumnType::Hstore,
        ));
    }
    res.push((
        String::from("layer"),
        ColumnSource::Layer,
        ColumnType::BigInteger,
    ));
    res.push((
        String::from("z_order"),
        ColumnSource::ZOrder,
        ColumnType::BigInteger,
    ));

    res.push((
        String::from("way_area"),
        ColumnSource::Area,
        ColumnType::Double,
    ));

    if with_minzoom {
        res.push((
            String::from("minzoom"),
            ColumnSource::MinZoom,
            ColumnType::BigInteger,
        ));
    }

    res.push((
        String::from("way"),
        ColumnSource::Geometry,
        ColumnType::Geometry,
    ));
    if with_point_geom {
        res.push((
            String::from("way_point"),
            ColumnSource::RepresentativePointGeometry,
            ColumnType::PointGeometry,
        ));
    }
    if with_boundary_geom {
        res.push((
            String::from("way_exterior"),
            ColumnSource::BoundaryLineGeometry,
            ColumnType::Geometry,
        ));
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
        }
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
        for (l, _) in &style.parent_tags {
            point_tag_cols.push(l.clone());
        }

        for l in &style.relation_tag_spec {
            line_tag_cols.push(l.target_key.clone());
        }
    }

    res.push(TableSpec::new(
        "point",
        make_point_spec(extended, &point_tag_cols, true, extended),
    ));
    res.push(TableSpec::new(
        "line",
        make_linestring_spec(extended, &line_tag_cols, true, extended, extended),
    ));
    res.push(TableSpec::new(
        "polygon",
        make_polygon_spec(extended, &poly_tag_cols, true, extended, false, extended),
    ));
    if extended {
        res.push(TableSpec::new(
            "highway",
            make_linestring_spec(true, &line_tag_cols, true, true, true),
        ));
        res.push(TableSpec::new(
            "building",
            make_polygon_spec(true, &line_tag_cols, true, true, false, true),
        ));
        res.push(TableSpec::new(
            "boundary",
            make_polygon_spec(true, &poly_tag_cols, true, true, true, true),
        ));
    }

    res
}


