use crate::elements::Tag;
use crate::geometry::postgresql::GeometryType;
use crate::geometry::postgresql::{make_table_spec, TableSpec};
use crate::geometry::GeometryStyle;
use std::sync::Arc;
pub enum PostgresqlConnection {
    Null,
    Connection((String, String, bool)),
    CopyFilePrfx(String),
    CopyFileBlob(String),
}

pub type AllocFunc = Arc<dyn Fn(&GeometryType) -> Vec<usize> + Send + Sync>;

pub struct PostgresqlOptions {
    pub connection: PostgresqlConnection,
    pub table_alloc: AllocFunc,
    pub table_spec: Vec<TableSpec>,
    pub extended: bool,
    pub planet_osm_views: bool,
    pub lowzoom: Option<Vec<(String,i64,bool)>>
}

impl PostgresqlOptions {
    pub fn osm2pgsql(conn: PostgresqlConnection, style: &GeometryStyle) -> PostgresqlOptions {
        PostgresqlOptions {
            connection: conn,
            table_alloc: Arc::new(osm2pgsql_alloc),
            table_spec: make_table_spec(style, false),
            extended: false,
            planet_osm_views: false,
            lowzoom: None
        }
    }

    pub fn extended(conn: PostgresqlConnection, style: &GeometryStyle) -> PostgresqlOptions {
        PostgresqlOptions {
            connection: conn,
            table_alloc: Arc::new(extended_alloc),
            table_spec: make_table_spec(style, true),
            extended: true,
            planet_osm_views: false,
            lowzoom: None
        }
    }

    pub fn other(
        conn: PostgresqlConnection,
        alloc_func: AllocFunc,
        table_spec: Vec<TableSpec>,
        
        
    ) -> PostgresqlOptions {
        PostgresqlOptions {
            connection: conn,
            table_alloc: alloc_func,
            table_spec: table_spec,
            extended: false,
            planet_osm_views: false,
            lowzoom: None
        }
    }
}

fn osm2pgsql_alloc(g: &GeometryType) -> Vec<usize> {
    match g {
        GeometryType::Point(_) => vec![0],
        GeometryType::Linestring(_) => vec![1],
        GeometryType::SimplePolygon(_) => vec![2],
        GeometryType::ComplicatedPolygon(_) => vec![2],
    }
}

fn is_building(tt: &[Tag]) -> bool {
    for t in tt {
        if t.key == "building" {
            if t.val != "no" {
                return true;
            } else {
                return false;
            }
        }
    }

    false
}

fn is_boundary(tt: &[Tag]) -> bool {
    for t in tt {
        if t.key == "type" {
            if t.val == "boundary" {
                return true;
            } else {
                return false;
            }
        }
    }

    false
}

fn extended_alloc(g: &GeometryType) -> Vec<usize> {
    match g {
        GeometryType::Point(_) => vec![0],
        GeometryType::Linestring(l) => match l.z_order {
            None => vec![1],
            Some(_) => vec![3],
        },
        GeometryType::SimplePolygon(sp) => {
            if is_building(&sp.tags) {
                vec![4]
            } else {
                vec![2]
            }
        }

        GeometryType::ComplicatedPolygon(cp) => {
            if is_building(&cp.tags) {
                vec![4]
            } else {
                if is_boundary(&cp.tags) {
                    vec![2, 5]
                } else {
                    vec![2]
                }
            }
        }
    }
}
