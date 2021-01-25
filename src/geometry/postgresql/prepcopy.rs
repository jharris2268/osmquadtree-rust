use crate::geometry::postgresql::{TableSpec,ColumnSource,ColumnType};
use crate::geometry::wkb::{write_uint16,write_int32,write_int64,/*write_uint32,write_uint64,*/write_f64};
use crate::geometry::{GeometryBlock,PointGeometry,LinestringGeometry,SimplePolygonGeometry,ComplicatedPolygonGeometry};
use crate::elements::{WithId,WithTags,WithQuadtree,Tag,Quadtree};

use std::io::{Result,Error,ErrorKind,Write};
use std::collections::BTreeMap;

//use postgres::types::{ToSql};
//use postgres::binary_copy::BinaryCopyInWriter;
//use postgis::ewkb::{AsEwkbPoint,EwkbWrite};

pub struct PrepTable {
    osm_id_col: Option<usize>,
    object_quadtree_col: Option<usize>,
    block_quadtree_col: Option<usize>,
    
    tag_cols: BTreeMap<String,usize>,
    other_tags_col: Option<usize>,
    
    minzoom_col: Option<usize>,
    layer_col: Option<usize>,
    z_order_col: Option<usize>,
    length_col: Option<usize>,
    area_col: Option<usize>,
    geometry_col: Option<(ColumnType,usize)>,
    representative_point_geometry_col: Option<usize>,
    boundary_line_geometry: Option<usize>,

    num_cols: usize,
    
}

fn check_type(i: usize, n: &str, src: &ColumnSource, typ: &ColumnType, expected_type: &ColumnType) -> Result<()> {
    if typ==expected_type {
        Ok(())
    } else {
        Err(Error::new(ErrorKind::Other, format!("wrong column type {} {} {:?} {:?} != {:?}", i,n,src,typ,expected_type)))
    }
}


pub enum CopyValue {
    Null,
    Integer(i64),
    Double(f64),
    Text(String),
    HStore(Vec<Tag>),
    Wkb(Vec<u8>),
}
    

/*
fn pack_bigint(_i: i64) -> Result<Vec<u8>> {
    return Err(Error::new(ErrorKind::Other, "not impl"));
}

fn pack_quadtree(q: &Quadtree) -> Result<Vec<u8>> {
    return pack_string(&q.to_string());
}

fn pack_string(_s: &str) -> Result<Vec<u8>> {
    return Err(Error::new(ErrorKind::Other, "not impl"));
}

fn pack_hstore_entry(_k: &str, _v: &str) -> Result<Vec<u8>> {
    return Err(Error::new(ErrorKind::Other, "not impl"));
}

fn pack_hstore(_: Vec<Vec<u8>>) -> Result<Vec<u8>> {
    return Err(Error::new(ErrorKind::Other, "not impl"));
}
*/



impl PrepTable {
    pub fn new() -> PrepTable {
        PrepTable{
            osm_id_col: None, object_quadtree_col: None, block_quadtree_col: None,
            tag_cols: BTreeMap::new(), other_tags_col: None,
            minzoom_col: None, layer_col: None, z_order_col: None,
            length_col: None, area_col: None,
            geometry_col: None, representative_point_geometry_col: None, boundary_line_geometry: None,
            num_cols: 0
        }
    }
                
    
    pub fn from_tablespec(spec: &TableSpec) -> Result<PrepTable> {
        let mut pt = PrepTable::new();
        
        pt.num_cols = spec.columns.len();
        for (i,(n, src, typ)) in spec.columns.iter().enumerate() {
            match src {
                ColumnSource::OsmId => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.osm_id_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<i64>));
                },
                ColumnSource::ObjectQuadtree => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.object_quadtree_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<String>));
                },
                ColumnSource::BlockQuadtree => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.block_quadtree_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<String>));
                },
                
                ColumnSource::Tag => {
                    check_type(i,n,src,typ, &ColumnType::Text)?;
                    pt.tag_cols.insert(n.clone(), i);
                    //pt.null_row.push(Box::new(&None: Option<String>));
                },
                ColumnSource::OtherTags => {
                    check_type(i,n,src,typ, &ColumnType::Hstore)?;
                    pt.other_tags_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<HashMap<(String,Option<String>)>>));
                },
                
                ColumnSource::MinZoom => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.minzoom_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<i64>));
                },
                ColumnSource::Layer => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.layer_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<i64>));
                },
                ColumnSource::ZOrder => {
                    check_type(i,n,src,typ, &ColumnType::BigInteger)?;
                    pt.z_order_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<i64>));
                },
                ColumnSource::Length => {
                    check_type(i,n,src,typ, &ColumnType::Double)?;
                    pt.length_col = Some(i);
                },
                ColumnSource::Area => {
                    check_type(i,n,src,typ, &ColumnType::Double)?;
                    pt.area_col = Some(i);
                },
                    ColumnSource::Geometry => {
                    match typ {
                        ColumnType::PointGeometry | ColumnType::LineGeometry | ColumnType::PolygonGeometry | ColumnType::Geometry => {},
                        _ => { check_type(i,n,src,typ,&ColumnType::PointGeometry)?; },
                    }
                    
                    pt.geometry_col = Some((typ.clone(),i));
                    //pt.null_row.push(Box::new(None: Option<postgis::ewkb::Geometry>));
                },
                ColumnSource::RepresentativePointGeometry => {
                    check_type(i,n,src,typ, &ColumnType::PointGeometry)?;
                    pt.representative_point_geometry_col = Some(i);
                    //pt.null_row.push(Box::new(None: Option<postgis::ewkb::Geometry>));
                },
                ColumnSource::BoundaryLineGeometry => {
                    check_type(i,n,src,typ, &ColumnType::LineGeometry)?;
                    pt.boundary_line_geometry = Some(i);
                    //pt.null_row.push(Box::new(None: Option<postgis::ewkb::Geometry>));
                },
            }
        }
        Ok(pt)
    }
    
    pub fn pack_common<O: WithId+WithTags+WithQuadtree>(&self, o: &O, tile: &Quadtree, flip_id: bool) -> Result<Vec<CopyValue>> {
        let mut res = Vec::with_capacity(self.num_cols);
        for _ in 0..self.num_cols {
            res.push(CopyValue::Null);
        }
        
        
        let mut other_tags = Vec::new();
        match self.osm_id_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Integer(
                    if flip_id {
                        -o.get_id()
                    } else {
                        o.get_id()
                    }
                ); }
        }
        
        match self.object_quadtree_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Integer(o.get_quadtree().as_int()); }
        }
        match self.block_quadtree_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Integer(tile.as_int()); }
        }
        
        for t in o.get_tags() {
            match self.tag_cols.get(&t.key) {
                None => {
                    if !self.other_tags_col.is_none() {
                        other_tags.push(t.clone());
                    }
                },
                Some(i) => { res[*i] = CopyValue::Text(t.val.clone()); }
            }
        }
        match self.other_tags_col {
            None => {},
            Some(i) => {
                if !other_tags.is_empty() {
                    res[i] = CopyValue::HStore(other_tags);
                }
            }
        }
        
        Ok(res)
    }
    
    pub fn pack_point_geometry(&self, pg: &PointGeometry, tile: &Quadtree) -> Result<Vec<CopyValue>> {
        
        let mut res = self.pack_common(pg, tile, false)?;
        
        
        match &self.geometry_col {
            None => {},
            Some((typ,i)) => {
                if *typ != ColumnType::PointGeometry {
                    return Err(Error::new(ErrorKind::Other, format!("{:?} wrong type for PointGeometry", typ)));
                }
                let d = pg.to_wkb(true,true)?;
                res[*i] = CopyValue::Wkb(d);
                
            }
            
            
            
        }
                
        
        match self.layer_col {
            None => {},
            Some(i) => {
                match &pg.layer {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.minzoom_col {
            None => {},
            Some(i) => {
                match &pg.minzoom {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
            
            
        Ok(res)
    }
        
    pub fn pack_linestring_geometry(&self, pg: &LinestringGeometry, tile: &Quadtree) -> Result<Vec<CopyValue>>  {
        let mut res = self.pack_common(pg, tile, false)?;
        
        
        match &self.geometry_col {
            None => {},
            Some((typ,i)) => {
                if *typ != ColumnType::LineGeometry {
                    return Err(Error::new(ErrorKind::Other, format!("{:?} wrong type for LinestringGeometry", typ)));
                }
                
                let d = pg.to_wkb(true,true)?;
                res[*i] = CopyValue::Wkb(d);
                
            }
            
            
            
        }
                
        
        match self.layer_col {
            None => {},
            Some(i) => {
                match &pg.layer {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.z_order_col {
            None => {},
            Some(i) => {
                match &pg.z_order {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.length_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Double(pg.length); }
        }
        match self.minzoom_col {
            None => {},
            Some(i) => {
                match &pg.minzoom {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
            
            
        Ok(res)
    }
    
    pub fn pack_simple_polygon_geometry(&self, pg: &SimplePolygonGeometry, tile: &Quadtree) -> Result<Vec<CopyValue>>  {
        let mut res = self.pack_common(pg, tile, false)?;
        
        
        match &self.geometry_col {
            None => {},
            Some((typ,i)) => {
                if !((*typ == ColumnType::Geometry) || (*typ == ColumnType::PolygonGeometry)) {
                    return Err(Error::new(ErrorKind::Other, format!("{:?} wrong type for SimplePolygonGeometry", typ)));
                }
                let d = pg.to_wkb(true,true)?;
                res[*i] = CopyValue::Wkb(d);
            }
            
            
            
        }
                
        
        match self.layer_col {
            None => {},
            Some(i) => {
                match &pg.layer {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.z_order_col {
            None => {},
            Some(i) => {
                match &pg.z_order {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.area_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Double(pg.area); }
        }
        match self.minzoom_col {
            None => {},
            Some(i) => {
                match &pg.minzoom {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
            
            
        Ok(res)
    }
    pub fn pack_complicated_polygon_geometry(&self, pg: &ComplicatedPolygonGeometry, tile: &Quadtree) -> Result<Vec<CopyValue>>  {
        let mut res = self.pack_common(pg, tile, true)?;
        
        
        match &self.geometry_col {
            None => {},
            Some((typ,i)) => {
                if *typ != ColumnType::Geometry {
                    return Err(Error::new(ErrorKind::Other, format!("{:?} wrong type for ComplicatedPolygonGeometry", typ)));
                }
                let d = pg.to_wkb(true,true)?;
                res[*i] = CopyValue::Wkb(d);
            }
            
            
            
        }
                
        
        match self.layer_col {
            None => {},
            Some(i) => {
                match &pg.layer {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.z_order_col {
            None => {},
            Some(i) => {
                match &pg.z_order {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
        match self.area_col {
            None => {},
            Some(i) => { res[i] = CopyValue::Double(pg.area); }
        }
        match self.minzoom_col {
            None => {},
            Some(i) => {
                match &pg.minzoom {
                    None => {},
                    Some(l) => { res[i] = CopyValue::Integer(*l); }
                }
            }
        }
            
            
        Ok(res)
    }
    
    
    
        
}



pub enum GeometryType<'a> {
    Point(&'a PointGeometry),
    Linestring(&'a LinestringGeometry),
    SimplePolygon(&'a SimplePolygonGeometry),
    ComplicatedPolygon(&'a ComplicatedPolygonGeometry)
}


fn pack_hstore(tt: &[Tag]) -> Result<Vec<u8>> {
    let mut w = Vec::new();
    
    write_int32(&mut w, tt.len() as i32)?;
    for t in tt {
        write_text(&mut w, &t.key)?;
        write_text(&mut w, &t.val)?;
    }
    Ok(w)
}

fn write_text<W: Write>(w: &mut W, t: &str) -> Result<()> {
    write_bytes(w, &t.as_bytes())
}

fn write_bytes<W: Write>(w: &mut W, b: &[u8]) -> Result<()> {
    write_int32(w, b.len() as i32)?;
    w.write_all(&b)
}
    
    

fn pack_all<W: Write>(w: &mut W, row: &Vec<CopyValue>) -> Result<()> {
    
    write_uint16(w, row.len())?;
    
    for r in row {
        match r {
            CopyValue::Null => { write_int32(w, -1)?; },
            CopyValue::Integer(i) => { write_int32(w, 8)?; write_int64(w, *i)?; },
            CopyValue::Double(d) => { write_int32(w, 8)?; write_f64(w, *d)?; },
            CopyValue::Text(t) => { write_text(w, &t)?; },
            CopyValue::HStore(tt) => {
                let hh = pack_hstore(&tt)?;
                write_bytes(w, &hh)?;
            },
            CopyValue::Wkb(wkb) => {
                write_bytes(w, &wkb)?;
            },
        }
    }
    
    Ok(())
}
            
            
        
        
    
    

pub fn pack_geometry_block<W: Write,A: Fn(&GeometryType)->Vec<usize> + ?Sized>(packers: &Vec<PrepTable>, outs: &mut Vec<W>, alloc_func: &A, bl: &GeometryBlock) -> Result<usize> {
    let mut count=0;
    for obj in &bl.points {
        for i in alloc_func(&GeometryType::Point(obj)) {
            let tt = packers[i].pack_point_geometry(obj, &bl.quadtree)?;
            pack_all(&mut outs[i], &tt)?;
                
            count+=1;
        
        }
    }
    
    for obj in &bl.linestrings {
        for i in alloc_func(&GeometryType::Linestring(obj)) {
            let tt = packers[i].pack_linestring_geometry(obj, &bl.quadtree)?;
            pack_all(&mut outs[i], &tt)?;
            count+=1;
        }
    }
    
    for obj in &bl.simple_polygons {
        for i in alloc_func(&GeometryType::SimplePolygon(obj)) {
            
            let tt = packers[i].pack_simple_polygon_geometry(obj, &bl.quadtree)?;
            pack_all(&mut outs[i], &tt)?;
            count+=1;
        }
    }
    
    for obj in &bl.complicated_polygons {
        for i in alloc_func(&GeometryType::ComplicatedPolygon(obj)) {
            let tt = packers[i].pack_complicated_polygon_geometry(obj, &bl.quadtree)?;
            pack_all(&mut outs[i], &tt)?;
            count+=1;
        }
    }
    Ok(count)
}
    

            

    
