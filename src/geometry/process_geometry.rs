pub use crate::geometry::postgresql::{PostgresqlOptions,make_write_postgresql_geometry};

use crate::geometry::{WorkingBlock,Timings,CollectWayNodes,OtherData,GeometryStyle,GeometryBlock,LinestringGeometry,PointGeometry,SimplePolygonGeometry};
use crate::geometry::multipolygons::ProcessMultiPolygons;
use crate::geometry::position::{calc_ring_area, calc_line_length};
use crate::geometry::addparenttag::AddParentTag;
use crate::geometry::relationtags::AddRelationTags;
use crate::geometry::minzoom::{MinZoomSpec,FindMinZoom};
use crate::geometry::elements::GeoJsonable;
use crate::geometry::pack_geometry::pack_geometry_block;

use crate::utils::{LogTimes,MergeTimings,ReplaceNoneWithTimings,parse_timestamp,ThreadTimer,CallAll};
use crate::callback::{Callback,CallbackMerge,CallbackSync,CallFinish};
use crate::pbfformat::writefile::WriteFile;
use crate::pbfformat::header_block::HeaderType;
use crate::update::get_file_locs;
use crate::pbfformat::read_file_block::{read_all_blocks_parallel_with_progbar,FileBlock,pack_file_block};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all;
use crate::mergechanges::inmem::read_filter;
use crate::elements::Quadtree;
use std::io::{Result,/*Error,ErrorKind*/};
use std::sync::Arc;
use std::collections::BTreeMap;
use serde_json::{Map,Value,json};


fn pack_geom(bl: GeometryBlock) -> (i64, Vec<u8>) {
    let p = pack_geometry_block(&bl).expect("!");
    let q = pack_file_block("OSMData", &p, true).expect("?");
    
    (bl.quadtree.as_int(), q)
}

struct WrapWriteFile(WriteFile);

impl CallFinish for WrapWriteFile {
    type CallType = (i64, Vec<u8>);
    type ReturnType = Timings;
    
    fn call(&mut self, p: Self::CallType) {
        self.0.call(vec![p]);
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let (t,_) = self.0.finish()?;
        let mut tms = Timings::new();
        tms.add("WriteFile", t);
        Ok(tms)
    }
}



struct StoreBlocks {
    tiles: BTreeMap<Quadtree,GeometryBlock>,
    nt: usize
}

impl StoreBlocks {
    pub fn new() -> StoreBlocks {
        StoreBlocks{tiles: BTreeMap::new(), nt: 0}
    }
}

impl CallFinish for StoreBlocks {
    type CallType = GeometryBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, gb: GeometryBlock) {
        self.nt+=1;
        if gb.len() == 0 {
            return;
        }
        
        match self.tiles.get_mut(&gb.quadtree) {
            Some(tl) => { tl.extend(gb); },
            None => { self.tiles.insert(gb.quadtree.clone(), gb); }
        }
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = Timings::new();
        for (_,t) in self.tiles.iter_mut() {
            t.sort();
        }
        tms.add_other("StoreBlocks", OtherData::Messages(vec![format!("recieved {} blocks, returning {} blocks", self.nt,self.tiles.len())]));
        tms.add_other("StoreBlocks", OtherData::GeometryBlocks(std::mem::take(&mut self.tiles)));
        Ok(tms)
    }
}

struct CollectWorkingTiles<T: ?Sized> {
    
    npt: usize,
    nls: usize,
    nsp: usize,
    ncp: usize,
    out: Option<Box<T>>
}

impl<T> CollectWorkingTiles<T>
    where T: CallFinish<CallType=GeometryBlock, ReturnType=Timings> + ?Sized
{
    pub fn new(out: Option<Box<T>>) -> CollectWorkingTiles<T> {
        CollectWorkingTiles{npt:0,nls:0,nsp:0,ncp:0,out:out}
    }
}

impl<T> CallFinish for CollectWorkingTiles<T>
    where T: CallFinish<CallType=GeometryBlock, ReturnType=Timings> + ?Sized
{
    type CallType = WorkingBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, wb: WorkingBlock) {
        self.npt += wb.geometry_block.points.len();
        self.nls += wb.geometry_block.linestrings.len();
        self.nsp += wb.geometry_block.simple_polygons.len();
        self.ncp += wb.geometry_block.complicated_polygons.len();
        
        match self.out.as_mut() {
            None => {},
            Some(out) => {out.call(wb.geometry_block);},
        }
        
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = match self.out.as_mut() {
            None => Timings::new(),
            Some(out) => out.finish()?
        };
        
        let m = format!("have {} points, {} linestrings, {} simple_polygons and {} complicated_polygons", self.npt,self.nls, self.nsp,self.ncp);
        tms.add_other("CollectWorkingTiles", OtherData::Messages(vec![m]));
        
        Ok(tms)
    }
}


struct MakeGeometries<T: ?Sized> {
    out: Box<T>,
    style: Arc<GeometryStyle>,
    recalcquadtree: bool,
    tm: f64,
    npt: usize,
    nls: usize,
    nsp: usize,
}

impl<T> MakeGeometries<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings> + ?Sized
{
    pub fn new(out: Box<T>, style: Arc<GeometryStyle>, recalcquadtree: bool) -> MakeGeometries<T> {
        MakeGeometries{out:out,style:style,recalcquadtree:recalcquadtree,tm:0.0,npt:0,nls:0,nsp:0}
    }
    
    fn process_block(&mut self, bl: &mut WorkingBlock) {
        
        std::mem::take(&mut bl.pending_relations);
        
        for n in std::mem::take(&mut bl.pending_nodes) {
            match self.style.process_node(&n.tags) {
                Err(_) => {},
                Ok((t,l)) => {
                    bl.geometry_block.points.push(PointGeometry::from_node(n, t, l));
                    self.npt+=1;
                }
            }
        }
        
        for (w,ll) in std::mem::take(&mut bl.pending_ways) {
            
            
            let is_ring = w.refs[0] == w.refs[w.refs.len()-1];
            
            match self.style.process_way(&w.tags, is_ring) {
                Err(_) => {},
                Ok((is_poly,tgs,zorder,layer)) => {
                    if is_poly {
                        let area = calc_ring_area(&ll);//.iter().collect::<Vec<&LonLat>>());
                        let reversed = area < 0.0;
                        bl.geometry_block.simple_polygons.push(SimplePolygonGeometry::from_way(w, ll, tgs, f64::abs(area), layer, None, reversed)); //no zorder for polys
                        self.nsp+=1;
                    } else {
                        let length = calc_line_length(&ll);//.iter().collect::<Vec<&LonLat>>());
                        bl.geometry_block.linestrings.push(LinestringGeometry::from_way(w, ll, tgs, length, layer, zorder));
                        self.nls+=1;
                    }
                    
                }
            }
        }
        
        if self.recalcquadtree {
            for n in bl.geometry_block.points.iter_mut() {
                n.quadtree = Quadtree::calculate_point(n.lonlat.lon, n.lonlat.lat, 18, 0.0);
            }
            
            for l in bl.geometry_block.linestrings.iter_mut() {
                
                l.quadtree = Quadtree::calculate(&l.bounds(), 18, 0.0);
            }
            
            for sp in bl.geometry_block.simple_polygons.iter_mut() {
                
                sp.quadtree = Quadtree::calculate(&sp.bounds(), 18, 0.0);
            }
            
            for sp in bl.geometry_block.complicated_polygons.iter_mut() {
                let bnd=sp.bounds();
                sp.quadtree = Quadtree::calculate(&bnd, 18, 0.0);
                
            }
        }
    }
    
}

impl<T> CallFinish for MakeGeometries<T>
    where T: CallFinish<CallType=WorkingBlock, ReturnType=Timings> + ?Sized
{
    type CallType=WorkingBlock;
    type ReturnType=Timings;

    fn call(&mut self, mut bl: WorkingBlock) {
        let tx=ThreadTimer::new();
        self.process_block(&mut bl);
        self.tm+=tx.since();
        self.out.call(bl);
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = self.out.finish()?;
        tms.add("MakeGeometries", self.tm);
        tms.add_other("MakeGeometries", OtherData::Messages(vec![format!("{} points, {} linestrings, {} simple polygons", self.npt,self.nls, self.nsp)]));
        Ok(tms)
    }
}
        
fn write_geojson_tiles(tiles: BTreeMap<Quadtree,GeometryBlock>, outfn: &str) -> Result<()> {
    let mut v = Vec::new();
    for (_,t) in tiles {
        v.push(t.to_geojson()?);
    }
    serde_json::to_writer(std::fs::File::create(outfn)?, &v)?;
    Ok(())
}



fn pack_feature_collection<F: GeoJsonable>(feats: &[F]) -> Result<Value> {
    let mut vv = Vec::with_capacity(feats.len());
    for f in feats {
        vv.push(f.to_geojson()?);
    }
    let mut m = Map::new();
    m.insert(String::from("type"),json!("FeatureCollection"));
    m.insert(String::from("features"), json!(vv));
    Ok(json!(m))
}

fn write_geojson_flat(tiles: BTreeMap<Quadtree,GeometryBlock>, outfn: &str) -> Result<()> {
    let mut tt = GeometryBlock::new(0,Quadtree::empty(),0);
    for (_,t) in tiles {
        tt.extend(t);
    }
    tt.sort();
    
    let mut m = Map::new();
    m.insert(String::from("points"), pack_feature_collection(&tt.points)?);
    m.insert(String::from("linestrings"), pack_feature_collection(&tt.linestrings)?);
    m.insert(String::from("simple_polygons"), pack_feature_collection(&tt.simple_polygons)?);
    m.insert(String::from("complicated_polygons"), pack_feature_collection(&tt.complicated_polygons)?);
    
    
    serde_json::to_writer(std::fs::File::create(outfn)?, &m)?;

    Ok(())
}
    
    
        
pub enum OutputType {
    None,
    Json(String),
    TiledJson(String),
    PbfFile(String),
    Postgresql(PostgresqlOptions),
}


pub fn process_geometry(prfx: &str, outfn: OutputType, filter: Option<&str>, timestamp: Option<&str>, find_minzoom: bool, style_name: Option<&str>, numchan: usize) -> Result<()> {


    let mut tx=LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;
    
    println!("bbox={}, poly={:?}", bbox, poly);
    
    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?)
    };
    
    let mut pfilelocs = get_file_locs(prfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");

    let style = match style_name {
        None => Arc::new(GeometryStyle::default()),
        Some(fname) => Arc::new(GeometryStyle::from_file(&fname)?)
    };
    tx.add("load_style");
    
    let minzoom: Option<MinZoomSpec> = if find_minzoom { Some(MinZoomSpec::default()) } else { None };
    tx.add("load_minzoom");
    
    let out: Option<Box<dyn CallFinish<CallType=GeometryBlock,ReturnType=Timings>>> = match &outfn {
        OutputType::None => None,
        OutputType::Json(_) | OutputType::TiledJson(_) => Some(Box::new(StoreBlocks::new())),
        OutputType::PbfFile(ofn) => {
            if numchan == 0 {
                let wt = Box::new(WrapWriteFile(WriteFile::with_bbox(&ofn, HeaderType::NoLocs, Some(&bbox))));
                let pt = Box::new(CallAll::new(wt, "pack_geometry", Box::new(pack_geom)));
                Some(pt)
            } else {
                let wts = CallbackSync::new(Box::new(WrapWriteFile(WriteFile::with_bbox(&ofn, HeaderType::NoLocs, Some(&bbox)))),numchan);
                let mut pps: Vec<Box<dyn CallFinish<CallType=GeometryBlock,ReturnType=Timings>>> = Vec::new();
                for wt in wts {
                    let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
                    pps.push(Box::new(Callback::new(Box::new(CallAll::new(wt2, "pack_geometry", Box::new(pack_geom))))));
                }
                Some(Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new()))))
            }
        },
        OutputType::Postgresql(options) => {
            Some(make_write_postgresql_geometry(&options, numchan)?)
        }
    };
    
    let cf = Box::new(CollectWorkingTiles::new(out));
    
    type CallFinishWorkingBlock = Box<dyn CallFinish<CallType=WorkingBlock,ReturnType=Timings>>;
    
    let pp: Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>> = if numchan == 0 {
        let fm = Box::new(FindMinZoom::new(cf, minzoom));
        let mg = Box::new(MakeGeometries::new(fm, style.clone(),true));
        let mm = Box::new(ProcessMultiPolygons::new(style.clone(), mg));
        let rt = Box::new(AddRelationTags::new(mm, style.clone()));
        let ap = Box::new(AddParentTag::new(rt, style.clone()));
        let ww = Box::new(CollectWayNodes::new(ap, style.clone()));
        make_read_primitive_blocks_combine_call_all(ww)
    } else {
        let cfb = Box::new(Callback::new(cf));
        let fm: CallFinishWorkingBlock = if find_minzoom {
            Box::new(Callback::new(Box::new(FindMinZoom::new(cfb, minzoom))))
        } else {
            cfb
        };
        let mg = Box::new(Callback::new(Box::new(MakeGeometries::new(fm, style.clone(),true))));
        let mm: CallFinishWorkingBlock = if style.multipolygons || style.boundary_relations { 
            Box::new(Callback::new(Box::new(ProcessMultiPolygons::new(style.clone(), mg))))
        } else {
            mg
        };
        let rt: CallFinishWorkingBlock = if !style.relation_tag_spec.is_empty() {
            Box::new(Callback::new(Box::new(AddRelationTags::new(mm, style.clone()))))
        } else {
            mm
        };
        let ap: CallFinishWorkingBlock = if !style.parent_tags.is_empty() {
            Box::new(Callback::new(Box::new(AddParentTag::new(rt, style.clone()))))
        } else {
            rt
        };
        
        let ww = CallbackSync::new(Box::new(CollectWayNodes::new(ap,style.clone())), numchan);
        
        let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for w in ww {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            pps.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all(w2))))
        }
        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
    };
        
    let msg = format!("process_geometry, numchan={}", numchan);
    let tm = read_all_blocks_parallel_with_progbar(&mut pfilelocs.0, &pfilelocs.1, pp, &msg, pfilelocs.2);
    
    tx.add("process_geometry");
    
    println!("{}", tm);
    let mut all_tiles = BTreeMap::new();
    for (w,x) in tm.others {
        match x {
            OtherData::Messages(mm) => {
                for m in mm {
                    println!("{}: {}", w, m);
                }
            },
            OtherData::Errors(ee) => {
                println!("{}: {} errors", w, ee.len());
            },
            OtherData::GeometryBlocks(tiles) => {
                all_tiles.extend(tiles);
                
            },
        }
    }
    tx.add("finish process_geometry");
    if !all_tiles.is_empty() {
        match outfn {
            OutputType::None | OutputType::PbfFile(_) | OutputType::Postgresql(_) => {},
            OutputType::Json(ofn) => {
                write_geojson_flat(all_tiles, &ofn)?;
                tx.add("write json");
            },
            OutputType::TiledJson(ofn) => {
                write_geojson_tiles(all_tiles, &ofn)?;
                tx.add("write json");
            } 
        }
    }
    
    println!("{}", tx);
    Ok(())

}
