use crate::geometry::{WorkingBlock,Timings,CollectWayNodes,OtherData,GeometryStyle,GeometryBlock,LinestringGeometry,PointGeometry,SimplePolygonGeometry,LonLat};
use crate::geometry::multipolygons::ProcessMultiPolygons;
use crate::geometry::position::{calc_ring_area, calc_line_length};
use crate::geometry::addparenttag::AddParentTag;
use crate::utils::{LogTimes,MergeTimings,ReplaceNoneWithTimings,parse_timestamp,ThreadTimer};
use crate::callback::{Callback,CallbackMerge,CallbackSync,CallFinish};
use crate::update::get_file_locs;
use crate::pbfformat::read_file_block::{ProgBarWrap,read_all_blocks_parallel_prog,FileBlock};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all;
use crate::mergechanges::inmem::read_filter;
use crate::elements::Quadtree;
use std::io::Result;
use std::sync::Arc;



struct CollectWorkingTiles {
    
    nn: usize,
    nw: usize,
    nl: usize,
    nr: usize,
    store: bool,
    tiles: Vec<GeometryBlock>
}

impl CollectWorkingTiles {
    pub fn new(store: bool) -> CollectWorkingTiles {
        CollectWorkingTiles{nn:0,nw:0,nl:0,nr:0,store:store,tiles:Vec::new()}
    }
}

impl CallFinish for CollectWorkingTiles {
    type CallType = WorkingBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, wb: WorkingBlock) {
        self.nn += wb.pending_nodes.len();
        self.nw += wb.pending_ways.len();
        self.nl += wb.pending_ways.iter().map(|(_,r,_)| { r.len() }).sum::<usize>(); 
        self.nr += wb.pending_relations.len();
        if self.store {
            self.tiles.push(wb.geometry_block);
        }
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = Timings::new();
        let m = format!("have {}, and {} ways (with {} locations), {} relations", self.nn,self.nw, self.nl,self.nr);
        tms.add_other("CollectWorkingTiles", OtherData::Messages(vec![m]));
        if self.store {
            tms.add_other("CollectWorkingTiles", OtherData::GeometryBlocks(std::mem::take(&mut self.tiles)));
        }
        Ok(tms)
    }
}


struct MakeGeometries<T> {
    out: Box<T>,
    style: Arc<GeometryStyle>,
    recalcquadtree: bool,
    tm: f64,
    npt: usize,
    nls: usize,
    nsp: usize,
}

impl<T> MakeGeometries<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
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
        
        for (mut w,ll,finished) in std::mem::take(&mut bl.pending_ways) {
            
            if !finished.is_empty() {
                let mut tgs=Vec::new();
                for t in std::mem::take(&mut w.tags) {
                    if !finished.contains(&t.key) {
                        tgs.push(t);
                    }
                }
                w.tags=tgs;
            }
            let is_ring = w.refs[0] == w.refs[w.refs.len()-1];
            
            match self.style.process_way(&w.tags, is_ring) {
                Err(_) => {},
                Ok((is_poly,tgs,layer,zorder)) => {
                    if is_poly {
                        let area = calc_ring_area(&ll.iter().collect::<Vec<&LonLat>>());
                        bl.geometry_block.simple_polygons.push(SimplePolygonGeometry::from_way(w, ll, tgs, area, layer, zorder));
                        self.nsp+=1;
                    } else {
                        let length = calc_line_length(&ll.iter().collect::<Vec<&LonLat>>());
                        bl.geometry_block.linestrings.push(LinestringGeometry::from_way(w, ll, tgs, length, layer, zorder));
                        self.nls+=1;
                    }
                    
                }
            }
        }
        
        if self.recalcquadtree {
            for n in bl.geometry_block.points.iter_mut() {
                n.quadtree = Quadtree::calculate_point(n.lonlat.lon, n.lonlat.lat, 18, 0.05);
            }
            
            for l in bl.geometry_block.linestrings.iter_mut() {
                
                l.quadtree = Quadtree::calculate(&l.bounds(), 18, 0.05);
            }
            
            for sp in bl.geometry_block.simple_polygons.iter_mut() {
                
                sp.quadtree = Quadtree::calculate(&sp.bounds(), 18, 0.05);
            }
            
            for sp in bl.geometry_block.complicated_polygons.iter_mut() {
                let bnd=sp.bounds();
                sp.quadtree = Quadtree::calculate(&bnd, 18, 0.05);
                
            }
        }
    }
    
}

impl<T> CallFinish for MakeGeometries<T>
    where T: CallFinish<CallType=WorkingBlock, ReturnType=Timings>
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
        
    
        
        


pub fn process_geometry(prfx: &str, outfn: Option<&str>, filter: Option<&str>, timestamp: Option<&str>, numchan: usize) -> Result<()> {


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

    let style = Arc::new(GeometryStyle::default());

    let cf = Box::new(CollectWorkingTiles::new(!outfn.is_none()));
    
    let pp: Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>> = if numchan == 0 {
        let mg = Box::new(MakeGeometries::new(cf, style.clone(),true));
        let mm = Box::new(ProcessMultiPolygons::new(style.clone(), mg));
        let ap = Box::new(AddParentTag::new(mm, style.clone()));
        let ww = Box::new(CollectWayNodes::new(ap, style.clone()));
        make_read_primitive_blocks_combine_call_all(ww)
    } else {
        let cfb = Box::new(Callback::new(cf));
        let mg = Box::new(Callback::new(Box::new(MakeGeometries::new(cfb, style.clone(),true))));
        let mm = Box::new(Callback::new(Box::new(ProcessMultiPolygons::new(style.clone(), mg))));
        
        let ap = Box::new(Callback::new(Box::new(AddParentTag::new(mm, style.clone()))));
        
        let ww = CallbackSync::new(Box::new(CollectWayNodes::new(ap,style.clone())), numchan);
        
        let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for w in ww {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            pps.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all(w2))))
        }
        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
    };
        
    let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!("process_geometry, numchan={}", numchan));
       
    let (tm,_) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog);
    prog.finish();
    tx.add("process_geometry");
    
    println!("{}\n{}", tm,tx);
    
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
                println!("{}: {} tiles", w, tiles.len());
                match outfn {
                    None => {println!("no outfn?"); },
                    Some(ofn) => {
                        serde_json::to_writer(std::fs::File::create(ofn)?, &tiles)?;
                    }
                }
            },
        }
    }
    
    Ok(())

}
