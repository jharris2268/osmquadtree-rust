use crate::elements::{Relation,ElementType,Way,Quadtree};
use crate::geometry::{WorkingBlock,GeometryStyle,Object,LonLat,ComplicatedPolygonGeometry,RingPart,Ring,PolygonPart,Timings,OtherData};
use crate::geometry::position::{point_in_poly};
use crate::geometry::elements::collect_rings;
use crate::callback::CallFinish;
use crate::utils::ThreadTimer;

use std::sync::Arc;
use std::collections::{BTreeMap,BTreeSet};
use std::io::{Error,ErrorKind,Result};

type WayEntry = (Way,Vec<LonLat>,Vec<String>);
type PendingWays = BTreeMap<i64, (BTreeSet<i64>, Option<WayEntry>)>;



fn add_ring<'a>(res: &mut Vec<PolygonPart>, q: Ring, must_be_inner: bool) -> Option<Ring> {
    for a in res.iter_mut() {
        if a.exterior.bbox.contains(&q.bbox) {
            let x = a.exterior.lonlats().unwrap();
            let y = &q.parts[0].lonlats[0];
            if point_in_poly(&x, y) {
            //let y = q.lonlats().unwrap();
            //if polygon_contains(&x, &y) {
                a.add_interior(q);
                return None;
            }
        }
    }
    if must_be_inner {
        return Some(q);
    }
    res.push(PolygonPart::new(q));
    None
}
        


fn order_rings(rings: Vec<Ring>, inner_rings: Vec<Ring>) -> (Vec<PolygonPart>,Vec<Ring>) {
    let mut pp = Vec::new();
    for mut r in rings {
        r.calc_area_bbox().expect("!");
        pp.push(r);
    }
    
    pp.sort_by(|p,q| { (-1.0 * f64::abs(p.area)).partial_cmp(&(-1.0*f64::abs(q.area))).unwrap() });
    
    let mut res = Vec::new();
    
    for p in pp {
        add_ring(&mut res, p, false);
    }
    
    let mut pp2 = Vec::new();
    for mut r in inner_rings {
        r.calc_area_bbox().expect("!");
        pp2.push(r);
    }
    
    pp2.sort_by(|p,q| { (-1.0 * f64::abs(p.area)).partial_cmp(&(-1.0*f64::abs(q.area))).unwrap() });
    let mut rem = Vec::new();
    for p in pp2 {
        match add_ring(&mut res, p, true) {
            None => {},
            Some(r) => { rem.push(r); }
        }
    }
    
    (res,rem)
}
       



fn is_multipolygon_rel(rel: &Relation) -> bool {
    //let mut is_bound = false;
    //let mut is_admin = true;
    for t in &rel.tags {
        if t.key == "type" {
            if t.val == "multipolygon" { return true; }
            else if t.val == "boundary" { return true; }//is_bound=true; }
            else { return false; }
        } /*else if t.key == "boundary" {
            is_admin = t.val == "administrative";
        }*/
    }
    //is_bound && is_admin
    false
}
struct MultiPolygons {
    style: Arc<GeometryStyle>,
    
    pending_relations: BTreeMap<i64, (Relation, BTreeSet<i64>)>,
    pending_ways: PendingWays,
    
    errs: Vec<(Object,String)>,
    pass_rels:bool,
    tma: f64,
    tmb: f64,
    tmc: f64,
    tmd: f64,
    tmca: f64,
    tmcb: f64,
    skipped_big_poly: usize
}

impl MultiPolygons {
    pub fn new(style: Arc<GeometryStyle>, pass_rels: bool) -> MultiPolygons {
        MultiPolygons{style:style, 
                pending_relations: BTreeMap::new(), pending_ways: BTreeMap::new(), errs: Vec::new(),
                pass_rels:pass_rels,tma: 0.0, tmb: 0.0, tmc: 0.0, tmca: 0.0, tmcb: 0.0, tmd: 0.0, skipped_big_poly: 0}
    }
    
    fn add_relation(&mut self, r: Relation) {
        let mut pw = BTreeSet::new();
        for m in &r.members {
            match m.mem_type {
                ElementType::Way => {
                    pw.insert(m.mem_ref);
                    match self.pending_ways.get_mut(&m.mem_ref) {
                        Some(p) => { p.0.insert(r.id);},
                        None => {
                            let mut x = BTreeSet::new();
                            x.insert(r.id);
                            self.pending_ways.insert(m.mem_ref, (x,None));
                        }
                    }
                },
                _ => {}
            }
        }
        self.pending_relations.insert(r.id, (r,pw));
    }
    
    fn make_complicated_polygon(&mut self, outer_ringparts: Vec<RingPart>, inner_ringparts: Vec<RingPart>, rel: &Relation) -> Result<Option<ComplicatedPolygonGeometry>> {
        if outer_ringparts.is_empty() {
            return Err(Error::new(ErrorKind::Other, "no ring parts"))
        }
        
        
        let mut tm = ThreadTimer::new();
        let (tags, _, layer) = self.style.process_multipolygon_relation(&rel.tags)?; //no zorder for polys
        
        //let rp = ringparts.len();
        let (rings, _left) = collect_rings(outer_ringparts)?;
        let (rings2, _left2) = collect_rings(inner_ringparts)?;
        self.tmca += tm.since();
        tm=ThreadTimer::new();
        
        if rings.is_empty() {
            return Err(Error::new(ErrorKind::Other, "no rings"))
        }
        
        /*if rings.len()+rings2.len()>200 {
            self.skipped_big_poly+=1;
            return Ok(None);
        }*/
            
        
        let (polys,_left3) = order_rings(rings, rings2);
        if polys.is_empty() {
            return Err(Error::new(ErrorKind::Other, "no polys"))
        }
        
        self.tmcb += tm.since();
        
        
        
        Ok(Some(ComplicatedPolygonGeometry::new(rel, tags, None, layer, polys)))
        
        
    }
    
    fn finish_relation(&mut self, finished_ways: &mut BTreeSet<i64>, rel: Relation) -> Option<ComplicatedPolygonGeometry> {
        
        let mut inner_ringparts=Vec::new();
        let mut ringparts = Vec::new();
        
        for m in &rel.members {
            match m.mem_type {
                ElementType::Way => {
                    match self.pending_ways.get_mut(&m.mem_ref) {
                        None => {},
                        
                        Some(p) => {
                            
                            match p.1.as_ref() {
                                Some(qq) => {
                                    p.0.remove(&rel.id);
                                    if p.0.is_empty() { finished_ways.insert(qq.0.id.clone()); }
                                    if m.role == "inner" {
                                        inner_ringparts.push(RingPart::new(qq.0.id.clone(), false, qq.0.refs.clone(), qq.1.clone()));
                                    } else {
                                        ringparts.push(RingPart::new(qq.0.id.clone(), false, qq.0.refs.clone(), qq.1.clone()));
                                    }
                                },
                                None => {
                                    println!("way object {} never added? [{:?}]", m.mem_ref, p.0);
                                    p.0.remove(&rel.id);
                                }
                            }
                            
                        }
                    }
                },
                _ => {}
            }
        }
        //if ringparts.len()+inner_ringparts.len() < 125 {
            match self.make_complicated_polygon(ringparts, inner_ringparts, &rel) {
                Err(e) => {
                    self.errs.push((Object::Relation(rel), e.to_string()));
                    None
                },
                Ok(p) => {
                    //Some(p)
                    p
                    
                }
            }
        //} else {
        //    self.skipped_big_poly += 1;
        //    None
        //}
    }
    
    
    pub fn process(&mut self, mut wb: WorkingBlock) -> (WorkingBlock,(usize,usize,usize,usize)) {
        
        let mut tm=ThreadTimer::new();
        
        let mut rr = Vec::new();
        let mut ww = Vec::new();
        let mut finished_rels=Vec::new();
        
        let mut rels_taken=0;
        let mut rels_finished=0;
        
        let mut ways_taken=0;
        let mut ways_finished=0;
        
        for r in wb.pending_relations {
            if is_multipolygon_rel(&r) {
                rels_taken+=1;
                self.add_relation(r);
            } else if self.pass_rels {
                rr.push(r);
            }
        }
        wb.pending_relations = rr;
        
        self.tma += tm.since();
        tm=ThreadTimer::new();
        
        for w in std::mem::take(&mut wb.pending_ways) {
            let i = w.0.id;
            match self.pending_ways.get_mut(&i) {
                None => { ww.push(w); },
                
                Some(pw) => {
                    ways_taken+=1;
                    if !pw.1.is_none() { 
                        panic!("way already present!");
                    }
                    pw.1 = Some(w);
                    for r in &pw.0 {
                        match self.pending_relations.get_mut(r) {
                            None => { panic!("missing rel"); },
                            Some((_,s)) => {
                                s.remove(&i);
                                if s.is_empty() {
                                    finished_rels.push(*r);
                                }
                            },
                        }
                    }
                }
            }
        }
        
        wb.pending_ways = ww;
        self.tmb += tm.since();
        tm=ThreadTimer::new();
        let mut finished_ways = BTreeSet::new();
        
        
        
        for r in finished_rels {
            let (_,(rel,_)) = self.pending_relations.remove_entry(&r).expect("!");
            match self.finish_relation(&mut finished_ways, rel) {
                Some(r) => { 
                    rels_finished+=1;
                    wb.geometry_block.complicated_polygons.push(r);
                },
                None => {},
                
            }
        }
        self.tmc += tm.since();
        tm=ThreadTimer::new();
        //finished_ways.len();
        for w in finished_ways {
            match self.pending_ways.remove_entry(&w) {
                None => { println!("\nway not added {}\n", w); },
                Some(pw) => {
                    match pw.1.1 {
                        None => { println!("way not found.. {}", w); },
                        Some(x) => {
                            ways_finished+=1;
                            wb.pending_ways.push(x);
                        },
                    }
                }
            }
        }
        self.tmd += tm.since();
        
        (wb,(rels_taken,ways_taken,rels_finished,ways_finished))
    }
    
    pub fn finish(&mut self) -> (WorkingBlock, Vec<(Object,String)>, String) {
        let mut tm=ThreadTimer::new();
        let mut res = WorkingBlock::new(-1, Quadtree::empty(), 0);
        let mut finished_ways = BTreeSet::new();
        for (_,(rel,_)) in std::mem::take(&mut self.pending_relations) {
            match self.finish_relation(&mut finished_ways, rel) {
                Some(r) => { res.geometry_block.complicated_polygons.push(r); },
                None => {},
                
            }
        }
        self.tmc += tm.since();
        tm = ThreadTimer::new();
        
        for w in finished_ways {
            match self.pending_ways.remove_entry(&w) {
                None => { println!("\nway not added {}\n", w); },
                Some(pw) => {
                    match pw.1.1 {
                        None => { println!("way not found.. {}", w); },
                        Some(x) => {res.pending_ways.push(x); },
                    }
                }
            }
        }
        self.tmd += tm.since();
        (res, std::mem::take(&mut self.errs), format!("check rels: {:0.1}s, check ways: {:0.1}s, make poly: {:0.1}s [collect rings: {:0.1}s, merge rings: {:0.1}s], finish ways: {:0.1}s. Skipped {} big polys", self.tma, self.tmb, self.tmc, self.tmca, self.tmcb, self.tmd, self.skipped_big_poly))
    }
    
}                

pub struct ProcessMultiPolygons<T: ?Sized> {
    multipolygons: MultiPolygons,
    out: Box<T>,
    tm: f64,
    counts: (usize,usize,usize,usize)
}
impl<T> ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings> + ?Sized
{
    pub fn new(style: Arc<GeometryStyle>, out: Box<T>) -> ProcessMultiPolygons<T> {
        ProcessMultiPolygons{multipolygons: MultiPolygons::new(style,false), out: out, tm: 0.0,counts:(0,0,0,0)}
    }
}

impl<T> CallFinish for ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings> + ?Sized
{
    type CallType = WorkingBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, wb: WorkingBlock) {
        let tx= ThreadTimer::new();
        let (ans,c) = self.multipolygons.process(wb);
        self.tm += tx.since();
        self.out.call(ans);
        self.counts.0 += c.0;
        self.counts.1 += c.1;
        self.counts.2 += c.2;
        self.counts.3 += c.3;
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let tx= ThreadTimer::new();
        let (ans,errs,mm) = self.multipolygons.finish();
        let a = format!("finished {} rels & {} ways at end", ans.geometry_block.complicated_polygons.len(), ans.pending_ways.len());
        
        self.tm += tx.since();
        self.out.call(ans);
        
        let mut tms = self.out.finish()?;
        tms.add("ProcessMultiPolygons", self.tm);
        let m = format!("rels taken/finished: {}/{}, ways_taken/finished: {}/{}", self.counts.0,self.counts.2,self.counts.1,self.counts.3);
        tms.add_other("ProcessMultiPolygons", OtherData::Messages(vec![m,a,mm]));
        tms.add_other("ProcessMultiPolygons", OtherData::Errors(errs));
        Ok(tms)
    }
}

