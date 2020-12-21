use crate::elements::{Relation,ElementType,Way,Quadtree};
use crate::geometry::{WorkingBlock,GeometryStyle,Object,LonLat,ComplicatedPolygonGeometry,Timings,OtherData};
use crate::callback::CallFinish;
use crate::utils::ThreadTimer;

use std::sync::Arc;
use std::collections::{BTreeMap,BTreeSet};
use std::io::{Error,ErrorKind,Result};

type WayEntry = (Way,Vec<LonLat>,Vec<String>);
type PendingWays = BTreeMap<i64, (BTreeSet<i64>, Option<WayEntry>)>;

fn make_complicated_polygon(_style: &GeometryStyle, _ways: &mut PendingWays, _rel: &Relation) -> Result<Option<ComplicatedPolygonGeometry>> {
    Err(Error::new(ErrorKind::Other,"not implemented"))
    
    
}

fn is_multipolygon_rel(rel: &Relation) -> bool {
    for t in &rel.tags {
        if t.key == "type" {
            return t.val == "multipolygon"
        }
    }
    false
}
struct MultiPolygons {
    style: Arc<GeometryStyle>,
    
    pending_relations: BTreeMap<i64, (Relation, BTreeSet<i64>)>,
    pending_ways: PendingWays,
    
    errs: Vec<(Object,String)>
}

impl MultiPolygons {
    pub fn new(style: Arc<GeometryStyle>) -> MultiPolygons {
        MultiPolygons{style:style, pending_relations: BTreeMap::new(), pending_ways: BTreeMap::new(), errs: Vec::new()}
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
    
    fn finish_relation(&mut self, finished_ways: &mut BTreeSet<i64>, rel: Relation) -> Option<ComplicatedPolygonGeometry> {
        
        
        
        for m in &rel.members {
            match m.mem_type {
                ElementType::Way => {
                    match self.pending_ways.get_mut(&m.mem_ref) {
                        None => {},
                        
                        Some(p) => {
                            p.0.remove(&rel.id);
                            let qq = p.1.as_mut().unwrap();
                            if p.0.is_empty() { finished_ways.insert(qq.0.id.clone()); }
                            //relww.push(qq);
                        }
                    }
                },
                _ => {}
            }
        }
        
        match make_complicated_polygon(&self.style, &mut self.pending_ways, &rel) {
            Err(e) => {
                self.errs.push((Object::Relation(rel), e.to_string()));
                None
            },
            Ok(p) => {
                p
                
            }
        }
    }
    
    
    pub fn process(&mut self, mut wb: WorkingBlock) -> WorkingBlock {
        
        let mut rr = Vec::new();
        let mut ww = Vec::new();
        let mut finished_rels=Vec::new();
        
        
        for r in wb.pending_relations {
            if is_multipolygon_rel(&r) {
                self.add_relation(r);
            } else {
                rr.push(r);
            }
        }
        wb.pending_relations = rr;
        
        
        for w in std::mem::take(&mut wb.pending_ways) {
            let i = w.0.id;
            match self.pending_ways.get_mut(&i) {
                None => { ww.push(w); },
                
                Some(pw) => {
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
        
        let mut finished_ways = BTreeSet::new();
        
        for r in finished_rels {
            let (_,(rel,_)) = self.pending_relations.remove_entry(&r).expect("!");
            match self.finish_relation(&mut finished_ways, rel) {
                Some(r) => { wb.geometry_block.complicated_polygons.push(r); },
                None => {},
                
            }
        }
        
        
        for w in finished_ways {
            match self.pending_ways.remove_entry(&w) {
                None => { println!("\nway not added {}\n", w); },
                Some(pw) => {
                    match pw.1.1 {
                        None => { println!("way not found.. {}", w); },
                        Some(x) => {wb.pending_ways.push(x); },
                    }
                }
            }
        }
        
        wb
    }
    
    pub fn finish(&mut self) -> (WorkingBlock, Vec<(Object,String)>) {
        
        let mut res = WorkingBlock::new(-1, Quadtree::empty(), 0);
        let mut finished_ways = BTreeSet::new();
        for (_,(rel,_)) in std::mem::take(&mut self.pending_relations) {
            match self.finish_relation(&mut finished_ways, rel) {
                Some(r) => { res.geometry_block.complicated_polygons.push(r); },
                None => {},
                
            }
        }
        
        
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
        (res, std::mem::take(&mut self.errs))
    }
    
}                

pub struct ProcessMultiPolygons<T> {
    multipolygons: MultiPolygons,
    out: Box<T>,
    tm: f64,
}
impl<T> ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
{
    pub fn new(style: Arc<GeometryStyle>, out: Box<T>) -> ProcessMultiPolygons<T> {
        ProcessMultiPolygons{multipolygons: MultiPolygons::new(style), out: out, tm: 0.0}
    }
}

impl<T> CallFinish for ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
{
    type CallType = WorkingBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, wb: WorkingBlock) {
        let tx= ThreadTimer::new();
        let ans = self.multipolygons.process(wb);
        self.tm += tx.since();
        self.out.call(ans);
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let tx= ThreadTimer::new();
        let (ans,errs) = self.multipolygons.finish();
        self.tm += tx.since();
        self.out.call(ans);
        
        let mut tms = self.out.finish()?;
        tms.add("ProcessMultiPolygons", self.tm);
        tms.add_other("ProcessMultiPolygons", OtherData::Errors(errs));
        Ok(tms)
    }
}

