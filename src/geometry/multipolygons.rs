use crate::elements::{Relation,ElementType,Way,Quadtree};
use crate::geometry::{WorkingBlock,GeometryStyle,Object,LonLat,ComplicatedPolygonGeometry,RingPart,Timings,OtherData};
use crate::callback::CallFinish;
use crate::utils::ThreadTimer;

use std::sync::Arc;
use std::collections::{BTreeMap,BTreeSet};
use std::io::{Error,ErrorKind,Result};

type WayEntry = (Way,Vec<LonLat>,Vec<String>);
type PendingWays = BTreeMap<i64, (BTreeSet<i64>, Option<WayEntry>)>;


fn reverse_parts(mut parts: Vec<RingPart>) -> Vec<RingPart> {
    parts.reverse();
    for p in parts.iter_mut() {
        p.is_reversed = !p.is_reversed;
    }
    parts
}

fn get_first_last(part: &Vec<RingPart>) -> (i64,i64) {
    let p = &part[0];
    let f = if p.is_reversed {
        p.refs[p.refs.len()-1]
    } else {
        p.refs[0]
    };
    
    let q = &part[part.len()-1];
    let t = if q.is_reversed {
        q.refs[0]
    } else {
        q.refs[q.refs.len()-1]
    };
    (f,t)
}
fn is_ring(part: &Vec<RingPart>) -> bool {
    let (f,t) = get_first_last(part);
    f==t
}

fn merge_rings(parts: &mut Vec<Vec<RingPart>>) -> (bool,Option<Vec<RingPart>>) {
    if parts.len() == 0 { return (false,None); }
    if parts.len() == 1 {
        if is_ring(&parts[0]) {
            let zz = parts.remove(0);
            return (true, Some(zz));
        }
        return (false,None);
    }
    
    
    for i in 0 .. parts.len()-1 {
        let (f,t) = get_first_last(&parts[i]);
        if f==t {
            let zz = parts.remove(i);
            return (true, Some(zz));
        }
        for j in i+1 .. parts.len() {
            let (g,u) = get_first_last(&parts[j]);
            
            if t == g {
                let zz = parts.remove(j);
                parts[i].extend(zz);
                if is_ring(&parts[i]) {
                    let zz = parts.remove(i);
                    return (true, Some(zz));
                }
                return (true,None);
            } else if t == u {
                let zz = parts.remove(j);
                parts[i].extend(reverse_parts(zz));
                if is_ring(&parts[i]) {
                    let zz = parts.remove(i);
                    return (true, Some(zz));
                }
                return (true,None);
            } else if f==u {
                let zz = parts.remove(i);
                parts[j-1].extend(zz);
                return (true,None);
            } else if f == g {
                let zz = parts.remove(i);
                parts[j-1].extend(reverse_parts(zz));
                return (true,None);
            }
        }
    }
    return (false,None);
}
                


fn collect_rings(ww: Vec<RingPart>) -> Result<(Vec<Vec<RingPart>>,Vec<Vec<RingPart>>)> {
    //let nw=ww.len();
    let mut parts = Vec::new();
    for w in ww {
        parts.push(vec![w]);
    }
    
    let mut res = Vec::new();
    loop {
        let (f,r) = merge_rings(&mut parts);
        match r {
            None => {},
            Some(r) => { res.push(r); }
        }
        if !f {
            break;
        }
    }
    /*
    let mut rem=Vec::new();
    for p in parts {
        for q in p {
            rem.push(q);
        }
    }*/
    
    //println!("found {} rings from {} ways, {} left", res.len(), nw, rem.len());
    //Err(Error::new(ErrorKind::Other,"not implemented"))
    Ok((res,parts))
}

fn make_complicated_polygon(_style: &GeometryStyle, ringparts: Vec<RingPart>, rel: &Relation) -> Result<Option<ComplicatedPolygonGeometry>> {
    
    
    let rp = ringparts.len();
    let (rings,left) = collect_rings(ringparts)?;
    if !left.is_empty()  {
        println!("relation {}, {:?}, {} ways, {} rings {} left", rel.id, rel.tags, rp, rings.len(), left.len());
        //if rel.id==8087080 {
            
            for (i,r) in rings.iter().enumerate() {
                println!("ring {}: {:?}", i,r);
            }
            println!("remaining: {:?}", left);
        //}
    }
    Err(Error::new(ErrorKind::Other,"not implemented"))
    
}

fn is_multipolygon_rel(rel: &Relation) -> bool {
    let mut is_bound = false;
    let mut is_admin = false;
    for t in &rel.tags {
        if t.key == "type" {
            if t.val == "multipolygon" { return true; }
            else if t.val == "boundary" { is_bound=true; }
            else { return false; }
        } else if t.key == "boundary" {
            is_admin = t.val == "administrative";
        }
    }
    is_bound && is_admin
}
struct MultiPolygons {
    style: Arc<GeometryStyle>,
    
    pending_relations: BTreeMap<i64, (Relation, BTreeSet<i64>)>,
    pending_ways: PendingWays,
    
    errs: Vec<(Object,String)>,
    pass_rels:bool
}

impl MultiPolygons {
    pub fn new(style: Arc<GeometryStyle>, pass_rels: bool) -> MultiPolygons {
        MultiPolygons{style:style, pending_relations: BTreeMap::new(), pending_ways: BTreeMap::new(), errs: Vec::new(),pass_rels:pass_rels}
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
        
        let mut ringparts = Vec::new();
        
        for m in &rel.members {
            match m.mem_type {
                ElementType::Way => {
                    match self.pending_ways.get_mut(&m.mem_ref) {
                        None => {},
                        
                        Some(p) => {
                            p.0.remove(&rel.id);
                            let qq = p.1.as_mut().unwrap();
                            if p.0.is_empty() { finished_ways.insert(qq.0.id.clone()); }
                            ringparts.push(RingPart::new(qq.0.id.clone(), false, qq.0.refs.clone(), qq.1.clone()));
                        }
                    }
                },
                _ => {}
            }
        }
        
        match make_complicated_polygon(&self.style, ringparts, &rel) {
            Err(e) => {
                self.errs.push((Object::Relation(rel), e.to_string()));
                None
            },
            Ok(p) => {
                p
                
            }
        }
    }
    
    
    pub fn process(&mut self, mut wb: WorkingBlock) -> (WorkingBlock,(usize,usize,usize,usize)) {
        
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
        
        finished_ways.len();
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
        
        (wb,(rels_taken,ways_taken,rels_finished,ways_finished))
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
    counts: (usize,usize,usize,usize)
}
impl<T> ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
{
    pub fn new(style: Arc<GeometryStyle>, out: Box<T>) -> ProcessMultiPolygons<T> {
        ProcessMultiPolygons{multipolygons: MultiPolygons::new(style,false), out: out, tm: 0.0,counts:(0,0,0,0)}
    }
}

impl<T> CallFinish for ProcessMultiPolygons<T>
    where T: CallFinish<CallType=WorkingBlock,ReturnType=Timings>
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
        let (ans,errs) = self.multipolygons.finish();
        let a = format!("finished {} rels & {} ways at end", ans.geometry_block.complicated_polygons.len(), ans.pending_ways.len());
        
        self.tm += tx.since();
        self.out.call(ans);
        
        let mut tms = self.out.finish()?;
        tms.add("ProcessMultiPolygons", self.tm);
        let m = format!("rels taken/finished: {}/{}, ways_taken/finished: {}/{}", self.counts.0,self.counts.2,self.counts.1,self.counts.3);
        tms.add_other("ProcessMultiPolygons", OtherData::Messages(vec![m,a]));
        tms.add_other("ProcessMultiPolygons", OtherData::Errors(errs));
        Ok(tms)
    }
}

