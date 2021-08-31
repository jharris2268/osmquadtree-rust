use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings, ReplaceNoneWithTimings};
use crate::elements::{
    Bbox, ElementType, IdSet, IdSetAll, IdSetSet, IdSetBool, MinimalBlock, MinimalNode, MinimalRelation, MinimalWay,
};
use crate::pbfformat::{
    make_read_minimal_blocks_combine_call_all, read_all_blocks_parallel_with_progbar, FileBlock,
};
use crate::utils::{as_int, ThreadTimer};
use simple_protocolbuffers::{DeltaPackedInt, PackedInt};

use crate::update::ParallelFileLocs;

use std::fs::File;
use std::io::{BufRead, /*Error,ErrorKind,*/ BufReader, Result};
use std::sync::Arc;

type Timings = channelled_callbacks::Timings<Arc<dyn IdSet>>;

use regex::Regex;
//const REGEX_STR: &str = r"^\s*(\-?\d\.\d+E[-|+]\d+)\s+(\-?\d\.\d+E[-|+]\d+)\s*$";
//const REGEX_STR: &str = r"^\s*(\-?\d*\.\d+(E[-|+]\d+)?)\s+(\-?\d*\.\d+(E[-|+]\d+)?)\s*$";


#[derive(Debug,Clone)]
pub struct Poly {
    pub vertsx: Vec<f64>,
    pub vertsy: Vec<f64>,
    pub name: String,
}

impl Poly {
    pub fn new(vertsx: Vec<f64>, vertsy: Vec<f64>, name: String) -> Poly {
        Poly {
            vertsx: vertsx,
            vertsy: vertsy,
            name: name
        }
    }

    pub fn from_file(fname: &str) -> Result<Poly> {
        //let re = Regex::new(REGEX_STR).unwrap();
        let re_name = Regex::new(r"[a-z]+").unwrap();
        let mut vertsx = Vec::new();
        let mut vertsy = Vec::new();
        let mut name=String::from("");
        for ln in BufReader::new(File::open(fname)?).lines() {
            let ln_trim = ln?.trim().to_string();
            let ln_parts = ln_trim.split_whitespace().collect::<Vec<&str>>();
            if ln_parts.len() == 1 {
                if ln_parts[0] == "1" || ln_parts[0] == "END" {
                        //pass
                } else if re_name.is_match(&ln_parts[0]) {
                    
                    name = ln_parts[0].to_string();
                } else {
                    println!("!!!: {:?}", ln_parts);
                }
                
            } else if ln_parts.len() == 2 {
                let lon = ln_parts[0].parse().expect(&format!("?? {:?}", ln_parts[0]));
                let lat = ln_parts[1].parse().expect(&format!("?? {:?}", ln_parts[1]));
                vertsx.push(lon);
                vertsy.push(lat);
            } else {
                
                println!("!!!: {:?}", ln_parts);
            }
            /*
            let caps = re.captures(&ln);
            match caps {
                None => {
                    if ln == "1" || ln == "END" {
                        //pass
                    } else if re_name.is_match(&ln) {
                        
                        name = ln.to_string();
                    } else {
                        println!("!!!: {}", ln);
                    }
                }
                Some(cp) => {
                    if cp.len() != 5 {
                        println!("?? {} {}", cp.len(), ln);
                    } else {
                        let ln = cp.get(1).unwrap().as_str().parse().expect(&format!("?? {:?}", cp.get(1).unwrap().as_str()));
                        let lt = cp.get(2).unwrap().as_str().parse().expect(&format!("?? {:?}", cp.get(2).unwrap().as_str()));

                        /*println!("found {} & {}", ln, lt);*/
                        vertsx.push(ln);
                        vertsy.push(lt);
                    }
                }
            }*/
        }
        Ok(Poly::new(vertsx, vertsy, name))
        //Err(Error::new(ErrorKind::Other,"not impl"))
    }

    pub fn bounds(&self) -> Bbox {
        let mut bx = Bbox::empty();
        for (a, b) in self.vertsx.iter().zip(self.vertsy.iter()) {
            bx.expand(as_int(*a), as_int(*b));
        }
        bx
    }
    pub fn check_box(&self, bx: &Bbox) -> bool {
        self.contains_point(bx.minlon, bx.minlat)
            && self.contains_point(bx.minlon, bx.maxlat)
            && self.contains_point(bx.maxlon, bx.minlat)
            && self.contains_point(bx.maxlon, bx.maxlat)
    }

    pub fn contains_point(&self, ln: i32, lt: i32) -> bool {
        /*from  https://wrf.ecse.rpi.edu//Research/Short_Notes/pnpoly.html
        Copyright (c) 1970-2003, Wm. Randolph Franklin

        Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

            Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimers.
            Redistributions in binary form must reproduce the above copyright notice in the documentation and/or other materials provided with the distribution.
            The name of W. Randolph Franklin may not be used to endorse or promote products derived from this Software without specific prior written permission.

        THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

        int pnpoly(int nvert, float *vertx, float *verty, float testx, float testy)
        {
          int i, j, c = 0;
          for (i = 0, j = nvert-1; i < nvert; j = i++) {
            if ( ((verty[i]>testy) != (verty[j]>testy)) &&
             (testx < (vertx[j]-vertx[i]) * (testy-verty[i]) / (verty[j]-verty[i]) + vertx[i]) )
               c = !c;
          }
          return c;
        }
        */
        let testx = (ln as f64) * 0.0000001;
        let testy = (lt as f64) * 0.0000001;

        let mut c = false;
        for i in 0..self.vertsx.len() {
            let j = if i == 0 { self.vertsx.len() - 1 } else { i - 1 };
            if (self.vertsy[i] > testy) != (self.vertsy[j] > testy) {
                if testx
                    < (self.vertsx[j] - self.vertsx[i]) * (testy - self.vertsy[i])
                        / (self.vertsy[j] - self.vertsy[i])
                        + self.vertsx[i]
                {
                    c = !c;
                }
            }
        }
        c
    }
}


pub enum IdSetEither {
    Set(IdSetSet),
    Bool(IdSetBool),
    All(IdSetAll)
}

impl IdSet for IdSetEither {
    fn contains(&self, t: ElementType, i: i64) -> bool {
        match self {
            IdSetEither::Set(ref s) => s.contains(t,i),
            IdSetEither::Bool(ref b) => b.contains(t,i),
            IdSetEither::All(ref a) => a.contains(t,i),
        }
    }
}    
use std::fmt::Display;
impl Display for IdSetEither {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdSetEither::Set(ref s) => write!(f, "{}", s),
            IdSetEither::Bool(ref b) => write!(f, "{}", b),
            IdSetEither::All(ref a) => write!(f, "{}", a)
        }
    }
}
    
    
impl IdSetEither {    
    fn add_node(&mut self, i: i64) {
        match self {
            IdSetEither::Set(ref mut s) =>  {s.nodes.insert(i);},
            IdSetEither::Bool(ref mut b) => {b.nodes.insert(i);},
            IdSetEither::All(ref _a) => {},
        }
    }
    fn add_exnode(&mut self, i: i64) {
        match self {
            IdSetEither::Set(ref mut s) =>  {s.exnodes.insert(i);},
            IdSetEither::Bool(ref mut b) => {b.exnodes.insert(i);},
            IdSetEither::All(ref _a) => {},
        }
    }
    fn add_way(&mut self, i: i64) {
        match self {
            IdSetEither::Set(ref mut s) =>  {s.ways.insert(i);},
            IdSetEither::Bool(ref mut b) => {b.ways.insert(i);},
            IdSetEither::All(ref _a) => {}
        }
    }
    fn add_relation(&mut self, i: i64) {
        match self {
            IdSetEither::Set(ref mut s) =>  {s.relations.insert(i);},
            IdSetEither::Bool(ref mut b) => {b.relations.insert(i);},
            IdSetEither::All(ref _a) => {}
        }
    }
}    
    
    


struct FilterObjs {
    bbox: Bbox,
    poly: Option<Poly>,
    idset: IdSetEither,
    pending_rels: Vec<MinimalRelation>,
    tm: f64,
}

fn has_node<T: IdSet>(idset: &T, w: &MinimalWay) -> bool {
    for rf in DeltaPackedInt::new(&w.refs_data) {
        if idset.contains(ElementType::Node, rf) {
            return true;
        }
    }
    return false;
}

impl FilterObjs {
    pub fn new(bbox: &Bbox, poly: &Option<Poly>, bool_idset: bool) -> FilterObjs {
        let ids = if bool_idset {
            IdSetEither::Bool(IdSetBool::new())
        } else {
            IdSetEither::Set(IdSetSet::new())
        };
        FilterObjs {
            bbox: bbox.clone(),
            poly: poly.clone(),
            idset: ids,
            tm: 0.0,
            pending_rels: Vec::new(),
        }
    }

    /*fn get_idset<'a>(&'a mut self) -> &'a mut Box<IdSetBool> {
        self.idset.as_mut().unwrap()
    }*/

    fn check_block(&mut self, mb: MinimalBlock) {
        let qb = mb.quadtree.as_bbox(0.05);
        if (self.poly.is_none() || self.poly.as_ref().unwrap().check_box(&qb))
            && self.bbox.contains(&qb)
        {
            self.add_all(mb)
        } else if self.bbox.overlaps(&qb) {
            for n in mb.nodes {
                self.check_node(&n);
            }
            for w in mb.ways {
                self.check_way(&w);
            }
            for r in mb.relations {
                if !self.check_relation(&r, false).0 {
                    self.pending_rels.push(r);
                }
            }
        } else {
            println!("?? {} {} // {}", mb.quadtree.as_string(), qb, self.bbox);
        }
    }
    fn add_all(&mut self, mb: MinimalBlock) {
        for n in mb.nodes {
            //self.get_idset().nodes.insert(n.id);
            self.idset.add_node(n.id)
        }
        for w in mb.ways {
            self.add_way(&w);
        }
        for r in mb.relations {
            //self.get_idset().relations.insert(r.id);
            self.idset.add_relation(r.id)
        }
    }

    fn check_pt(&self, ln: i32, lt: i32) -> bool {
        if !self.bbox.contains_point(ln, lt) {
            return false;
        }
        match &self.poly {
            None => true,
            Some(poly) => poly.contains_point(ln, lt),
        }
    }

    fn check_node(&mut self, n: &MinimalNode) {
        if self.check_pt(n.lon, n.lat) {
            //self.get_idset().nodes.insert(n.id);
            self.idset.add_node(n.id)
        }
    }

    fn check_way(&mut self, w: &MinimalWay) {
        if has_node(&self.idset, w) {
            self.add_way(w);
        }
    }
    fn add_way(&mut self, w: &MinimalWay) {
        //self.get_idset().ways.insert(w.id);
        self.idset.add_way(w.id);

        for n in DeltaPackedInt::new(&w.refs_data) {
            if !self.idset.contains(ElementType::Node, n) {
                self.idset.add_exnode(n);
            }
            /*if !self.get_idset().nodes.contains(&n) {
                self.get_idset().exnodes.insert(n);
            }*/
        }
    }

    fn check_relation(&mut self, r: &MinimalRelation, justr: bool) -> (bool, bool) {
        let mut hasm = false;
        let mut hasr = false;
        for (tyi, rf) in PackedInt::new(&r.types_data).zip(DeltaPackedInt::new(&r.refs_data)) {
            let ty = ElementType::from_int(tyi);
            hasm = true;
            if ty == ElementType::Relation {
                hasr = true;
            }
            if !justr || ty == ElementType::Relation {
                if self.idset.contains(ty, rf) {
                    //self.get_idset().relations.insert(r.id);
                    self.idset.add_node(r.id);
                    return (true, false);
                }
            }
        }
        (!hasm, hasr)
    }

    fn check_pending_relations(&mut self) {
        
        match &mut self.idset {
            IdSetEither::Bool(ii) => {
                let exn = std::mem::take(&mut ii.exnodes);
                for e in exn {
                    ii.nodes.insert(e);
                }
            },
            IdSetEither::Set(ii) => {
                let exn = std::mem::take(&mut ii.exnodes);
                for e in exn {
                    ii.nodes.insert(e);
                }
            },
            IdSetEither::All(_) => {}
        };

        let mut relrels = Vec::new();
        for r in std::mem::take(&mut self.pending_rels) {
            let (x, y) = self.check_relation(&r, false);
            if !x && y {
                relrels.push(r);
            }
        }

        for _ in 0..5 {
            for r in &relrels {
                self.check_relation(r, true);
            }
        }
    }
}

impl CallFinish for FilterObjs {
    type CallType = MinimalBlock;
    type ReturnType = Timings;

    fn call(&mut self, mb: MinimalBlock) {
        let tx = ThreadTimer::new();
        self.check_block(mb);
        self.tm += tx.since();
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add("FilterBBox::call", self.tm);
        let tx = ThreadTimer::new();
        self.check_pending_relations();
        tm.add("FilterBBox::finish", tx.since());

        let aa: Arc<dyn IdSet> = Arc::from(std::mem::replace(&mut self.idset, IdSetEither::All(IdSetAll())));

        tm.add_other("idset", aa);
        Ok(tm)
    }
}

pub fn prep_bbox_filter(
    pfilelocs: &mut ParallelFileLocs,
    bbox: &Bbox,
    poly: &Option<Poly>,
    numchan: usize,
) -> Result<Arc<dyn IdSet>> {
    /*let mut pb = ProgBarWrap::new(100);
    pb.set_range(100);
    pb.set_message("prep_bbox_filter");*/

    let fb = Box::new(FilterObjs::new(bbox, poly, pfilelocs.2 > 512*1024*1024));

    let conv: Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>> =
        if numchan == 0 {
            make_read_minimal_blocks_combine_call_all(fb)
        } else {
            let fbb = CallbackSync::new(fb, numchan);
            let mut convs: Vec<
                Box<dyn CallFinish<CallType = (usize, Vec<FileBlock>), ReturnType = Timings>>,
            > = Vec::new();
            for f in fbb {
                let f2 = Box::new(ReplaceNoneWithTimings::new(f));
                convs.push(Box::new(Callback::new(
                    make_read_minimal_blocks_combine_call_all(f2),
                )));
            }
            Box::new(CallbackMerge::new(convs, Box::new(MergeTimings::new())))
        };

    //let (mut tm,_) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, conv, &pb);
    let mut tm = read_all_blocks_parallel_with_progbar(
        &mut pfilelocs.0,
        &pfilelocs.1,
        conv,
        "prep_bbox_filter",
        pfilelocs.2,
    );
    //pb.finish();
    Ok(tm.others.pop().unwrap().1)
}
