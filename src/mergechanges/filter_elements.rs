use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, MergeTimings, ReplaceNoneWithTimings};
use crate::elements::{
    Bbox, ElementType, IdSet, IdSetBool, MinimalBlock, MinimalNode, MinimalRelation, MinimalWay,
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
const REGEX_STR: &str = r"^\s*(\-?\d\.\d+E[-|+]\d+)\s+(\-?\d\.\d+E[-|+]\d+)\s*$";

#[derive(Debug,Clone)]
pub struct Poly {
    vertsx: Vec<f64>,
    vertsy: Vec<f64>,
}

impl Poly {
    pub fn new(vertsx: Vec<f64>, vertsy: Vec<f64>) -> Poly {
        Poly {
            vertsx: vertsx,
            vertsy: vertsy,
        }
    }

    pub fn from_file(fname: &str) -> Result<Poly> {
        let re = Regex::new(REGEX_STR).unwrap();
        let mut vertsx = Vec::new();
        let mut vertsy = Vec::new();
        for ln in BufReader::new(File::open(fname)?).lines() {
            let ln = ln?;
            let caps = re.captures(&ln);
            match caps {
                None => {
                    if ln == "1" || ln == "none" || ln == "END" {
                        //pass
                    } else {
                        println!("!!!: {}", ln);
                    }
                }
                Some(cp) => {
                    if cp.len() != 3 {
                        /*println!("?? {}", ln);*/
                    } else {
                        let ln = cp.get(1).unwrap().as_str().parse().unwrap();
                        let lt = cp.get(2).unwrap().as_str().parse().unwrap();

                        /*println!("found {} & {}", ln, lt);*/
                        vertsx.push(ln);
                        vertsy.push(lt);
                    }
                }
            }
        }
        Ok(Poly::new(vertsx, vertsy))
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

struct FilterObjs {
    bbox: Bbox,
    poly: Option<Poly>,
    idset: Option<Box<IdSetBool>>,
    pending_rels: Vec<MinimalRelation>,
    tm: f64,
}

fn has_node<T: IdSet>(idset: &Box<T>, w: &MinimalWay) -> bool {
    for rf in DeltaPackedInt::new(&w.refs_data) {
        if idset.contains(ElementType::Node, rf) {
            return true;
        }
    }
    return false;
}

impl FilterObjs {
    pub fn new(bbox: &Bbox, poly: &Option<Poly>) -> FilterObjs {
        FilterObjs {
            bbox: bbox.clone(),
            poly: poly.clone(),
            idset: Some(Box::new(IdSetBool::new())),
            tm: 0.0,
            pending_rels: Vec::new(),
        }
    }

    fn get_idset<'a>(&'a mut self) -> &'a mut Box<IdSetBool> {
        self.idset.as_mut().unwrap()
    }

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
            self.get_idset().nodes.insert(n.id);
        }
        for w in mb.ways {
            self.add_way(&w);
        }
        for r in mb.relations {
            self.get_idset().relations.insert(r.id);
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
            self.get_idset().nodes.insert(n.id);
        }
    }

    fn check_way(&mut self, w: &MinimalWay) {
        if has_node(self.get_idset(), w) {
            self.add_way(w);
        }
    }
    fn add_way(&mut self, w: &MinimalWay) {
        self.get_idset().ways.insert(w.id);

        for n in DeltaPackedInt::new(&w.refs_data) {
            if !self.get_idset().nodes.contains(&n) {
                self.get_idset().exnodes.insert(n);
            }
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
                if self.get_idset().contains(ty, rf) {
                    self.get_idset().relations.insert(r.id);
                    return (true, false);
                }
            }
        }
        (!hasm, hasr)
    }

    fn check_pending_relations(&mut self) {
        let exn = std::mem::take(&mut self.get_idset().exnodes);
        let ii = self.get_idset();
        for e in exn {
            ii.nodes.insert(e);
        }

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

        let aa: Arc<IdSetBool> = Arc::from(std::mem::take(&mut self.idset).unwrap());

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

    let fb = Box::new(FilterObjs::new(bbox, poly));

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
