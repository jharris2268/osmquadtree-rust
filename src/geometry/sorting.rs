use crate::geometry::{GeometryBlock,PointGeometry, LinestringGeometry, SimplePolygonGeometry, ComplicatedPolygonGeometry};
use crate::geometry::Timings;
use crate::sortblocks::QuadtreeTree;

use crate::elements::Quadtree;
use crate::callback::CallFinish;

use std::collections::BTreeMap;
use std::sync::Arc;


struct CollectTempGeometry<T> {
    out: Box<T>,
    limit: usize,
    splitat: i64,
    groups: Arc<QuadtreeTree>,
    pending: BTreeMap<i64, GeometryBlock>,
    qttoidx: BTreeMap<Quadtree, i64>,
    tm: f64,
    //count: usize
}

impl<'a, T> CollectTempGeometry<T>
where
    T: CallFinish<CallType = Vec<GeometryBlock>, ReturnType = Timings>,
{
    pub fn new(
        out: Box<T>,
        limit: usize,
        splitat: i64,
        groups: Arc<QuadtreeTree>,
    ) -> CollectTemp<T> {
        let mut qttoidx = BTreeMap::new();
        let mut i = 0;
        for (_, t) in groups.iter() {
            qttoidx.insert(t.qt, i);
            i += 1;
        }
        CollectTemp {
            out: out,
            limit: limit,
            splitat: splitat,
            groups: groups,
            qttoidx: qttoidx,
            pending: BTreeMap::new(),
            tm: 0.0,
            
        }
    }

    fn add_all(&mut self, bl: GeometryBlock) -> Vec<GeometryBlock> {
        let mut mm = Vec::new();
        
        for o in bl.points {
            match self.add_point(o) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        for o in bl.linestrings {
            match self.add_linestring(o) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        for o in bl.simple_polygons {
            match self.add_simple_polygon(o) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        for o in bl.complicated_polygons {
            match self.add_complicated_polygons(o) {
                Some(m) => mm.push(m),
                None => {}
            }
        }
        
        mm
    }

    fn get_block(&'a mut self, q: Quadtree) -> &'a mut GeometryBlock {
        let q = self.groups.find(q).1.qt;
        let i = self.qttoidx.get(&q).unwrap();
        let k = i / self.splitat;
        if !self.pending.contains_key(&k) {
            let t = GeometryBlock::new(k, Quadtree::empty(), 0);
            self.pending.insert(k.clone(), t);
        }
        self.pending.get_mut(&k).unwrap()
    }
    
    fn add_point(&mut self, n: PointGeometry) -> Option<GeometryBlock> {
        let l = self.limit;
        let t = self.get_block(n.quadtree);
        t.nodes.push(n);
        if t.points.len() + 8 * t.linestrings.len() + 8 * t.simple_polygons.len() + 20 * t.complicated_polygons.len() >= l {
            return Some(std::mem::replace(t, GeometryBlock::new(t.index, Quadtree::empty(), 0)));
        }
        None
    }

    fn add_linestring(&mut self, w: LinestringGeometry) -> Option<GeometryBlock> {
        let l = self.limit;
        let t = self.get_block(w.quadtree);
        t.linestrings.push(w);
        if t.points.len() + 8 * t.linestrings.len() + 8 * t.simple_polygons.len() + 20 * t.complicated_polygons.len() >= l {
            return Some(std::mem::replace(t, GeometryBlock::new(t.index, Quadtree::empty(), 0)));
        }
        None
    }

    fn add_simple_polygon(&mut self, r: SimplePolygonGeometry) -> Option<GeometryBlock> {
        let l = self.limit;
        let t = self.get_block(r.quadtree);
        t.simple_polygons.push(r);
        if t.points.len() + 8 * t.linestrings.len() + 8 * t.simple_polygons.len() + 20 * t.complicated_polygons.len() >= l {
            return Some(std::mem::replace(t, GeometryBlock::new(t.index, Quadtree::empty(), 0)));
        }
        None
    }
    fn add_complicated_polygon(&mut self, r: ComplicatedPolygonGeometry) -> Option<GeometryBlock> {
        let l = self.limit;
        let t = self.get_block(r.quadtree);
        t.complicated_polygons.push(r);
        if t.points.len() + 8 * t.linestrings.len() + 8 * t.simple_polygons.len() + 20 * t.complicated_polygons.len() >= l {
            return Some(std::mem::replace(t, GeometryBlock::new(t.index, Quadtree::empty(), 0)));
        }
        None
    }
    
}

impl<T> CallFinish for CollectTempGeometry<T>
where
    T: CallFinish<CallType = Vec<GeometryBlock>, ReturnType = Timings>,
{
    type CallType = GeometryBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: GeometryBlock) {
        let tx = Timer::new();
        let mm = self.add_all(bl);
        self.tm += tx.since();
        self.out.call(mm);
    }

    fn finish(&mut self) -> io::Result<Timings> {
        let mut mm = Vec::new();
        for (_, b) in std::mem::take(&mut self.pending) {
            mm.push(b);
        }
        self.out.call(mm);
        
        let mut r = self.out.finish()?;
        
        r.add("collect temp", self.tm);
        Ok(r)
    }
}
