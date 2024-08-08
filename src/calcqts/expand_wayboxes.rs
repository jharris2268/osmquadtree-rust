use crate::calcqts::node_waynodes::NodeWayNodeComb;
use crate::calcqts::quadtree_store::{
    QuadtreeSimple, QuadtreeTileInt, WAY_SPLIT_MASK, WAY_SPLIT_SHIFT, WAY_SPLIT_VAL,
};
use channelled_callbacks::{CallFinish, Result as ccResult};
use crate::utils::{Timer,Error};

use crate::calcqts::{OtherData, Timings};

use crate::elements::{Bbox, Quadtree, QuadtreeBlock};
use std::collections::BTreeMap;

use crate::message;
pub struct WayBoxesSimple {
    boxes: BTreeMap<i64, Bbox>,
    tm: f64,
    qt_level: usize,
    qt_buffer: f64,
}

impl WayBoxesSimple {
    pub fn new(qt_level: usize, qt_buffer: f64) -> WayBoxesSimple {
        WayBoxesSimple {
            boxes: BTreeMap::new(),
            tm: 0.0,
            qt_level: qt_level,
            qt_buffer: qt_buffer,
        }
    }

    pub fn expand(&mut self, w: i64, lon: i32, lat: i32) {
        match self.boxes.get_mut(&w) {
            None => {
                self.boxes.insert(w, Bbox::new(lon, lat, lon, lat));
            }
            Some(nb) => {
                nb.expand(lon, lat);
            }
        }
    }

    pub fn calculate(&mut self, maxlevel: usize, buffer: f64) -> Box<QuadtreeSimple> {
        let mut qts = BTreeMap::new();
        for (w, b) in std::mem::take(&mut self.boxes) {
            qts.insert(w, Quadtree::calculate(&b, maxlevel, buffer));
        }
        Box::new(QuadtreeSimple::from_values(qts))
    }

    /*pub fn iter(&self) -> impl Iterator<Item = (&i64, &Bbox)> + '_ {
        self.boxes.iter()
    }*/
}

impl CallFinish for WayBoxesSimple {
    type CallType = Vec<NodeWayNodeComb>;
    type ReturnType = Timings;
    type ErrorType = Error;

    fn call(&mut self, nodewaynodes: Vec<NodeWayNodeComb>) {
        let t = Timer::new();
        for n in nodewaynodes {
            for w in &n.ways {
                self.expand(*w, n.lon, n.lat);
            }
        }
        self.tm += t.since();
    }

    fn finish(&mut self) -> ccResult<Timings, Error> {
        let mut t = Timings::new();
        t.add("wayboxessimple", self.tm);
        let tx = Timer::new();
        let r = self.calculate(self.qt_level, self.qt_buffer);
        t.add("calc quadtrees", tx.since());
        t.add_other("quadtrees", OtherData::QuadtreeSimple(r));
        Ok(t)
    }
}

pub struct WayBoxesVec {
    //wb: Vec<i32>,
    minlon: Vec<i32>,
    minlat: Vec<i32>,
    maxlon: Vec<i32>,
    maxlat: Vec<i32>,
    off: i64,
    c: usize,
}

impl WayBoxesVec {
    pub fn new(off: i64) -> WayBoxesVec {
        let minlon = vec![2000000000i32; WAY_SPLIT_VAL];
        let minlat = vec![2000000000i32; WAY_SPLIT_VAL];
        let maxlon = vec![-2000000000i32; WAY_SPLIT_VAL];
        let maxlat = vec![-2000000000i32; WAY_SPLIT_VAL];
        let c = 0;
        WayBoxesVec {
            minlon,
            minlat,
            maxlon,
            maxlat,
            off,
            c,
        }
    }

    pub fn expand(&mut self, i: usize, lon: i32, lat: i32) {
        if self.minlon[i] > 1800000000 {
            self.c += 1;
        }

        if lon < self.minlon[i] {
            self.minlon[i] = lon;
        }
        if lat < self.minlat[i] {
            self.minlat[i] = lat;
        }
        if lon > self.maxlon[i] {
            self.maxlon[i] = lon;
        }
        if lat > self.maxlat[i] {
            self.maxlat[i] = lat;
        }
    }

    pub fn calculate(&mut self, maxlevel: usize, buffer: f64) -> Box<QuadtreeBlock> {
        let mut t = Box::new(QuadtreeBlock::with_capacity(self.c));
        for i in 0..WAY_SPLIT_VAL {
            if self.minlon[i] <= 1800000000 {
                let q = Quadtree::calculate_vals(
                    self.minlon[i],
                    self.minlat[i],
                    self.maxlon[i],
                    self.maxlat[i],
                    maxlevel,
                    buffer,
                );

                //t.set(i,q.as_int());
                t.add_way(self.off + (i as i64), q);
            }
        }
        if t.len() != self.c {
            message!(
                "?? tile {} {} != {}",
                self.off >> WAY_SPLIT_SHIFT,
                self.c,
                t.len()
            );
        }
        t
    }

    pub fn calculate_tile(&mut self, maxlevel: usize, buffer: f64) -> Box<QuadtreeTileInt> {
        let mut t = Box::new(QuadtreeTileInt::new(self.off));

        for i in 0..WAY_SPLIT_VAL {
            if self.minlon[i] <= 1800000000 {
                let q = Quadtree::calculate_vals(
                    self.minlon[i],
                    self.minlat[i],
                    self.maxlon[i],
                    self.maxlat[i],
                    maxlevel,
                    buffer,
                );
                if q.as_int()<0 {
                    message!("\n\n?? way {} [{} {} {} {}] qt {}??\n\n", 
                    (self.off >> WAY_SPLIT_SHIFT)+i as i64,
                    self.minlon[i],
                    self.minlat[i],
                    self.maxlon[i],
                    self.maxlat[i],
                    q.as_int());
                }
                t.set(i, q.as_int());
            }
        }
        if t.count != self.c {
            message!(
                "?? tile {} {} != {}",
                self.off >> WAY_SPLIT_SHIFT,
                self.c,
                t.count
            );
        }
        t
    }
}

pub struct WayBoxesSplit {
    tiles: BTreeMap<i64, Box<WayBoxesVec>>,

    tm: f64,
    ram_limit: u64,
}

impl WayBoxesSplit {
    pub fn new(ram_gb: u64) -> WayBoxesSplit {
        WayBoxesSplit {
            tiles: BTreeMap::new(),
            tm: 0.0,
            ram_limit: ram_gb * 1024 * 1024 * 1024,
        }
    }

    fn take_tiles(&mut self) -> BTreeMap<i64, Box<WayBoxesVec>> {
        std::mem::take(&mut self.tiles)
    }

    fn approx_memory_use(&self) -> u64 {
        16 * (self.tiles.len() << WAY_SPLIT_SHIFT) as u64
    }

    pub fn expand(&mut self, w: i64, lon: i32, lat: i32) {
        let wt = w >> WAY_SPLIT_SHIFT;
        let wi = (w & WAY_SPLIT_MASK) as usize;

        if !self.tiles.contains_key(&wt) {
            self.tiles
                .insert(wt, Box::new(WayBoxesVec::new(wt << WAY_SPLIT_SHIFT)));
            if self.approx_memory_use() > self.ram_limit {
                // i.e. expected memory use > 8gb
                panic!("too many tiles");
            }
        }
        self.tiles.get_mut(&wt).unwrap().expand(wi, lon, lat);
    }
}

impl CallFinish for WayBoxesSplit {
    type CallType = Vec<NodeWayNodeComb>;
    type ReturnType = Timings;
    type ErrorType = Error;
    fn call(&mut self, nodewaynodes: Vec<NodeWayNodeComb>) {
        let tx = Timer::new();
        for n in nodewaynodes {
            for w in &n.ways {
                self.expand(*w, n.lon, n.lat);
            }
        }
        self.tm += tx.since();
    }

    fn finish(&mut self) -> ccResult<Timings, Error> {
        let mut t = Timings::new();
        t.add("expand boxes", self.tm);

        t.add_other("way bboxes", OtherData::WayBoxTiles(self.take_tiles()));

        Ok(t)
    }
}
