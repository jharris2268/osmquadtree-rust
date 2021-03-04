use crate::callback::CallFinish;
use crate::elements::{Tag, EARTH_WIDTH};
use crate::geometry::default_minzoom_values::DEFAULT_MINZOOM_VALUES;
use crate::geometry::{GeometryBlock, OtherData, Timings, WorkingBlock};
use crate::utils::ThreadTimer;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Read, Result};

fn get_type(t: &str, line: &usize) -> Result<usize> {
    if t == "0" {
        Ok(0)
    } else if t == "1" {
        Ok(1)
    } else if t == "2" {
        Ok(2)
    } else {
        Err(Error::new(
            ErrorKind::Other,
            format!("wrong type at line {}", line),
        ))
    }
}

fn test_val(vals: &BTreeMap<Tag, (i64, String)>, kv: &Tag) -> Option<i64> {
    match vals.get(&kv) {
        Some((m, _)) => Some(*m),
        None => match vals.get(&Tag::new(kv.key.clone(), String::from("*"))) {
            Some((m, _)) => Some(*m),
            None => None,
        },
    }
}

fn find_from_tags(vals: &BTreeMap<Tag, (i64, String)>, tgs: &[Tag]) -> Option<i64> {
    let mut ans: Option<i64> = None;
    for t in tgs {
        match test_val(vals, &t) {
            None => {}
            Some(m) => match ans {
                None => {
                    ans = Some(m);
                }
                Some(n) => {
                    ans = Some(i64::min(m, n));
                }
            },
        }
    }
    ans
}

const MAX_MINZOOM: i64 = 18;

fn area_minzoom(area: f64, min_area: f64) -> i64 {
    i64::min(
        MAX_MINZOOM,
        res_zoom(f64::sqrt(area / min_area)).floor() as i64,
    )
}

fn res_zoom(res: f64) -> f64 {
    if f64::abs(res) < 0.001 {
        return 20.0;
    }
    f64::log(EARTH_WIDTH * 2.0 / res / 256.0, 2.0)
}

pub struct MinZoomSpec {
    pub min_area: f64,
    pub max_minzoom: Option<i64>,
    pub points: BTreeMap<Tag, (i64, String)>,
    pub lines: BTreeMap<Tag, (i64, String)>,
    pub polygons: BTreeMap<Tag, (i64, String)>,
}

impl MinZoomSpec {
    pub fn new(min_area: f64, max_minzoom: Option<i64>) -> MinZoomSpec {
        MinZoomSpec {
            min_area: min_area,
            max_minzoom: max_minzoom,
            points: BTreeMap::new(),
            lines: BTreeMap::new(),
            polygons: BTreeMap::new(),
        }
    }

    pub fn from_reader<R: Read>(
        min_area: f64,
        max_minzoom: Option<i64>,
        reader: R,
    ) -> Result<MinZoomSpec> {
        let mut res = MinZoomSpec::new(min_area, max_minzoom);

        for (line, row) in csv::Reader::from_reader(reader).records().enumerate() {
            match row {
                Ok(rec) => {
                    if rec.len() != 5 {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("minzoom at line {} wrong length", line),
                        ));
                    }
                    let typ = get_type(&rec[0], &line)?;
                    let key = rec[1].to_string();
                    let val = rec[2].to_string();
                    let zoom: i64 = match rec[3].parse() {
                        Ok(p) => p,
                        Err(_) => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                format!("minzoom at line {} zoom not int", line),
                            ));
                        }
                    };
                    let table = rec[4].to_string();

                    if typ == 0 {
                        if &key[0..4] == "addr" {
                            println!("{},{},{},{}", key, val, zoom, table);
                        }
                        res.points.insert(Tag::new(key, val), (zoom, table));
                    } else if typ == 1 {
                        res.lines.insert(Tag::new(key, val), (zoom, table));
                    } else if typ == 2 {
                        res.polygons.insert(Tag::new(key, val), (zoom, table));
                    }
                }
                Err(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("{:?} at line {}", e, line),
                    ));
                }
            }
        }
        Ok(res)
    }

    pub fn default(min_area: f64, max_minzoom: Option<i64>) -> MinZoomSpec {
        MinZoomSpec::from_reader(min_area, max_minzoom, DEFAULT_MINZOOM_VALUES.as_bytes())
            .expect("!!")
    }

    fn check_max_minzoom(&self, v: Option<i64>) -> Option<i64> {
        match self.max_minzoom {
            None => v,
            Some(mx) => match v {
                None => None,
                Some(m) => {
                    if m > mx {
                        None
                    } else {
                        Some(m)
                    }
                }
            },
        }
    }

    fn find_point(&self, tgs: &[Tag]) -> Option<i64> {
        self.check_max_minzoom(find_from_tags(&self.points, tgs))
    }

    fn find_line(&self, tgs: &[Tag]) -> Option<i64> {
        self.check_max_minzoom(find_from_tags(&self.lines, tgs))
    }

    fn find_polygon(&self, tgs: &[Tag], a: f64) -> Option<i64> {
        match find_from_tags(&self.polygons, tgs) {
            None => None,
            Some(p) => self.check_max_minzoom(Some(i64::max(p, area_minzoom(a, self.min_area)))),
        }
    }

    fn find_all(&self, gb: &mut GeometryBlock) -> usize {
        let mut na = 0;

        for mut p in std::mem::take(&mut gb.points) {
            match self.find_point(&p.tags) {
                None => {
                    if self.max_minzoom.is_none() {
                        gb.points.push(p);
                    }
                }
                Some(v) => {
                    p.minzoom = Some(v);
                    p.quadtree = p.quadtree.round(v as usize);
                    na += 1;
                    gb.points.push(p);
                }
            }
        }

        for mut p in std::mem::take(&mut gb.linestrings) {
            match self.find_line(&p.tags) {
                None => {
                    if self.max_minzoom.is_none() {
                        gb.linestrings.push(p);
                    }
                }
                Some(v) => {
                    p.minzoom = Some(v);
                    p.quadtree = p.quadtree.round(v as usize);
                    na += 1;
                    gb.linestrings.push(p);
                }
            }
        }

        for mut p in std::mem::take(&mut gb.simple_polygons) {
            match self.find_polygon(&p.tags, p.area) {
                None => {
                    if self.max_minzoom.is_none() {
                        gb.simple_polygons.push(p);
                    }
                }
                Some(v) => {
                    p.minzoom = Some(v);
                    p.quadtree = p.quadtree.round(v as usize);
                    na += 1;
                    gb.simple_polygons.push(p);
                }
            }
        }
        for mut p in std::mem::take(&mut gb.complicated_polygons) {
            match self.find_polygon(&p.tags, p.area) {
                None => {
                    if self.max_minzoom.is_none() {
                        gb.complicated_polygons.push(p);
                    }
                }
                Some(v) => {
                    p.minzoom = Some(v);
                    p.quadtree = p.quadtree.round(v as usize);
                    na += 1;
                    gb.complicated_polygons.push(p);
                }
            }
        }
        na
    }
}

pub struct FindMinZoom<T: ?Sized> {
    out: Box<T>,
    spec: Option<MinZoomSpec>,

    tm: f64,
    na: usize,
}

impl<T> FindMinZoom<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings>,
{
    pub fn new(out: Box<T>, spec: Option<MinZoomSpec>) -> FindMinZoom<T> {
        FindMinZoom {
            out: out,
            spec: spec,
            tm: 0.0,
            na: 0,
        }
    }
}

impl<T> CallFinish for FindMinZoom<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings>,
{
    type CallType = WorkingBlock;
    type ReturnType = Timings;

    fn call(&mut self, mut bl: WorkingBlock) {
        match &self.spec {
            None => {}
            Some(mz) => {
                let tx = ThreadTimer::new();
                self.na += mz.find_all(&mut bl.geometry_block);
                self.tm += tx.since();
            }
        }
        self.out.call(bl);
    }

    fn finish(&mut self) -> Result<Timings> {
        let mut tms = self.out.finish()?;
        if !self.spec.is_none() {
            tms.add("FindMinZoom", self.tm);
            tms.add_other(
                "FindMinZoom",
                OtherData::Messages(vec![format!("found {} minzooms", self.na)]),
            );
        }

        Ok(tms)
    }
}
