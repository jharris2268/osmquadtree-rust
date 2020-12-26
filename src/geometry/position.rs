use crate::elements::quadtree::{
        EARTH_WIDTH, coordinate_as_float, coordinate_as_integer,
        latitude_mercator, latitude_un_mercator, Bbox};

use std::borrow::Borrow;

#[derive(Clone,Eq,PartialEq,Ord,PartialOrd,Debug)]
pub struct LonLat {
    pub lon: i32,
    pub lat: i32
}
impl LonLat {
    pub fn new(lon: i32, lat: i32) -> LonLat {
        LonLat{lon,lat}
    }
    
    pub fn forward(&self) -> XY {
        let x = coordinate_as_float(self.lon)*EARTH_WIDTH / 180.0;
        let y = latitude_mercator(coordinate_as_float(self.lat), EARTH_WIDTH);
        XY::new(f64::round(x*100.0)/100.0, f64::round(y*100.0)/100.0)
    }
}

use serde::ser::{Serialize, Serializer, SerializeSeq};
impl Serialize for LonLat {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.lon)?;
        seq.serialize_element(&self.lat)?;
        seq.end()
    }
}


#[derive(Clone,PartialEq,PartialOrd,Debug)]
pub struct XY {
    pub x: f64,
    pub y: f64
}
impl XY {
    pub fn new(x: f64, y: f64) -> XY {
        XY{x,y}
    }
    pub fn backward(&self) -> LonLat {
        let lon = coordinate_as_integer(self.x*180.0 /EARTH_WIDTH);
        let lat = coordinate_as_integer(latitude_un_mercator(self.y, EARTH_WIDTH));
        LonLat::new(lon, lat)
    }
    
}


#[allow(dead_code)]
pub fn epsg_code(transform: bool) -> u32 {
    if transform {
        3857
    } else {
        4326
    }
}
#[allow(dead_code)]
pub fn pythag(p: &XY, q: &XY) -> f64 {
    f64::sqrt(f64::powi(p.x-q.x, 2) + f64::powi(p.y-q.y, 2))
}

#[allow(dead_code)]
pub fn calc_line_length<T: Borrow<LonLat>>(lonlats: &[T]) -> f64 {
    if lonlats.len() < 2 {
        return 0.0;
    }
    
    let mut ans = 0.0;
    let mut prev = lonlats[0].borrow().forward();
    for i in 1..lonlats.len() {
        let curr = lonlats[i].borrow().forward();
        ans += pythag(&prev, &curr);
        prev=curr;
    }
    
    ans
}

pub fn calc_ring_area<T: Borrow<LonLat>>(lonlats: &[T]) -> f64 {
    
    if lonlats.len() < 3 {
        return 0.0;
    }
    let mut area = 0.0;
    
    let mut prev = lonlats[0].borrow().forward();
    
    for i in 1..lonlats.len() {
        
        let curr = lonlats[i].borrow().forward();
        area += prev.x * curr.y - prev.y * curr.x;
        prev = curr
    }

    return -1.0 * area / 2.0; //want polygon exteriors to be anti-clockwise
}
#[allow(dead_code)]
pub fn calc_ring_centroid<T: Borrow<LonLat>>(lonlats: &[T]) -> XY {
    
    if lonlats.len() == 0 {
        return XY::new(0.0,0.0);
    }
    
    let mut prev = lonlats[0].borrow().forward();
    if lonlats.len() == 1 {
        return prev;
    }
    
    if lonlats.len() == 2 {
        let curr = lonlats[1].borrow().forward();
        return XY::new( (prev.x+curr.x) / 2.0, (prev.y+curr.y)/2.0);
    }
    
    let mut area = 0.0;
    let mut res = XY::new(0.0, 0.0);
    for i in 1..lonlats.len() {
        let curr = lonlats[i].borrow().forward();
        
        let cross = prev.x * curr.y - prev.y * curr.x;
        res.x += (prev.x+curr.x)*cross;
        res.y += (prev.y+curr.y)*cross;
        area += cross;
        prev=curr;
    }
    
    area *= 3.0;
    res.x /= area;
    res.y /= area;
    
    res
}
 
 
fn segment_side(p1: &LonLat, p2: &LonLat, q: &LonLat) -> i32 {
    let s = (q.lon as f64 - p1.lon as f64) * (p2.lat as f64 - p1.lat as f64) - (p2.lon as f64 - p1.lon as f64) * (q.lat as f64 - p1.lat as f64);
    
    if s<0.0 {
        -1
    } else if s>0.0 {
        1
    } else {
        0
    }
}
    
    
    
pub fn segment_intersects(p1: &LonLat, p2: &LonLat, q1: &LonLat, q2: &LonLat) -> bool {
    
    let pq1 = segment_side(p1,p2,q1);
    let pq2 = segment_side(p1,p2,q2);
    if pq1==pq2 { return false; }
    
    let qp1 = segment_side(q1,q2,p1);
    let qp2 = segment_side(q1,q2,p2);
    if qp1==qp2 { return false; }
    return true;
}
    
pub fn line_intersects<T0: Borrow<LonLat>, T1: Borrow<LonLat>>(left: &[T0], right: &[T1]) -> bool {
    
    if left.len() < 2 || right.len() < 2 {
        return false;
    }
    
    for i in 0..(left.len()-1) {
        for j in 0..(right.len()-1) {
            if segment_intersects(&left[i].borrow(), &left[i+1].borrow(), &right[j].borrow(), &right[j+1].borrow()) {
                return true;
            }
        }
    }
    false
}   
#[allow(dead_code)]
pub fn line_box_intersects<T: Borrow<LonLat>>(line: &[T], bbox: &Bbox) -> bool {
    
    if line.len()<2 { return false; }
    
    let a = LonLat::new(bbox.minlon, bbox.minlat);
    let b = LonLat::new(bbox.maxlon, bbox.minlat);
    let c = LonLat::new(bbox.maxlon, bbox.maxlat);
    let d = LonLat::new(bbox.minlon, bbox.minlat);
    let boxl = vec![a,b,c,d];
    line_intersects(line, &boxl)
}


pub fn point_in_poly<T: Borrow<LonLat>>(line: &[T], pt: &LonLat) -> bool {
    
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
    
    let testx = coordinate_as_float(pt.lon);
    let testy = coordinate_as_float(pt.lat);

    let mut c = false;
    for i in 0..line.len() {
        let j = if i==0 { line.len()-1 } else { i-1 };
        let vxi = coordinate_as_float(line[i].borrow().lon);
        let vyi = coordinate_as_float(line[i].borrow().lat);
        let vxj = coordinate_as_float(line[j].borrow().lon);
        let vyj = coordinate_as_float(line[j].borrow().lat);
        
        if (vyi > testy) != (vyj > testy) {
            if testx < (vxj-vxi) * (testy - vyi) / (vyj - vyi) + vxi {
            
                c = !c;
            }
        }
    }
    c
}
#[allow(dead_code)]
pub fn polygon_box_intersects<T: Borrow<LonLat>>(poly: &[T], bbox: &Bbox) -> bool {

    if poly.len() < 3 { return false; }
    
    //if line_box_intersects(poly,bbox) { return true; }
    
    if bbox.contains_point(poly[0].borrow().lon,poly[0].borrow().lat) {
        return true;
    }
    
    if point_in_poly(poly, &LonLat::new(bbox.minlon,bbox.minlat)) {
        return true;
    }
    
    return false;
}


pub fn polygon_contains<T0: Borrow<LonLat>,T1: Borrow<LonLat>>(bigger: &[T0], smaller: &[T1]) -> bool {
    if line_intersects(bigger, smaller) {
        return false;
    }
    
    point_in_poly(bigger, smaller[0].borrow())
}
