use crate::pbfformat::read_pbf;
use std::fmt;
use std::io::{Error, ErrorKind, Result};

use std::f64::consts::PI;

fn coordinate_as_integer(v: f64) -> i32 {
    if v > 0.0 {
        return ((v * 10000000.0) + 0.5) as i32;
    }
    ((v * 10000000.0) - 0.5) as i32
}

fn coordinate_as_float(v: i32) -> f64 {
    (v as f64) * 0.0000001
}
fn latitude_mercator(y: f64, scale: f64) -> f64 {
    (PI * (1.0 + y / 90.0) / 4.0).tan().ln() * scale / PI

    //return log(tan(M_PI*(1.0+y/90.0)/4.0)) * scale / PI;
}

fn latitude_un_mercator(d: f64, scale: f64) -> f64 {
    ((d * PI / scale).exp().atan() * 4.0 / PI - 1.0) * 90.0

    //return (atan(exp(d*M_PI/scale))*4/M_PI - 1.0) * 90.0;
}

//const EARTH_WIDTH: f64 = 20037508.342789244;

#[derive(Debug, Eq, PartialEq)]
pub struct Tuple {
    pub x: u32,
    pub y: u32,
    pub z: u32,
}

impl Tuple {
    pub fn new(x: u32, y: u32, z: u32) -> Tuple {
        Tuple { x: x, y: y, z: z }
    }
    pub fn read(data: &[u8]) -> Result<Tuple> {
        let mut res = Tuple::new(0, 0, 0);
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Value(1, x) => res.x = x as u32,
                read_pbf::PbfTag::Value(2, y) => res.y = y as u32,
                read_pbf::PbfTag::Value(3, z) => res.z = z as u32,
                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }
        Ok(res)
    }

    pub fn from_integer(qt: i64) -> Result<Tuple> {
        if qt < 0 {
            return Err(Error::new(ErrorKind::Other, format!("out of range {}", qt)));
        }

        if (qt & 31) > 20 {
            return Err(Error::new(
                ErrorKind::Other,
                format!("out of range {}", qt & 31),
            ));
        }

        let mut res = Tuple::new(0, 0, (qt & 31) as u32);

        for i in 0..res.z {
            res.x <<= 1;
            res.y <<= 1;
            let t = (qt >> (61 - 2 * i)) & 3;
            if t == 1 || t == 3 {
                res.x |= 1;
            }
            if t == 2 || t == 3 {
                res.y |= 1;
            }
        }
        Ok(res)
    }

    pub fn xyz(&self) -> (u32, u32, u32) {
        (self.x, self.y, self.z)
    }
    pub fn as_int(&self) -> Quadtree {
        Quadtree::from_xyz(self.x, self.y, self.z)
    }
}

impl fmt::Display for Tuple {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Quadtree({}, {}, {})", self.x, self.y, self.z)
    }
}

#[derive(Clone)]
pub struct Bbox {
    pub minlon: i32,
    pub minlat: i32,
    pub maxlon: i32,
    pub maxlat: i32,
}
impl Bbox {
    pub fn new(minlon: i32, minlat: i32, maxlon: i32, maxlat: i32) -> Bbox {
        Bbox {
            minlon,
            minlat,
            maxlon,
            maxlat,
        }
    }
    pub fn empty() -> Bbox {
        Bbox::new(1800000000, 900000000, -1800000000, -900000000)
    }
    
    pub fn from_str(fstr: &str) -> Result<Bbox> {
        let vv: Vec<&str> = fstr.split(",").collect();
        if vv.len() != 4 {
            return Err(Error::new(ErrorKind::Other, "expected four vals"));
        }
        let mut vvi = Vec::new();
        for v in vv {
            vvi.push(v.parse().unwrap());
        }
        Ok(Bbox::new(vvi[0], vvi[1], vvi[2], vvi[3]))
    }
    
    pub fn contains(&self,other: &Bbox) -> bool {
        if self.minlon > other.minlon {
            return false;
        }
        if self.minlat > other.minlat {
            return false;
        }
        if self.maxlon < other.maxlon {
            return false;
        }
        if self.maxlat < other.maxlat {
            return false;
        }
        true
    }
    pub fn contains_point(&self, ln: i32, lt: i32) -> bool {
        if self.minlon > ln {
            return false;
        }
        if self.minlat > lt {
            return false;
        }
        if self.maxlon < ln {
            return false;
        }
        if self.maxlat < lt {
            return false;
        }
        true
    }
        
    
    pub fn expand(&mut self, lon: i32, lat: i32) {
        if lon < self.minlon {
            self.minlon = lon;
        }
        if lat < self.minlat {
            self.minlat = lat;
        }
        if lon > self.maxlon {
            self.maxlon = lon;
        }
        if lat > self.maxlat {
            self.maxlat = lat;
        }
    }

    pub fn overlaps(&self, other: &Bbox) -> bool {
        if self.minlon > other.maxlon {
            return false;
        }
        if self.minlat > other.maxlat {
            return false;
        }
        if other.minlon > self.maxlon {
            return false;
        }
        if other.minlat > self.maxlat {
            return false;
        }
        return true;
    }
}

impl fmt::Display for Bbox {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{:-10} {:-10} {:-10} {:-10}]",
            self.minlon, self.minlat, self.maxlon, self.maxlat
        )
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, Copy)]
pub struct Quadtree(i64);

impl Quadtree {
    pub fn new(i: i64) -> Quadtree {
        Quadtree(i)
    }

    pub fn empty() -> Quadtree {
        Quadtree(-2)
    }

    pub fn calculate(bbox: &Bbox, maxlevel: usize, buffer: f64) -> Quadtree {
        Quadtree(make_quad_tree_floating(
            coordinate_as_float(bbox.minlon),
            coordinate_as_float(bbox.minlat),
            coordinate_as_float(bbox.maxlon),
            coordinate_as_float(bbox.maxlat),
            buffer,
            maxlevel,
        ))
    }

    pub fn calculate_vals(
        minlon: i32,
        minlat: i32,
        maxlon: i32,
        maxlat: i32,
        maxlevel: usize,
        buffer: f64,
    ) -> Quadtree {
        Quadtree(make_quad_tree_floating(
            coordinate_as_float(minlon),
            coordinate_as_float(minlat),
            coordinate_as_float(maxlon),
            coordinate_as_float(maxlat),
            buffer,
            maxlevel,
        ))
    }

    pub fn calculate_point(lon: i32, lat: i32, maxlevel: usize, buffer: f64) -> Quadtree {
        Quadtree(make_quad_tree_floating(
            coordinate_as_float(lon),
            coordinate_as_float(lat),
            coordinate_as_float(lon + 1),
            coordinate_as_float(lat + 1),
            buffer,
            maxlevel,
        ))
    }
    pub fn read(data: &[u8]) -> Result<Quadtree> {
        let mut res = (0, 0, 0);
        for x in read_pbf::IterTags::new(&data, 0) {
            match x {
                read_pbf::PbfTag::Value(1, x) => res.0 = x as u32,
                read_pbf::PbfTag::Value(2, y) => res.1 = y as u32,
                read_pbf::PbfTag::Value(3, z) => res.2 = z as u32,
                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }
        Ok(Self::from_xyz(res.0, res.1, res.2))
    }

    pub fn from_xyz(x: u32, y: u32, z: u32) -> Quadtree {
        if z > 20 {
            return Quadtree(-2);
        }

        let mut ans: i64 = 0;
        let mut scale = 1;
        for i in 0..(z as usize) {
            ans += ((((x >> i) & 1) | (((y >> i) & 1) << 1)) as i64) * scale;
            scale *= 4;
        }
        ans <<= 63 - 2 * (z as usize);
        ans += z as i64;
        Quadtree(ans)
    }

    pub fn depth(&self) -> usize {
        (self.0 & 31) as usize
    }

    pub fn as_string(&self) -> String {
        if self.0 < 0 {
            return String::from("NULL");
        }

        let l = self.depth();
        let mut r = String::with_capacity(l);

        for i in 0..l {
            r.push(match self.quad(i) {
                0 => 'A',
                1 => 'B',
                2 => 'C',
                3 => 'D',
                _ => {
                    panic!("??");
                }
            });
        }
        r
    }

    pub fn as_int(&self) -> i64 {
        self.0.clone()
    }

    pub fn as_tuple(&self) -> Tuple {
        Tuple::from_integer(self.0).expect("??")
    }

    pub fn quad(&self, d: usize) -> usize {
        if self.0 < 0 {
            return usize::MAX;
        }

        ((self.0 >> (61 - 2 * d)) & 3) as usize
    }

    pub fn round(&self, level: usize) -> Quadtree {
        if self.depth() <= level {
            return Quadtree(self.0);
        }
        let mut qt = self.0;
        qt >>= 63 - 2 * level;
        qt <<= 63 - 2 * level;
        Quadtree(qt + level as i64)
    }

    pub fn common(&self, other: &Quadtree) -> Quadtree {
        if self.0 < 0 {
            return Quadtree(other.0);
        } else if other.0 < 0 {
            return Quadtree(self.0);
        } else if self.0 == other.0 {
            return Quadtree(self.0);
        }

        let mut d = self.depth();
        if other.depth() < d {
            d = other.depth();
        }

        let mut p = 0;

        for i in 0..d {
            let q = self.round(i + 1).0;
            if q != other.round(i + 1).0 {
                return Quadtree(p);
            }
            p = q;
        }

        Quadtree(p)
    }

    pub fn as_bbox(&self, buffer: f64) -> Bbox {
        let mut min_x = -180.0;
        let mut min_y = -90.0;
        let mut max_x = 180.0;
        let mut max_y = 90.0;

        let l = self.depth();

        for i in 0..l {
            let v = (self.0 >> (61 - 2 * i)) & 3;

            if (v == 0) || (v == 2) {
                max_x -= (max_x - min_x) / 2.0;
            } else {
                min_x += (max_x - min_x) / 2.0;
            }

            if (v == 2) || (v == 3) {
                max_y -= (max_y - min_y) / 2.0;
            } else {
                min_y += (max_y - min_y) / 2.0;
            }
        }

        let mut min_y_m = latitude_un_mercator(min_y, 90.0);
        let mut max_y_m = latitude_un_mercator(max_y, 90.0);

        if buffer > 0.0 {
            let xx = (max_x - min_x) * buffer;
            let yy = (max_y_m - min_y_m) * buffer;

            min_x -= xx;
            min_y_m -= yy;
            max_x += xx;
            max_y_m += yy;
        }

        Bbox::new(
            coordinate_as_integer(min_x),
            coordinate_as_integer(min_y_m),
            coordinate_as_integer(max_x),
            coordinate_as_integer(max_y_m),
        )
    }
}
impl fmt::Display for Quadtree {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}

fn find_quad(min_x: f64, min_y: f64, max_x: f64, max_y: f64, buffer: f64) -> i64 {
    if (min_x < (-1.0 - buffer))
        || (min_y < (-1.0 - buffer))
        || (max_x > (1.0 + buffer))
        || (max_y > (1.0 + buffer))
    {
        return -1;
    }

    if (max_x <= 0.0) && (min_y >= 0.0) {
        return 0;
    } else if (min_x >= 0.0) && (min_y >= 0.0) {
        return 1;
    } else if (max_x <= 0.0) && (max_y <= 0.0) {
        return 2;
    } else if (min_x >= 0.0) && (max_y <= 0.0) {
        return 3;
    } else if (max_x < buffer)
        && (max_x.abs() < min_x.abs())
        && (min_y > -buffer)
        && (max_y.abs() >= min_y.abs())
    {
        return 0;
    } else if (min_x > -buffer)
        && (max_x.abs() >= min_x.abs())
        && (min_y > -buffer)
        && (max_y.abs() >= min_y.abs())
    {
        return 1;
    } else if (max_x < buffer)
        && (max_x.abs() < min_x.abs())
        && (max_y < buffer)
        && (max_y.abs() < min_y.abs())
    {
        return 2;
    } else if (min_x > -buffer)
        && (max_x.abs() >= min_x.abs())
        && (max_y < buffer)
        && (max_y.abs() < min_y.abs())
    {
        return 3;
    }
    return -1;
}

fn make_quad_tree_internal(
    mut min_x: f64,
    mut min_y: f64,
    mut max_x: f64,
    mut max_y: f64,
    buffer: f64,
    max_level: usize,
    current_level: usize,
) -> i64 {
    if max_level == 0 {
        return 0;
    }

    let q = find_quad(min_x, min_y, max_x, max_y, buffer);
    if q == -1 {
        return 0;
    }
    if (q == 0) || (q == 2) {
        min_x += 0.5;
        max_x += 0.5;
    } else {
        min_x -= 0.5;
        max_x -= 0.5;
    }
    if (q == 2) || (q == 3) {
        min_y += 0.5;
        max_y += 0.5;
    } else {
        min_y -= 0.5;
        max_y -= 0.5;
    }
    return (q << (61 - 2 * current_level))
        + 1
        + make_quad_tree_internal(
            2.0 * min_x,
            2.0 * min_y,
            2.0 * max_x,
            2.0 * max_y,
            buffer,
            max_level - 1,
            current_level + 1,
        );
}

fn make_quad_tree_floating(
    min_x: f64,
    min_y: f64,
    mut max_x: f64,
    mut max_y: f64,
    buffer: f64,
    max_level: usize,
) -> i64 {
    if (min_x > max_x) || (min_y > max_y) {
        return -1;
    }
    if max_x == min_x {
        max_x += 0.0000001;
    }
    if max_y == min_y {
        max_y += 0.0000001;
    }
    let min_y_merc = latitude_mercator(min_y, 1.0);
    let max_y_merc = latitude_mercator(max_y, 1.0);
    let min_m_merc = min_x / 180.0;
    let max_m_merc = max_x / 180.0;

    return make_quad_tree_internal(
        min_m_merc, min_y_merc, max_m_merc, max_y_merc, buffer, max_level, 0,
    );
}

/*
oqt::bbox bbox(int64 qt, double buffer) {

    double mx=-180.0, my=-90., Mx=180., My=90.;

    uint64 l = qt & 31;

    for (uint64 i=0; i < l; i++) {
        int64 v = (qt >> (61 - 2*i)) & 3;

        if ((v==0) || (v==2)) {
            Mx -= (Mx - mx) / 2;
        } else {
            mx += (Mx - mx) / 2;
        }
        if ((v==2) || (v==3)) {
            My -= (My - my) / 2;
        } else {
            my += (My - my) / 2;
        }

    }

    my = latitude_un_mercator(my);
    My = latitude_un_mercator(My);

    if (buffer > 0.0) {
        double xx = (Mx - mx) * buffer;
        double yy = (My - my) * buffer;
        mx -= xx;
        my -= yy;
        Mx += xx;
        My += yy;
    }

    return oqt::bbox{
            coordinate_as_integer(mx), coordinate_as_integer(my),
            coordinate_as_integer(Mx), coordinate_as_integer(My)};

}

xyz tuple(int64 qt) {
    uint64 z = qt & 31;
    int64 x=0, y=0;
    for (uint64 i = 0; i < z; i++) {
        x <<= 1;
        y <<= 1;
        int64 t = (qt >> (61-2*i)) & 3;
        if ((t & 1) == 1) {
            x |= 1;
        }
        if ((t & 2) == 2) {
            y |= 1;
        }
    }

    return xyz{x, y, (int64) z};
}




int64 from_tuple(int64 x, int64 y, int64 z) {
    int64 ans =0;
    int64 scale = 1;
    for (size_t i=0; i < (size_t) z; i++) {
        ans += (  ((x>>i)&1)  | (((y>>i)&1)<<1))  * scale;
        scale *= 4;
    }

    ans <<= (63 - (2 * uint(z)));
    ans |= z;
    return ans;
}

int64 from_string(const std::string& str) {

    int64 ans = 0;
    size_t i=0;
    for (auto itr = str.begin(); itr < str.end(); ++itr) {
        int64 p = -1;

        switch (*itr) {
            case 'A': p=0; break;
            case 'B': p=1; break;
            case 'C': p=2; break;
            case 'D': p=3; break;
            default: return 0;
        }
        ans |= p<<(61-2*i );
        i++;
    }
    ans |= str.size();

    return ans;

}
}
bool overlaps_quadtree(const bbox& l, int64 qt) {
    bbox r = quadtree::bbox(qt,0.05);
    return overlaps(l,r);
}

}
*/
