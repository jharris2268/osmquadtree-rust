use crate::geometry::XY;
use std::io::{Result, Write /*,ErrorKind,Error*/};
//use geos::Geom;

pub fn write_uint16<W: Write>(w: &mut W, i: usize) -> Result<()> {
    w.write_all(&[((i >> 8) & 255) as u8, (i & 255) as u8])
}

pub fn write_int32<W: Write>(w: &mut W, i: i32) -> Result<()> {
    write_uint32(w, i as u32)
}

pub fn write_int64<W: Write>(w: &mut W, i: i64) -> Result<()> {
    write_uint64(w, i as u64)
}

pub fn write_uint32<W: Write>(w: &mut W, i: u32) -> Result<()> {
    w.write_all(&[
        ((i >> 24) & 255) as u8,
        ((i >> 16) & 255) as u8,
        ((i >> 8) & 255) as u8,
        (i & 255) as u8,
    ])
}

pub fn write_uint64<W: Write>(w: &mut W, i: u64) -> Result<()> {
    w.write_all(&[
        ((i >> 56) & 255) as u8,
        ((i >> 48) & 255) as u8,
        ((i >> 40) & 255) as u8,
        ((i >> 32) & 255) as u8,
        ((i >> 24) & 255) as u8,
        ((i >> 16) & 255) as u8,
        ((i >> 8) & 255) as u8,
        (i & 255) as u8,
    ])
}

pub fn write_f64<W: Write>(w: &mut W, f: f64) -> Result<()> {
    let i = unsafe { *(&f as *const f64 as *const u64) };
    write_uint64(w, i)
}

pub fn prep_wkb(transform: bool, with_srid: bool, ty: u32, ln: usize) -> Result<Vec<u8>> {
    let l = 1 + 4 + (if with_srid { 4 } else { 0 }) + ln;
    let mut res = Vec::with_capacity(l);

    res.push(0);
    if with_srid {
        write_uint32(&mut res, ty + (32 << 24))?;
        write_uint32(&mut res, if transform { 3857 } else { 4326 })?;
    } else {
        write_uint32(&mut res, ty)?;
    }

    Ok(res)
}

pub fn write_point<W: Write>(w: &mut W, xy: &XY) -> Result<()> {
    write_f64(w, xy.x)?;
    write_f64(w, xy.y)
}

pub fn write_ring<W: Write, Iter: Iterator<Item = XY>>(
    w: &mut W,
    ln: usize,
    iter: Iter,
) -> Result<()> {
    write_uint32(w, ln as u32)?;
    for i in iter {
        write_point(w, &i)?;
    }
    Ok(())
}

pub trait AsWkb {
    fn as_wkb(&self, srid: Option<u32>) -> Result<Vec<u8>>;
}

fn prep_wkb_alt(srid: Option<u32>, ty: u32, ln: u32) -> Result<Vec<u8>> {
    let mut res = if ln == 0 {
        Vec::new()
    } else {
        Vec::with_capacity(1 + 4 + (if srid.is_none() { 4 } else { 0 }) + ln as usize)
    };

    res.push(0);
    match srid {
        None => {
            write_uint32(&mut res, ty)?;
        }
        Some(s) => {
            write_uint32(&mut res, ty + (32 << 24))?;
            write_uint32(&mut res, s)?;
        }
    }
    Ok(res)
}

impl AsWkb for geo::Point<f64> {
    fn as_wkb(&self, srid: Option<u32>) -> Result<Vec<u8>> {
        let mut res = prep_wkb_alt(srid, 1, 8)?;
        write_f64(&mut res, self.x())?;
        write_f64(&mut res, self.y())?;
        Ok(res)
    }
}

fn write_geo_linestring<W: Write>(w: &mut W, ls: &geo::LineString<f64>) -> Result<()> {
    write_uint32(w, ls.0.len() as u32)?;
    for p in ls.0.iter() {
        write_f64(w, p.x)?;
        write_f64(w, p.y)?;
    }
    Ok(())
}

impl AsWkb for geo::LineString<f64> {
    fn as_wkb(&self, srid: Option<u32>) -> Result<Vec<u8>> {
        let mut res = prep_wkb_alt(srid, 2, 8 * self.0.len() as u32)?;
        write_geo_linestring(&mut res, self)?;
        Ok(res)
    }
}

fn poly_num_points(poly: &geo::Polygon<f64>) -> u32 {
    let mut tot = poly.exterior().0.len();
    for ii in poly.interiors() {
        tot += ii.0.len();
    }
    tot as u32
}

impl AsWkb for geo::Polygon<f64> {
    fn as_wkb(&self, srid: Option<u32>) -> Result<Vec<u8>> {
        let mut res = prep_wkb_alt(
            srid,
            1,
            4 * (1 + self.interiors().len() as u32) + 8 * poly_num_points(self),
        )?;
        write_geo_linestring(&mut res, self.exterior())?;
        for ii in self.interiors() {
            write_geo_linestring(&mut res, ii)?;
        }
        Ok(res)
    }
}

impl AsWkb for geo::MultiPolygon<f64> {
    fn as_wkb(&self, srid: Option<u32>) -> Result<Vec<u8>> {
        if self.0.len() == 1 {
            return self.0[0].as_wkb(srid);
        }

        let mut res = prep_wkb_alt(srid, 6, 0)?;
        for p in self.iter() {
            res.extend(p.as_wkb(srid.clone())?);
        }
        Ok(res)
    }
}

