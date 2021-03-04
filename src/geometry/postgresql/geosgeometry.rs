use crate::geometry::{
    ComplicatedPolygonGeometry, LinestringGeometry, PointGeometry, PolygonPart,
    SimplePolygonGeometry, XY,
};
use geos_sys::*;

use std::io::{Error, ErrorKind, Result};

pub struct GeosGeometry {
    handle: GEOSContextHandle_t,
    geometry: *mut GEOSGeometry,
}

unsafe fn make_coord_one(handle: GEOSContextHandle_t, p: &XY) -> *mut GEOSCoordSequence {
    let coords = GEOSCoordSeq_create_r(handle, 1, 2);
    GEOSCoordSeq_setX_r(handle, coords, 0, p.x);
    GEOSCoordSeq_setY_r(handle, coords, 0, p.y);
    coords
}

unsafe fn make_coords<Iter: Iterator<Item = XY>>(
    handle: GEOSContextHandle_t,
    len: usize,
    pts: Iter,
) -> *mut GEOSCoordSequence {
    let coords = GEOSCoordSeq_create_r(handle, len as u32, 2);
    for (i, p) in pts.enumerate() {
        GEOSCoordSeq_setX_r(handle, coords, i as u32, p.x);
        GEOSCoordSeq_setY_r(handle, coords, i as u32, p.y);
    }
    coords
}
unsafe fn write_wkb(handle: GEOSContextHandle_t, geom: *mut GEOSGeometry) -> Result<Vec<u8>> {
    if geom.is_null() {
        return Err(Error::new(ErrorKind::Other, "no geometry!"));
    }
    GEOSSetSRID_r(handle, geom, 3857);

    let writer = GEOSWKBWriter_create_r(handle);
    GEOSWKBWriter_setIncludeSRID_r(handle, writer, 1);
    GEOSWKBWriter_setByteOrder_r(handle, writer, 0); //GEOS_WKB_XDR);

    let mut sz = 0;
    let c = GEOSWKBWriter_write_r(handle, writer, geom, &mut sz);

    let mut res = Vec::new();
    if !c.is_null() {
        let temp = std::slice::from_raw_parts(c, sz);
        res.extend(temp);
        GEOSFree_r(handle, c as *mut core::ffi::c_void);
    }
    GEOSWKBWriter_destroy_r(handle, writer);
    Ok(res)
}

unsafe fn from_complicatedpolygon_part(
    handle: GEOSContextHandle_t,
    part: &PolygonPart,
) -> Result<*mut GEOSGeometry> {
    let coords = make_coords(
        handle,
        part.exterior.len(),
        part.exterior.lonlats_iter().map(|l| l.to_xy(true)),
    );
    let outer = GEOSGeom_createLinearRing_r(handle, coords);

    let mut inners = Vec::new();
    for ii in &part.interiors {
        let coords = make_coords(handle, ii.len(), ii.lonlats_iter().map(|l| l.to_xy(true)));
        inners.push(GEOSGeom_createLinearRing_r(handle, coords));
    }

    let res = GEOSGeom_createPolygon_r(handle, outer, inners.as_mut_ptr(), inners.len() as u32);
    if res.is_null() {
        return Err(Error::new(
            ErrorKind::Other,
            "GEOSGeom_createPolygon_r failed",
        ));
    }
    Ok(res)
}

impl GeosGeometry {
    pub fn from_point(pt: &PointGeometry) -> Result<GeosGeometry> {
        unsafe {
            let handle = GEOS_init_r();
            let coords = make_coord_one(handle, &pt.lonlat.to_xy(true));
            let geometry = GEOSGeom_createPoint_r(handle, coords);
            if geometry.is_null() {
                GEOS_finish_r(handle);
                return Err(Error::new(ErrorKind::Other, "GEOSGeom_createPoint failed"));
            }
            Ok(GeosGeometry { handle, geometry })
        }
    }

    pub fn from_linestring(ln: &LinestringGeometry) -> Result<GeosGeometry> {
        unsafe {
            let handle = GEOS_init_r();
            let coords = make_coords(
                handle,
                ln.lonlats.len(),
                ln.lonlats.iter().map(|l| l.to_xy(true)),
            );
            let geometry = GEOSGeom_createLineString_r(handle, coords);
            if geometry.is_null() {
                GEOS_finish_r(handle);
                return Err(Error::new(
                    ErrorKind::Other,
                    "GEOSGeom_createLineString failed",
                ));
            }
            Ok(GeosGeometry { handle, geometry })
        }
    }

    pub fn from_simplepolygon(ln: &SimplePolygonGeometry) -> Result<GeosGeometry> {
        unsafe {
            let handle = GEOS_init_r();
            let coords = make_coords(
                handle,
                ln.lonlats.len(),
                ln.lonlats.iter().map(|l| l.to_xy(true)),
            );

            let outer = GEOSGeom_createLinearRing_r(handle, coords);
            let mut v = Vec::new();
            let geometry = GEOSGeom_createPolygon_r(handle, outer, v.as_mut_ptr(), 0);
            if geometry.is_null() {
                GEOS_finish_r(handle);
                return Err(Error::new(ErrorKind::Other, "GEOSGeom_createPoint failed"));
            }
            Ok(GeosGeometry { handle, geometry })
        }
    }

    pub fn from_complicatedpolygon(cp: &ComplicatedPolygonGeometry) -> Result<GeosGeometry> {
        if cp.parts.is_empty() {
            return Err(Error::new(
                ErrorKind::Other,
                "empty complicatedpolygongeometry!",
            ));
        }
        unsafe {
            let handle = GEOS_init_r();

            let geometry = if cp.parts.len() == 0 {
                //can't happen
                let mut v = Vec::new();
                GEOSGeom_createCollection_r(handle, 7 as i32, v.as_mut_ptr(), 0)
            } else if cp.parts.len() == 1 {
                match from_complicatedpolygon_part(handle, &cp.parts[0]) {
                    Ok(g) => g,
                    Err(e) => {
                        GEOS_finish_r(handle);
                        return Err(e);
                    }
                }
            } else {
                let mut parts = Vec::new();
                for p in &cp.parts {
                    match from_complicatedpolygon_part(handle, p) {
                        Ok(poly) => {
                            parts.push(poly);
                        }
                        Err(_) => {
                            // just skip this part

                            /*println!("\npart {} / {} failed {:?}\n", parts.len(), cp.parts.len(),e);
                            for p in parts {
                                GEOSGeom_destroy_r(handle, p);
                            }
                            GEOS_finish_r(handle);
                            return Err(e);*/
                        }
                    }
                }
                if parts.is_empty() {
                    GEOS_finish_r(handle);
                    return Err(Error::new(
                        ErrorKind::Other,
                        "complicatedpolygongeometry with no valid parts",
                    ));
                }
                GEOSGeom_createCollection_r(
                    handle,
                    6 as i32,
                    parts.as_mut_ptr(),
                    parts.len() as u32,
                )
            };

            if geometry.is_null() {
                GEOS_finish_r(handle);
                return Err(Error::new(
                    ErrorKind::Other,
                    "GEOSGeom_createCollection_r failed",
                ));
            }

            Ok(GeosGeometry { handle, geometry })
        }
    }

    pub fn is_valid(&self) -> bool {
        unsafe { GEOSisValid(self.geometry) != 0 }
    }

    pub fn validate(&mut self) -> bool {
        unsafe {
            let t = GEOSGeomTypeId_r(self.handle, self.geometry);
            if (t == 3) || (t == 6) {
                if GEOSisValid_r(self.handle, self.geometry) == 0 {
                    let result = GEOSMakeValid_r(self.handle, self.geometry);
                    if !result.is_null() {
                        GEOSGeom_destroy_r(self.handle, self.geometry);
                        self.geometry = result;
                        return true;
                    } else {
                        return false;
                    }
                }
            }
            return true;
        }
    }
    pub fn simplify(&mut self, tol: f64) -> bool {
        unsafe {
            let result = GEOSTopologyPreserveSimplify_r(self.handle, self.geometry, tol);
            if !result.is_null() {
                GEOSGeom_destroy_r(self.handle, self.geometry);
                self.geometry = result;
                true
            } else {
                false
            }
        }
    }

    pub fn wkb(&self) -> Result<Vec<u8>> {
        unsafe { write_wkb(self.handle, self.geometry) }
    }
    pub fn point_wkb(&self) -> Result<Vec<u8>> {
        unsafe {
            let point = GEOSPointOnSurface_r(self.handle, self.geometry);
            if point.is_null() {
                return Err(Error::new(ErrorKind::Other, "GEOSPointOnSurface failed"));
            }
            let wkb = write_wkb(self.handle, point);
            GEOSGeom_destroy_r(self.handle, point);
            wkb
        }
    }

    pub fn boundary_line_wkb(&self) -> Result<Vec<u8>> {
        unsafe {
            let line = GEOSBoundary_r(self.handle, self.geometry);
            if line.is_null() {
                return Err(Error::new(ErrorKind::Other, "GEOSPointOnSurface failed"));
            }
            let wkb = write_wkb(self.handle, line);
            GEOSGeom_destroy_r(self.handle, line);
            wkb
        }
    }
}

impl Drop for GeosGeometry {
    fn drop(&mut self) {
        unsafe {
            if !self.geometry.is_null() {
                GEOSGeom_destroy_r(self.handle, self.geometry);
            }
            GEOS_finish_r(self.handle);
        }
    }
}

/*
class GeosGeometryImpl : public GeosGeometry {
    public:
        GeosGeometryImpl(std::shared_ptr<oqt::BaseGeometry> geom, bool round) {

            handle = GEOS_init_r();



            if (geom->Type() == oqt::ElementType::Point) {
                geometry = make_point(std::dynamic_pointer_cast<Point>(geom), round);
            } else if (geom->Type() == oqt::ElementType::Linestring) {
                geometry = make_linestring(std::dynamic_pointer_cast<Linestring>(geom), round);
            }  else if (geom->Type() == oqt::ElementType::SimplePolygon) {
                geometry = make_simplepolygon(std::dynamic_pointer_cast<SimplePolygon>(geom), round);
            }  else if (geom->Type() == oqt::ElementType::ComplicatedPolygon) {
                geometry = make_complicatedpolygon(std::dynamic_pointer_cast<ComplicatedPolygon>(geom), round);
            } else {
                geometry = GEOSGeom_createEmptyCollection_r(handle, 7);
            }
        }

        GeosGeometryImpl(std::shared_ptr<ComplicatedPolygon> geom, size_t part, bool round) {
            handle = GEOS_init_r();
            geometry = make_complicatedpolygon_part(geom->Parts().at(part), round);
        }

        virtual ~GeosGeometryImpl() {
            if (geometry) {
                GEOSGeom_destroy_r(handle, geometry);
            }
            GEOS_finish_r(handle);
        };

        void validate() {

            //MakeValid not present yet in released versions of libgeos [May 2019]
            //GEOSGeometry* result = GEOSMakeValid_r(handle, geometry);



            int t = GEOSGeomTypeId_r(handle,geometry);
            if ((t==3) || (t==6)) {
                if (!GEOSisValid_r(handle, geometry)) {
                    //GEOSGeometry* result = GEOSBuffer_r(handle, geometry, 0, 16);
                    GEOSGeometry* result = GEOSMakeValid_r(handle, geometry);
                    if (result) {
                        GEOSGeom_destroy_r(handle, geometry);
                        geometry=result;
                    }
                }
            }
        }
        void simplify(double tol) {
            GEOSGeometry* result = GEOSTopologyPreserveSimplify_r(handle, geometry, tol);
            if (result) {
                GEOSGeom_destroy_r(handle, geometry);
                geometry=result;
            }
        }


        std::string Wkb() {
            return write_wkb(geometry);
        }


        std::string PointWkb() {
            GEOSGeometry* point = GEOSPointOnSurface_r(handle, geometry);
            auto wkb= write_wkb(point);
            GEOSGeom_destroy_r(handle, point);
            return wkb;
        }

        std::string BoundaryLineWkb() {
            GEOSGeometry* line = GEOSBoundary_r(handle, geometry);
            auto wkb= write_wkb(line);
            GEOSGeom_destroy_r(handle, line);
            return wkb;
        }

    private:
        GEOSContextHandle_t handle;
        GEOSGeometry* geometry;


        std::string write_wkb(GEOSGeometry* geom) {
            //GEOS_setWKBByteOrder_r(handle, GEOS_WKB_XDR);
            GEOSSetSRID_r(handle, geom, 3857);

            GEOSWKBWriter* writer = GEOSWKBWriter_create_r(handle);
            GEOSWKBWriter_setIncludeSRID_r(handle, writer, 1);
            GEOSWKBWriter_setByteOrder_r(handle, writer, GEOS_WKB_XDR);


            std::string s;
            size_t sz;
            unsigned char* c = GEOSWKBWriter_write_r(handle, writer, geom, &sz);


            if (c) {
                s = std::string(reinterpret_cast<const char*>(c), sz);
                GEOSFree_r(handle, c);
            }
            GEOSWKBWriter_destroy_r(handle, writer);
            return s;


        }


        GEOSGeometry* make_point(std::shared_ptr<Point> pt, bool round) {


            GEOSCoordSequence* coords = make_coords({pt->LonLat()}, round);
            return GEOSGeom_createPoint_r(handle, coords);
        }

        GEOSCoordSequence* make_coords(const std::vector<LonLat>& lls, bool round) {

            GEOSCoordSequence* coords = GEOSCoordSeq_create_r(handle, lls.size(), 2);
            for (size_t i=0; i < lls.size(); i++) {
                const auto& ll = lls[i];
                auto p = forward_transform(ll.lon, ll.lat);
                if (round) {
                    p = p.round_2dp();
                }
                GEOSCoordSeq_setX_r(handle, coords, i, p.x);
                GEOSCoordSeq_setY_r(handle, coords, i, p.y);
            }
            return coords;
        }

        GEOSGeometry* make_linestring(std::shared_ptr<Linestring> line, bool round) {
            GEOSCoordSequence* coords = make_coords(line->LonLats(),round);
            return GEOSGeom_createLineString_r(handle, coords);
        }

        GEOSGeometry* make_simplepolygon(std::shared_ptr<SimplePolygon> line, bool round) {
            GEOSGeometry* outer = GEOSGeom_createLinearRing_r(handle, make_coords(line->LonLats(),round));
            return GEOSGeom_createPolygon_r(handle, outer, nullptr, 0);
        }

        GEOSGeometry* make_complicatedpolygon_part(const PolygonPart& part, bool round) {
            GEOSGeometry* outer = GEOSGeom_createLinearRing_r(handle, make_coords(ringpart_lonlats(part.outer), round));
            if (part.inners.empty()) {
                return GEOSGeom_createPolygon_r(handle, outer, nullptr, 0);
            }

            std::vector<GEOSGeometry*> inners;
            for (const auto& inn: part.inners) {
                inners.push_back(GEOSGeom_createLinearRing_r(handle, make_coords(ringpart_lonlats(inn), round)));
            }

            return GEOSGeom_createPolygon_r(handle, outer, &inners[0], inners.size());
        }

        GEOSGeometry* make_complicatedpolygon(std::shared_ptr<ComplicatedPolygon> poly, bool round) {

            if (poly->Parts().size()==0) {
                return GEOSGeom_createEmptyCollection_r(handle, 7);
            } else if (poly->Parts().size()==1) {

                return make_complicatedpolygon_part(poly->Parts()[0], round);
            }
            std::vector<GEOSGeometry*> geoms;
            for (const auto& pt: poly->Parts()) {
                geoms.push_back(make_complicatedpolygon_part(pt, round));
            }
            return GEOSGeom_createCollection_r(handle, 6, &geoms[0], geoms.size());
        }

};
*/
