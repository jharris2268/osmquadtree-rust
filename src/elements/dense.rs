use crate::elements::common::PackStringTable;
use crate::elements::info::Info;
use crate::elements::node;
use crate::elements::quadtree;
use crate::elements::tags;
use crate::elements::traits::{Changetype, ElementType};

use simple_protocolbuffers::{
    data_length, pack_data, pack_delta_int, pack_delta_int_ref, pack_int_ref,
    read_delta_packed_int, read_packed_int, IterTags, PbfTag,
};

use std::io::{Error, ErrorKind, Result};

use crate::elements::idset::IdSet;

fn check_id(id: i64, idset: Option<&dyn IdSet>) -> bool {
    match idset {
        None => true,
        Some(idset) => idset.contains(ElementType::Node, id),
    }
}

pub struct Dense {}

impl Dense {
    pub fn read(
        changetype: Changetype,
        strings: &Vec<String>,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<Vec<node::Node>> {
        let mut res = Vec::new();

        let mut ids = Vec::new();
        let mut lons = Vec::new();
        let mut lats = Vec::new();
        let mut kvs = Vec::new();
        let mut qts = Vec::new();
        let mut vs = Vec::new();
        let mut cs = Vec::new();
        let mut ts = Vec::new();
        let mut ui = Vec::new();
        let mut us = Vec::new();

        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, d) => ids = read_delta_packed_int(&d),
                PbfTag::Data(5, d) => {
                    if !minimal {
                        for y in IterTags::new(&d) {
                            match y {
                                PbfTag::Data(1, d) => vs = read_packed_int(&d), //version NOT delta packed
                                PbfTag::Data(2, d) => ts = read_delta_packed_int(&d),
                                PbfTag::Data(3, d) => cs = read_delta_packed_int(&d),
                                PbfTag::Data(4, d) => ui = read_delta_packed_int(&d),
                                PbfTag::Data(5, d) => us = read_delta_packed_int(&d),
                                _ => {
                                    return Err(Error::new(
                                        ErrorKind::Other,
                                        format!("unexpected {:?} for dense nodes info", y),
                                    ))
                                }
                            }
                        }
                    }
                }
                PbfTag::Data(8, d) => lats = read_delta_packed_int(&d),
                PbfTag::Data(9, d) => lons = read_delta_packed_int(&d),
                PbfTag::Data(10, d) => {
                    if !minimal {
                        kvs = read_packed_int(&d)
                    }
                }
                PbfTag::Data(20, d) => qts = read_delta_packed_int(&d),
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("unexpected {:?} for dense nodes", x),
                    ))
                }
            }
        }

        if ids.len() == 0 {
            return Ok(res);
        }
        if lats.len() > 0 && lats.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} lats", ids.len(), lats.len()),
            ));
        }
        if lons.len() > 0 && lons.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} lons", ids.len(), lons.len()),
            ));
        }
        if qts.len() > 0 && qts.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} qts", ids.len(), qts.len()),
            ));
        }

        if !minimal {
            if vs.len() > 0 && vs.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("dense nodes: {} ids but {} infos", ids.len(), vs.len()),
                ));
            }
            if ts.len() > 0 && ts.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("dense nodes: {} ids but {} timestamps", ids.len(), ts.len()),
                ));
            }
            if cs.len() > 0 && cs.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("dense nodes: {} ids but {} changesets", ids.len(), cs.len()),
                ));
            }
            if ui.len() > 0 && ui.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("dense nodes: {} ids but {} user_ids", ids.len(), ui.len()),
                ));
            }
            if us.len() > 0 && us.len() != ids.len() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("dense nodes: {} ids but {} users", ids.len(), us.len()),
                ));
            }
        }

        let mut kvs_idx = 0;
        for i in 0..ids.len() {
            if check_id(ids[i], idset) {
                let mut nd = node::Node::new(ids[i], changetype);
                if lats.len() > 0 {
                    nd.lat = lats[i] as i32;
                }
                if lons.len() > 0 {
                    nd.lon = lons[i] as i32;
                }
                if qts.len() > 0 {
                    nd.quadtree = quadtree::Quadtree::new(qts[i]);
                }
                if !minimal {
                    let mut info = Info::new();
                    if vs.len() > 0 {
                        info.version = vs[i] as i64;
                    }
                    if ts.len() > 0 {
                        info.timestamp = ts[i];
                    }
                    if cs.len() > 0 {
                        info.changeset = cs[i];
                    }
                    if ui.len() > 0 {
                        info.user_id = ui[i];
                    }
                    if us.len() > 0 {
                        info.user = strings[us[i] as usize].clone();
                    }
                    nd.info = Some(info);

                    while kvs_idx < kvs.len() && kvs[kvs_idx] != 0 {
                        nd.tags.push(tags::Tag::new(
                            strings[kvs[kvs_idx] as usize].clone(),
                            strings[kvs[kvs_idx + 1] as usize].clone(),
                        ));
                        kvs_idx += 2;
                    }
                    if kvs_idx < kvs.len() {
                        kvs_idx += 1;
                    }
                }
                res.push(nd);
            } else {
                while kvs_idx < kvs.len() && kvs[kvs_idx] != 0 {
                    kvs_idx += 2;
                }
                if kvs_idx < kvs.len() {
                    kvs_idx += 1;
                }
            }
        }

        return Ok(res);
    }

    fn pack_info(feats: &[node::Node], pack_strings: &mut Box<PackStringTable>) -> Result<Vec<u8>> {
        let mut vsv = Vec::with_capacity(feats.len());
        let mut tsv = Vec::with_capacity(feats.len());
        let mut csv = Vec::with_capacity(feats.len());
        let mut uiv = Vec::with_capacity(feats.len());
        let mut usv = Vec::with_capacity(feats.len());
        for n in feats {
            match &n.info {
                Some(info) => {
                    vsv.push(info.version as u64);
                    tsv.push(info.timestamp);
                    csv.push(info.changeset);
                    uiv.push(info.user_id);
                    usv.push(pack_strings.call(&info.user) as i64);
                }
                None => {
                    vsv.push(0);
                    tsv.push(0);
                    csv.push(0);
                    uiv.push(0);
                    usv.push(0);
                }
            }
        }
        let vs = pack_int_ref(vsv.iter());
        let ts = pack_delta_int_ref(tsv.iter());
        let cs = pack_delta_int_ref(csv.iter());
        let ui = pack_delta_int_ref(uiv.iter());
        let us = pack_delta_int_ref(usv.iter());

        /*let vs = pack_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).version as u64 }));
        let ts = pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).timestamp }));
        let cs = pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).changeset }));
        let ui = pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).user_id }));
        let us = pack_delta_int(feats.iter().map( |n| { pack_strings.call(&n.info.as_ref().unwrap_or_else(default_info).user) as i64 }));*/

        let l = data_length(1, vs.len())
            + data_length(2, cs.len())
            + data_length(3, ts.len())
            + data_length(4, ui.len())
            + data_length(3, us.len());

        let mut res = Vec::with_capacity(l);
        pack_data(&mut res, 1, &vs);
        pack_data(&mut res, 2, &ts);
        pack_data(&mut res, 3, &cs);
        pack_data(&mut res, 4, &ui);
        pack_data(&mut res, 5, &us);
        Ok(res)
    }

    pub fn pack(
        feats: &[node::Node],
        pack_strings: &mut Box<PackStringTable>,
        include_qts: bool,
    ) -> Result<Vec<u8>> {
        let ids = pack_delta_int(feats.iter().map(|n| n.id));
        let lats = pack_delta_int(feats.iter().map(|n| n.lat as i64));
        let lons = pack_delta_int(feats.iter().map(|n| n.lon as i64));

        let mut qts: Option<Vec<u8>> = None;
        if include_qts {
            qts = Some(pack_delta_int(feats.iter().map(|n| {
                if n.quadtree.as_int() < 0 {
                    0
                } else {
                    n.quadtree.as_int()
                }
            })));
        }

        let mut tl = 0;
        for f in feats {
            tl += 1 + 2 * f.tags.len();
        }

        let mut tgs = Vec::with_capacity(tl);
        for f in feats {
            let kk: Vec<u64> = f.tags.iter().map(|t| pack_strings.call(&t.key)).collect();
            let vv: Vec<u64> = f.tags.iter().map(|t| pack_strings.call(&t.val)).collect();

            for (k, v) in kk.iter().zip(vv) {
                tgs.push(*k);
                tgs.push(v);
            }
            /*for t in &f.common.tags {
                tgs.push(pack_strings.call(&t.key));
                tgs.push(pack_strings.call(&t.val));
            }*/
            match &f.info {
                Some(info) => {
                    pack_strings.call(&info.user);
                }
                None => {}
            }
            tgs.push(0);
        }
        let infs = Self::pack_info(feats, pack_strings)?;

        let tt = pack_int_ref(tgs.iter());

        let mut l = data_length(1, ids.len()) + data_length(5, infs.len());
        l += data_length(8, lats.len()) + data_length(9, lons.len());
        l += data_length(10, tgs.len());
        if include_qts {
            l += data_length(20, qts.as_ref().unwrap().len());
        }

        let mut res = Vec::with_capacity(l);

        pack_data(&mut res, 1, &ids);
        pack_data(&mut res, 5, &infs);
        pack_data(&mut res, 8, &lats);
        pack_data(&mut res, 9, &lons);
        pack_data(&mut res, 10, &tt);
        if include_qts {
            pack_data(&mut res, 20, qts.as_ref().unwrap());
        }
        Ok(res)
    }
}
