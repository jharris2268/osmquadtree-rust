mod osmquadtree {
    pub use super::super::super::*;
}

use super::node;
use osmquadtree::read_pbf;
use super::quadtree;
use super::tags;
use super::info::Info;
use super::common::{Changetype,PackStringTable};
use osmquadtree::write_pbf;
use std::io::{Result,Error,ErrorKind};
pub struct Dense {}


impl Dense {
    pub fn read(changetype: Changetype, strings: &Vec<String>, data: &[u8], minimal: bool) -> Result<Vec<node::Node>> {
        
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
        
        
        for x in read_pbf::IterTags::new(&data,0) {
            match x {
                read_pbf::PbfTag::Data(1, d) => ids = read_pbf::read_delta_packed_int(&d),
                read_pbf::PbfTag::Data(5, d) => {
                    if !minimal {
                        for y in read_pbf::IterTags::new(&d, 0) {
                            match y {
                                read_pbf::PbfTag::Data(1, d) => vs = read_pbf::read_packed_int(&d), //version NOT delta packed
                                read_pbf::PbfTag::Data(2, d) => ts = read_pbf::read_delta_packed_int(&d),
                                read_pbf::PbfTag::Data(3, d) => cs = read_pbf::read_delta_packed_int(&d),
                                read_pbf::PbfTag::Data(4, d) => ui = read_pbf::read_delta_packed_int(&d),
                                read_pbf::PbfTag::Data(5, d) => us = read_pbf::read_delta_packed_int(&d),
                                _ => return Err(Error::new(ErrorKind::Other,format!("unexpected {:?} for dense nodes info", y))),
                            }
                        }
                    }
                },
                read_pbf::PbfTag::Data(8, d) => lats = read_pbf::read_delta_packed_int(&d),
                read_pbf::PbfTag::Data(9, d) => lons = read_pbf::read_delta_packed_int(&d),
                read_pbf::PbfTag::Data(10, d) => {
                    if !minimal {
                        kvs = read_pbf::read_packed_int(&d)
                    }
                },
                read_pbf::PbfTag::Data(20, d) => qts = read_pbf::read_delta_packed_int(&d),
                _ => return Err(Error::new(ErrorKind::Other,format!("unexpected {:?} for dense nodes", x))),
            }
        }
        
        if ids.len() == 0 { return Ok(res); }
        if lats.len() > 0 && lats.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} lats", ids.len(), lats.len()))); }
        if lons.len() > 0 && lons.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} lons", ids.len(), lons.len()))); }
        if qts.len() > 0 && qts.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} qts", ids.len(), qts.len()))); }
        
        if !minimal {
            if vs.len() > 0 && vs.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} infos", ids.len(), vs.len()))); }
            if ts.len() > 0 && ts.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} timestamps", ids.len(), ts.len()))); }
            if cs.len() > 0 && cs.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} changesets", ids.len(), cs.len()))); }
            if ui.len() > 0 && ui.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} user_ids", ids.len(), ui.len()))); }
            if us.len() > 0 && us.len()!=ids.len() { return Err(Error::new(ErrorKind::Other,format!("dense nodes: {} ids but {} users", ids.len(), us.len()))); }
        }
        
        let mut kvs_idx=0;
        for i in 0..ids.len() {
            let mut nd = node::Node::new(ids[i], changetype);
            if lats.len()>0 { nd.lat = lats[i]; }
            if lons.len()>0 { nd.lon = lons[i]; }
            if qts.len()>0 { nd.quadtree = quadtree::Quadtree::new(qts[i]); }
            if !minimal {
                let mut info = Info::new();
                if vs.len()>0 { info.version = vs[i] as i64; }
                if ts.len()>0 { info.timestamp = ts[i]; }
                if cs.len()>0 { info.changeset = cs[i]; }
                if ui.len()>0 { info.user_id = ui[i]; }
                if us.len()>0 { info.user = strings[us[i] as usize].clone(); }
                nd.info = Some(info);
            
                while kvs_idx < kvs.len() && kvs[kvs_idx]!=0 {
                    nd.tags.push(
                        tags::Tag::new(
                            strings[kvs[kvs_idx] as usize].clone(),
                            strings[kvs[kvs_idx+1] as usize].clone()));
                    kvs_idx+=2;
                }
                if kvs_idx < kvs.len() { kvs_idx+=1; }
            }
            res.push(nd);
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
                },
                None => {
                    vsv.push(0);
                    tsv.push(0);
                    csv.push(0);
                    uiv.push(0);
                    usv.push(0);
                }
            }
        }
        let vs = write_pbf::pack_int_ref(vsv.iter());
        let ts = write_pbf::pack_delta_int_ref(tsv.iter());
        let cs = write_pbf::pack_delta_int_ref(csv.iter());
        let ui = write_pbf::pack_delta_int_ref(uiv.iter());
        let us = write_pbf::pack_delta_int_ref(usv.iter());
        
        /*let vs = write_pbf::pack_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).version as u64 }));
        let ts = write_pbf::pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).timestamp }));
        let cs = write_pbf::pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).changeset }));
        let ui = write_pbf::pack_delta_int(feats.iter().map( |n| { n.info.as_ref().unwrap_or_else(default_info).user_id }));
        let us = write_pbf::pack_delta_int(feats.iter().map( |n| { pack_strings.call(&n.info.as_ref().unwrap_or_else(default_info).user) as i64 }));*/
        
        let l =   write_pbf::data_length(1,vs.len()) + write_pbf::data_length(2,cs.len())
                + write_pbf::data_length(3,ts.len()) + write_pbf::data_length(4,ui.len())
                + write_pbf::data_length(3,us.len());
            
        let mut res = Vec::with_capacity(l);
        write_pbf::pack_data(&mut res, 1, &vs);
        write_pbf::pack_data(&mut res, 2, &ts);
        write_pbf::pack_data(&mut res, 3, &cs);
        write_pbf::pack_data(&mut res, 4, &ui);
        write_pbf::pack_data(&mut res, 5, &us);
        Ok(res)
    }
        
    
    pub fn pack(feats: &[node::Node], pack_strings: &mut Box<PackStringTable>, include_qts: bool) -> Result<Vec<u8>> {
        
        let ids = write_pbf::pack_delta_int(feats.iter().map( |n| { n.id }));
        let lats = write_pbf::pack_delta_int(feats.iter().map( |n| { n.lat }));
        let lons = write_pbf::pack_delta_int(feats.iter().map( |n| { n.lon }));
        
        let mut qts: Option<Vec<u8>> = None;
        if include_qts {
            qts=Some(write_pbf::pack_delta_int(feats.iter().map( |n| { n.quadtree.as_int() })));
        }
        
        
        
        let mut tl = 0;
        for f in feats {
            tl += 1 + 2*f.tags.len();
        }
        
        let mut tgs = Vec::with_capacity(tl);
        for f in feats { 
            let kk: Vec<u64> = f.tags.iter().map(|t| { pack_strings.call(&t.key) }).collect();
            let vv: Vec<u64> = f.tags.iter().map(|t| { pack_strings.call(&t.val) }).collect();
            
            for (k,v) in kk.iter().zip(vv) {
                tgs.push(*k);
                tgs.push(v);
            }   
            /*for t in &f.common.tags {
                tgs.push(pack_strings.call(&t.key));
                tgs.push(pack_strings.call(&t.val));
            }*/
            match &f.info {
                Some(info) => { pack_strings.call(&info.user); },
                None => {}
            }
            tgs.push(0);
        }
        let infs = Self::pack_info(feats, pack_strings)?;
        
        let tt = write_pbf::pack_int_ref(tgs.iter());
        
        let mut l = write_pbf::data_length(1, ids.len()) + write_pbf::data_length(5, infs.len());
        l += write_pbf::data_length(8, lats.len()) + write_pbf::data_length(9, lons.len());
        l += write_pbf::data_length(10, tgs.len());
        if include_qts {
            l += write_pbf::data_length(20, qts.as_ref().unwrap().len());
        }
        
        let mut res = Vec::with_capacity(l);
        
        write_pbf::pack_data(&mut res, 1, &ids);
        write_pbf::pack_data(&mut res, 5, &infs);
        write_pbf::pack_data(&mut res, 8, &lats);
        write_pbf::pack_data(&mut res, 9, &lons);
        write_pbf::pack_data(&mut res, 10, &tt);
        if include_qts {
            write_pbf::pack_data(&mut res, 20, qts.as_ref().unwrap());
        }
        Ok(res)
    }
}
