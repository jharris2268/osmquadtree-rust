use simple_protocolbuffers::{
    data_length, pack_data, pack_int, pack_value, read_packed_int, un_zig_zag, zig_zag, IterTags,
    PbfTag,
};

use crate::elements::info::Info;
use crate::elements::quadtree::Quadtree;
use crate::elements::tags::Tag;
use crate::elements::traits::*;

use core::cmp::Ordering;
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind, Result};

pub fn read_common<'a, T: SetCommon + WithId>(
    obj: &mut T,
    strings: &Vec<String>,
    data: &'a [u8],
    minimal: bool,
) -> Result<Vec<PbfTag<'a>>> {
    let mut kk = Vec::new();
    let mut vv = Vec::new();
    let mut rem = Vec::new();

    for t in IterTags::new(data) {
        match t {
            PbfTag::Value(1, i) => {
                obj.set_id(i as i64);
            }
            PbfTag::Data(4, d) => {
                if !minimal {
                    obj.set_info(Info::read(strings, d)?);
                }
            }

            PbfTag::Data(2, d) => {
                if !minimal {
                    if !kk.is_empty() {
                        return Err(Error::new(ErrorKind::Other, "more than one keys??"));
                    }
                    kk = read_packed_int(d);
                }
            }
            PbfTag::Data(3, d) => {
                if !minimal {
                    if !vv.is_empty() {
                        return Err(Error::new(ErrorKind::Other, "more than one keys??"));
                    }
                    vv = read_packed_int(d);
                }
            }
            PbfTag::Value(20, q) => {
                obj.set_quadtree(Quadtree::new(un_zig_zag(q)));
            }
            x => {
                rem.push(x);
            }
        }
    }
    if kk.len() != vv.len() {
        return Err(Error::new(
            ErrorKind::Other,
            format!(
                "tags don't match: [id: {}] {} // {}",
                obj.get_id(),
                kk.len(),
                vv.len()
            ),
        ));
    }
    if kk.len() > 0 {
        let mut tags = Vec::new();
        tags.reserve(kk.len());
        for i in 0..kk.len() {
            tags.push(Tag::new(
                strings[kk[i] as usize].clone(),
                strings[vv[i] as usize].clone(),
            ));
        }
        obj.set_tags(tags);
    }
    Ok(rem)
}

pub fn pack_length(
    tags: &Vec<Tag>,
    _pack_strings: &mut Box<PackStringTable>,
    _include_qts: bool,
) -> usize {
    70 + 10 * tags.len()
}

pub fn pack_head(
    id: &i64,
    info: &Option<Info>,
    tags: &Vec<Tag>,
    res: &mut Vec<u8>,
    pack_strings: &mut Box<PackStringTable>,
) -> Result<()> {
    pack_value(res, 1, *id as u64);
    if !tags.is_empty() {
        pack_data(
            res,
            2,
            &pack_int(tags.iter().map(|t| pack_strings.call(&t.key))),
        );
        pack_data(
            res,
            3,
            &pack_int(tags.iter().map(|t| pack_strings.call(&t.val))),
        );
    }
    match info {
        Some(info) => {
            pack_data(res, 4, &info.pack(pack_strings)?);
        }
        None => {}
    }
    Ok(())
}

pub fn pack_tail(quadtree: &Quadtree, res: &mut Vec<u8>, include_qts: bool) -> Result<()> {
    if include_qts && quadtree.as_int() >= 0 {
        pack_value(res, 20, zig_zag(quadtree.as_int()));
    }
    Ok(())
}

pub fn common_cmp(
    left_id: &i64,
    left_info: &Option<Info>,
    left_changetype: &Changetype,
    right_id: &i64,
    right_info: &Option<Info>,
    right_changetype: &Changetype,
) -> Ordering {
    let a = left_id.cmp(right_id);
    if a != Ordering::Equal {
        return a;
    }

    let b = left_info
        .as_ref()
        .unwrap()
        .version
        .cmp(&right_info.as_ref().unwrap().version);
    if b != Ordering::Equal {
        return b;
    }

    left_changetype.cmp(right_changetype)
}

pub fn common_eq(
    left_id: &i64,
    left_info: &Option<Info>,
    left_changetype: &Changetype,
    right_id: &i64,
    right_info: &Option<Info>,
    right_changetype: &Changetype,
) -> bool {
    if left_id != right_id {
        return false;
    }

    if left_info.as_ref().unwrap().version != right_info.as_ref().unwrap().version {
        return false;
    }

    left_changetype == right_changetype
}

pub struct PackStringTable {
    strings: BTreeMap<String, u64>,
}

impl PackStringTable {
    pub fn new() -> PackStringTable {
        let mut strings = BTreeMap::new();
        strings.insert(String::from("(*%£("), 0);

        PackStringTable { strings: strings }
    }

    pub fn call(&mut self, s: &String) -> u64 {
        if !self.strings.contains_key(s) {
            let x = self.strings.len() as u64;
            self.strings.insert(s.clone(), x);
            x
        } else {
            *self.strings.get(s).unwrap()
        }
    }

    pub fn len(&self) -> usize {
        let mut l = data_length(1, 0);
        for (s, t) in &self.strings {
            if *t != 0 {
                l += data_length(1, s.as_bytes().len());
            }
        }
        l
    }
    pub fn pack(&self) -> Vec<u8> {
        let mut m = vec![String::new(); self.strings.len()];
        let mut tl = 0;

        for (s, t) in &self.strings {
            if *t == 0 {
                m[0] = String::new()
            } else {
                m[*t as usize] = s.clone();
                tl += data_length(1, s.as_bytes().len());
            }
        }

        let mut r = Vec::with_capacity(tl);
        for t in m {
            pack_data(&mut r, 1, t.as_bytes());
        }
        r
    }
}
