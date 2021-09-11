use simple_protocolbuffers::{
    data_length, pack_data, pack_delta_int_ref, pack_value, read_delta_packed_int, PbfTag,
};

use crate::elements::common::{
    common_cmp, common_eq, pack_head, pack_length, pack_tail, read_common, PackStringTable,
};
use crate::elements::info::Info;
use crate::elements::quadtree::Quadtree;
use crate::elements::tags::Tag;
use crate::elements::traits::*;

use core::cmp::Ordering;
use std::io::Result;

#[derive(Debug, Eq, Clone,serde::Serialize)]
pub struct Way {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub refs: Vec<i64>,

    pub quadtree: Quadtree,
}

impl Way {
    pub fn new(id: i64, changetype: Changetype) -> Way {
        Way {
            id: id,
            changetype: changetype,
            info: None,
            tags: Vec::new(),
            refs: Vec::new(),
            quadtree: Quadtree::empty(),
        }
    }
    pub fn read(
        changetype: Changetype,
        strings: &Vec<String>,
        data: &[u8],
        minimal: bool,
    ) -> Result<Way> {
        let mut w = Way::new(0, changetype);

        let rem = read_common(&mut w, &strings, data, minimal)?;

        for t in rem {
            match t {
                PbfTag::Data(8, d) => w.refs = read_delta_packed_int(&d),
                _ => {}
            }
        }
        Ok(w)
    }
    pub fn pack(
        &self,
        pack_strings: &mut Box<PackStringTable>,
        include_qts: bool,
    ) -> Result<Vec<u8>> {
        let refs = pack_delta_int_ref(self.refs.iter());

        let l = pack_length(&self.tags, pack_strings, include_qts) + data_length(8, refs.len());

        let mut res = Vec::with_capacity(l);
        pack_head(&self.id, &self.info, &self.tags, &mut res, pack_strings)?;
        if refs.is_empty() {
            pack_value(&mut res, 8, 0);
        } else {
            pack_data(&mut res, 8, &refs);
        }
        pack_tail(&self.quadtree, &mut res, include_qts)?;
        Ok(res)

        //Err(Error::new(ErrorKind::Other, "not impl"))
    }
}

impl WithType for Way {
    fn get_type(&self) -> ElementType {
        ElementType::Way
    }
}

impl WithId for Way {
    fn get_id(&self) -> i64 {
        self.id
    }
}

impl WithInfo for Way {
    fn get_info<'a>(&'a self) -> &Option<Info> {
        &self.info
    }
}

impl WithTags for Way {
    fn get_tags<'a>(&'a self) -> &'a [Tag] {
        &self.tags
    }
}

impl WithQuadtree for Way {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl SetCommon for Way {
    fn set_id(&mut self, id: i64) {
        self.id = id;
    }
    fn set_info(&mut self, info: Info) {
        self.info = Some(info);
    }
    fn set_tags(&mut self, tags: Vec<Tag>) {
        self.tags = tags;
    }
    fn set_quadtree(&mut self, quadtree: Quadtree) {
        self.quadtree = quadtree;
    }
}

impl Ord for Way {
    fn cmp(&self, other: &Self) -> Ordering {
        common_cmp(
            &self.id,
            &self.info,
            &self.changetype,
            &other.id,
            &other.info,
            &other.changetype,
        )
    }
}
impl PartialOrd for Way {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(common_cmp(
            &self.id,
            &self.info,
            &self.changetype,
            &other.id,
            &other.info,
            &other.changetype,
        ))
    }
}

impl PartialEq for Way {
    fn eq(&self, other: &Self) -> bool {
        common_eq(
            &self.id,
            &self.info,
            &self.changetype,
            &other.id,
            &other.info,
            &other.changetype,
        )
    }
}
