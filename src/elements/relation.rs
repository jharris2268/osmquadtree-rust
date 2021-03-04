use simple_protocolbuffers::{
    pack_data, pack_delta_int, pack_int, read_delta_packed_int, read_packed_int, PbfTag,
};

use crate::elements::common::{
    common_cmp, common_eq, pack_head, pack_length, pack_tail, read_common, PackStringTable,
};
use crate::elements::info::Info;
use crate::elements::quadtree::Quadtree;
use crate::elements::tags::Tag;
use crate::elements::traits::*;
use crate::elements::IdSet;
use core::cmp::Ordering;
use std::io::{Error, ErrorKind, Result};

#[derive(Debug, Eq, Clone)]
pub struct Relation {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    pub members: Vec<Member>,
    pub quadtree: Quadtree,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Member {
    pub role: String,
    pub mem_type: ElementType,
    pub mem_ref: i64,
}
impl Member {
    pub fn new(role: String, mem_type: ElementType, mem_ref: i64) -> Member {
        Member {
            role,
            mem_type,
            mem_ref,
        }
    }
}
impl Relation {
    pub fn new(id: i64, changetype: Changetype) -> Relation {
        Relation {
            id: id,
            changetype: changetype,
            info: None,
            tags: Vec::new(),
            members: Vec::new(),
            quadtree: Quadtree::empty(),
        }
    }

    pub fn read(
        changetype: Changetype,
        strings: &Vec<String>,
        data: &[u8],
        minimal: bool,
    ) -> Result<Relation> {
        let mut rel = Relation::new(0, changetype);

        let rem = read_common(&mut rel, &strings, data, minimal)?;

        let mut roles = Vec::new();
        let mut refs = Vec::new();
        let mut types = Vec::new();

        for t in rem {
            match t {
                PbfTag::Data(8, d) => {
                    if !minimal {
                        roles = read_packed_int(&d)
                    }
                }

                PbfTag::Data(9, d) => refs = read_delta_packed_int(&d),
                PbfTag::Data(10, d) => types = read_packed_int(&d),
                _ => {}
            }
        }
        if types.len() != refs.len() || (!minimal && types.len() != roles.len()) {
            return Err(Error::new(ErrorKind::Other, "member lens don't match"));
        }
        if types.len() != 0 {
            for i in 0..types.len() {
                if minimal {
                    rel.members.push(Member {
                        role: String::from(""),
                        mem_type: ElementType::from_int(types[i]),
                        mem_ref: refs[i],
                    });
                } else {
                    let m = Member {
                        role: strings[roles[i] as usize].clone(),
                        mem_type: ElementType::from_int(types[i]),
                        mem_ref: refs[i],
                    };
                    rel.members.push(m);
                }
            }
        }

        Ok(rel)
    }
    pub fn pack(
        &self,
        pack_strings: &mut Box<PackStringTable>,
        include_qts: bool,
    ) -> Result<Vec<u8>> {
        let l = pack_length(&self.tags, pack_strings, include_qts) + self.members.len() * 10 + 6;

        let mut res = Vec::with_capacity(l);
        pack_head(&self.id, &self.info, &self.tags, &mut res, pack_strings)?;

        if !self.members.is_empty() {
            let roles = pack_int(self.members.iter().map(|m| pack_strings.call(&m.role)));
            let refs = pack_delta_int(self.members.iter().map(|m| m.mem_ref));
            let types = pack_int(self.members.iter().map(|m| m.mem_type.as_int()));

            pack_data(&mut res, 8, &roles);
            pack_data(&mut res, 9, &refs);
            pack_data(&mut res, 10, &types);
        }
        pack_tail(&self.quadtree, &mut res, include_qts)?;
        Ok(res)

        //Err(Error::new(ErrorKind::Other, "not impl"))
    }

    pub fn filter_relations(&mut self, ids: &dyn IdSet) -> bool {
        let nm = self.members.len();
        for m in std::mem::take(&mut self.members) {
            if ids.contains(m.mem_type.clone(), m.mem_ref) {
                self.members.push(m);
            }
        }
        self.members.len() != nm
    }
}
impl WithType for Relation {
    fn get_type(&self) -> ElementType {
        ElementType::Relation
    }
}

impl WithId for Relation {
    fn get_id(&self) -> i64 {
        self.id
    }
}

impl WithInfo for Relation {
    fn get_info<'a>(&'a self) -> &Option<Info> {
        &self.info
    }
}

impl WithTags for Relation {
    fn get_tags<'a>(&'a self) -> &'a [Tag] {
        &self.tags
    }
}

impl WithQuadtree for Relation {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl SetCommon for Relation {
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

impl Ord for Relation {
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
impl PartialOrd for Relation {
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

impl PartialEq for Relation {
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
