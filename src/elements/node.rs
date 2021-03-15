use simple_protocolbuffers::{un_zig_zag, PbfTag};

use crate::elements::common::{common_cmp, common_eq, read_common, PackStringTable};
use crate::elements::info::Info;
use crate::elements::quadtree::Quadtree;
use crate::elements::tags::Tag;
use crate::elements::traits::*;

use core::cmp::Ordering;
use std::io::{Error, ErrorKind, Result};



/// Representation of openstreetmap node element.
/// See [https://wiki.openstreetmap.org/wiki/Node](https://wiki.openstreetmap.org/wiki/Node).
/// Longitude and latitude are represented as integer values of 10^-7 degrees.
#[derive(Debug, Eq, Clone)]
pub struct Node {
    pub id: i64,
    pub changetype: Changetype,
    pub info: Option<Info>,
    pub tags: Vec<Tag>,
    
    pub lat: i32,
    pub lon: i32,
    
    pub quadtree: Quadtree,
}

impl Node {
    /// Returns a new Node. Latitude, longitude, tags and metadata must be set by user
    pub fn new(id: i64, changetype: Changetype) -> Node {
        Node {
            id: id,
            changetype: changetype,
            info: None,
            tags: Vec::new(),
            lat: 0,
            lon: 0,
            quadtree: Quadtree::empty(),
        }
    }

    pub fn read(
        changetype: Changetype,
        strings: &Vec<String>,
        data: &[u8],
        minimal: bool,
    ) -> Result<Node> {
        let mut nd = Node::new(0, changetype);

        let rem = read_common(&mut nd, &strings, data, minimal)?;

        for t in rem {
            match t {
                PbfTag::Value(8, lat) => nd.lat = un_zig_zag(lat) as i32,
                PbfTag::Value(9, lon) => nd.lon = un_zig_zag(lon) as i32,
                _ => {}
            }
        }
        Ok(nd)
    }
    
    pub fn pack(
        &self,
        _prep_strings: &mut Box<PackStringTable>,
        _include_qts: bool,
    ) -> Result<Vec<u8>> {
        Err(Error::new(ErrorKind::Other, "not impl"))
    }
}

impl WithType for Node {
    fn get_type(&self) -> ElementType {
        ElementType::Node
    }
}

impl WithId for Node {
    fn get_id(&self) -> i64 {
        self.id
    }
}

impl WithInfo for Node {
    fn get_info<'a>(&'a self) -> &Option<Info> {
        &self.info
    }
}

impl WithTags for Node {
    fn get_tags<'a>(&'a self) -> &'a [Tag] {
        &self.tags
    }
}

impl WithQuadtree for Node {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl SetCommon for Node {
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

impl Ord for Node {
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

impl PartialOrd for Node {
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

impl PartialEq for Node {
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
