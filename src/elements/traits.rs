use crate::elements::{Info, Quadtree, Tag};
use crate::elements::{Node, Relation, Way};
use crate::geometry::{
    ComplicatedPolygonGeometry, LinestringGeometry, PointGeometry, SimplePolygonGeometry,
};

use std::cmp::Ordering;

#[derive(Debug,serde::Serialize)]
pub enum Element {
    Node(Node),
    Way(Way),
    Relation(Relation),
    PointGeometry(PointGeometry),
    LinestringGeometry(LinestringGeometry),
    SimplePolygonGeometry(SimplePolygonGeometry),
    ComplicatedPolygonGeometry(ComplicatedPolygonGeometry),
}

impl WithType for Element {
    fn get_type(&self) -> ElementType {
        match self {
            Element::Node(_) => ElementType::Node,
            Element::Way(_) => ElementType::Way,
            Element::Relation(_) => ElementType::Relation,
            Element::PointGeometry(_) => ElementType::PointGeometry,
            Element::LinestringGeometry(_) => ElementType::LinestringGeometry,
            Element::SimplePolygonGeometry(_) => ElementType::SimplePolygonGeometry,
            Element::ComplicatedPolygonGeometry(_) => ElementType::ComplicatedPolygonGeometry,
        }
    }
}

impl WithId for Element {
    fn get_id(&self) -> i64 {
        match self {
            Element::Node(n) => n.id,
            Element::Way(n) => n.id,
            Element::Relation(n) => n.id,
            Element::PointGeometry(n) => n.id,
            Element::LinestringGeometry(n) => n.id,
            Element::SimplePolygonGeometry(n) => n.id,
            Element::ComplicatedPolygonGeometry(n) => n.id,
        }
    }
}
impl PartialEq for Element {
    fn eq(&self, other: &Self) -> bool {
        self.get_type()==other.get_type() && self.get_id()==other.get_id()
    }
}
impl PartialOrd for Element {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.get_type().cmp(&other.get_type()) {
            Ordering::Equal => Some(self.get_id().cmp(&other.get_id())),
            x => Some(x)
        }
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Ord, PartialOrd,serde::Serialize)]
pub enum ElementType {
    Node,
    Way,
    Relation,
    PointGeometry,
    LinestringGeometry,
    SimplePolygonGeometry,
    ComplicatedPolygonGeometry
}

impl ElementType {
    pub fn from_int(t: u64) -> ElementType {
        match t {
            0 => ElementType::Node,
            1 => ElementType::Way,
            2 => ElementType::Relation,
            _ => {
                panic!("wrong type");
            }
        }
    }
    pub fn as_int(&self) -> u64 {
        match self {
            ElementType::Node => 0,
            ElementType::Way => 1,
            ElementType::Relation => 2,
            _ => { panic!("not impl for geometry types"); }
        }
    }
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone,serde::Serialize)]
pub enum Changetype {
    Normal,
    Delete,
    Remove,
    Unchanged,
    Modify,
    Create,
}

impl std::fmt::Display for Changetype {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Normal => "Normal",
                Self::Delete => "Delete",
                Self::Remove => "Remove",
                Self::Unchanged => "Unchanged",
                Self::Modify => "Modify",
                Self::Create => "Create",
            }
        )
    }
}
impl Changetype {
    pub fn from_int(ct: u64) -> Changetype {
        match ct {
            0 => Changetype::Normal,
            1 => Changetype::Delete,
            2 => Changetype::Remove,
            3 => Changetype::Unchanged,
            4 => Changetype::Modify,
            5 => Changetype::Create,
            _ => {
                panic!("wronge changetype");
            }
        }
    }

    pub fn as_int(&self) -> u64 {
        match self {
            Changetype::Normal => 0,
            Changetype::Delete => 1,
            Changetype::Remove => 2,
            Changetype::Unchanged => 3,
            Changetype::Modify => 4,
            Changetype::Create => 5,
        }
    }
}

pub trait WithType {
    fn get_type(&self) -> ElementType;
}

pub trait WithId {
    fn get_id(&self) -> i64;
}

pub trait WithInfo {
    fn get_info<'a>(&'a self) -> &'a Option<Info>;
}

pub trait WithTimestamp {
    fn get_timestamp(&self) -> i64;
    fn get_timestamp_string(&self) -> String {
        crate::utils::timestamp_string(self.get_timestamp())
    }
}

pub trait WithVersion {
    fn get_version(&self) -> i64;
}

impl<T> WithTimestamp for T
where
    T: WithInfo,
{
    fn get_timestamp(&self) -> i64 {
        match self.get_info() {
            Some(info) => info.timestamp,
            None => -1,
        }
    }
}

impl<T> WithVersion for T
where
    T: WithInfo,
{
    fn get_version(&self) -> i64 {
        match self.get_info() {
            Some(info) => info.version,
            None => -1,
        }
    }
}

pub trait WithChangetype {
    fn get_changetype(&self) -> Changetype;
}

pub trait WithTags {
    fn get_tags<'a>(&'a self) -> &'a [Tag];
}

pub trait WithQuadtree {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree;
}

impl WithQuadtree for Element {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        match self {
            Element::Node(n) => n.get_quadtree(),
            Element::Way(n) => n.get_quadtree(),
            Element::Relation(n) => n.get_quadtree(),
            Element::PointGeometry(n) => n.get_quadtree(),
            Element::LinestringGeometry(n) => n.get_quadtree(),
            Element::SimplePolygonGeometry(n) => n.get_quadtree(),
            Element::ComplicatedPolygonGeometry(n) => n.get_quadtree(),
        }
    }
}

pub trait SetCommon {
    fn set_id(&mut self, id: i64);
    fn set_tags(&mut self, tags: Vec<Tag>);
    fn set_info(&mut self, info: Info);
    fn set_quadtree(&mut self, quadtree: Quadtree);
}

        
