use crate::elements::{Info, Quadtree, Tag};
use crate::elements::{Node, Relation, Way};
use crate::geometry::{
    ComplicatedPolygonGeometry, LinestringGeometry, PointGeometry, SimplePolygonGeometry,
};
#[derive(Debug)]
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

#[derive(Debug, Eq, PartialEq, Clone, Ord, PartialOrd)]
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

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone)]
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

#[derive(Debug)]
pub enum ElementCompare {
    OnlyLeft(Element),
    OnlyRight(Element),
    DifferentInfo(Element,Element),
    DifferentTags(Element,Element),
    DifferentData(Element,Element),
    DifferentQuadtree(Element,Element),
    ChangedUserName(String,String),
    Same
}

use std::io::{Error,ErrorKind,Result};
use std::cmp::{Ordering};

fn pointgeometry_compare(_left: PointGeometry,_right: PointGeometry) -> Result<ElementCompare> {
    Err(Error::new(ErrorKind::Other, "not impl for geometry types"))
}

fn linestringgeometry_compare(_left: LinestringGeometry,_right: LinestringGeometry) -> Result<ElementCompare> {
    Err(Error::new(ErrorKind::Other, "not impl for geometry types"))
}

fn simplepolygongeometry_compare(_left: SimplePolygonGeometry,_right: SimplePolygonGeometry) -> Result<ElementCompare> {
    Err(Error::new(ErrorKind::Other, "not impl for geometry types"))
}

fn compliatedpolygongeometry_compare(_left: ComplicatedPolygonGeometry,_right: ComplicatedPolygonGeometry) -> Result<ElementCompare> {
    Err(Error::new(ErrorKind::Other, "not impl for geometry types"))
}

fn different_info(left: &Option<Info>, right: &Option<Info>) -> bool {
    match (left,right) {
        (None,None) => false,
        (Some(_),None) => true,
        (None,Some(_)) => true,
        (Some(left),Some(right)) => 
            left.version != right.version || left.changeset != right.changeset ||
            left.timestamp != right.timestamp || left.user_id != right.user_id
        
    }
}

fn node_compare(left: Node, right: Node) -> Result<ElementCompare> {
    
    if left.id != right.id {
        Err(Error::new(ErrorKind::Other, "different elements"))
    } else if different_info(&left.info, &right.info) {
        Ok(ElementCompare::DifferentInfo(Element::Node(left), Element::Node(right)))
    } else if left.tags != right.tags {
        Ok(ElementCompare::DifferentTags(Element::Node(left), Element::Node(right)))
    } else if left.lon != right.lon || left.lat != right.lat {
        Ok(ElementCompare::DifferentData(Element::Node(left), Element::Node(right)))
    } else if left.quadtree != right.quadtree {
        Ok(ElementCompare::DifferentQuadtree(Element::Node(left), Element::Node(right)))
    } else {
        match (left.info, right.info) {
            (Some(li),Some(ri)) => {
                if li.user != ri.user {
                    Ok(ElementCompare::ChangedUserName(li.user,ri.user))
                } else {
                    Ok(ElementCompare::Same)
                }
            },
            (_,_) => Ok(ElementCompare::Same)
        }
    }
}
fn way_compare(left: Way, right: Way) -> Result<ElementCompare> {
    
    
    if left.id != right.id {
        Err(Error::new(ErrorKind::Other, "different elements"))
    } else if different_info(&left.info, &right.info) {
        Ok(ElementCompare::DifferentInfo(Element::Way(left), Element::Way(right)))
    } else if left.tags != right.tags {
        Ok(ElementCompare::DifferentTags(Element::Way(left), Element::Way(right)))
    } else if left.refs != right.refs {
            Ok(ElementCompare::DifferentData(Element::Way(left), Element::Way(right)))
    } else if left.quadtree != right.quadtree {
        Ok(ElementCompare::DifferentQuadtree(Element::Way(left), Element::Way(right)))
    } else {
        match (left.info, right.info) {
            (Some(li),Some(ri)) => {
                if li.user != ri.user {
                    Ok(ElementCompare::ChangedUserName(li.user,ri.user))
                } else {
                    Ok(ElementCompare::Same)
                }
            },
            (_,_) => Ok(ElementCompare::Same)
        }
    }
    
   
}

fn relation_compare(left: Relation, right: Relation) -> Result<ElementCompare> {
    
    
    if left.id != right.id {
        Err(Error::new(ErrorKind::Other, "different elements"))
    } else if different_info(&left.info, &right.info) {
        Ok(ElementCompare::DifferentInfo(Element::Relation(left), Element::Relation(right)))
    } else if left.tags != right.tags {
        Ok(ElementCompare::DifferentTags(Element::Relation(left), Element::Relation(right)))
    } else if left.members != right.members {
        Ok(ElementCompare::DifferentData(Element::Relation(left), Element::Relation(right)))
    } else if left.quadtree != right.quadtree {
        Ok(ElementCompare::DifferentQuadtree(Element::Relation(left), Element::Relation(right)))
    } else {
        match (left.info, right.info) {
            (Some(li),Some(ri)) => {
                if li.user != ri.user {
                    Ok(ElementCompare::ChangedUserName(li.user,ri.user))
                } else {
                    Ok(ElementCompare::Same)
                }
            },
            (_,_) => Ok(ElementCompare::Same)
        }
    }

}

    
pub fn element_compare(left: Option<Element>, right: Option<Element>) -> Result<ElementCompare> {
    match (left, right) {
        (Some(left), Some(right)) => {
            match (left, right) {
                (Element::Node(left), Element::Node(right)) => node_compare(left,right),
                (Element::Way(left), Element::Way(right)) => way_compare(left,right),
                (Element::Relation(left), Element::Relation(right)) => relation_compare(left,right),
                (Element::PointGeometry(left), Element::PointGeometry(right)) => pointgeometry_compare(left,right),
                (Element::LinestringGeometry(left), Element::LinestringGeometry(right)) => linestringgeometry_compare(left,right),
                (Element::SimplePolygonGeometry(left), Element::SimplePolygonGeometry(right)) => simplepolygongeometry_compare(left,right),
                (Element::ComplicatedPolygonGeometry(left), Element::ComplicatedPolygonGeometry(right)) => compliatedpolygongeometry_compare(left,right),
                (_, _) => Err(Error::new(ErrorKind::Other, "different element types!!"))
            }
        }
        (Some(left), None) => Ok(ElementCompare::OnlyLeft(left)),
        (None, Some(right)) => Ok(ElementCompare::OnlyRight(right)),
        (None, None) => Err(Error::new(ErrorKind::Other, "no elements??"))
    }
}
use crate::elements::PrimitiveBlock;
pub fn primitiveblock_compare(left: PrimitiveBlock, right: PrimitiveBlock) -> Result<(Vec<ElementCompare>,HashSet<(String,String)>)> {
    let mrs=100000000000;
    let left_iter = left.into_iter();
    let right_iter = right.into_iter();
    
    
    combine_element_iters(left_iter, right_iter,mrs)
}

use std::collections::HashSet;

fn check_left_right(left_ele: &mut Option<Element>, right_ele: &mut Option<Element>, res: &mut Vec<ElementCompare>,changed_users: &mut HashSet<(String,String)>) -> Result<()> {
    match (&left_ele, &right_ele) {
        (None, None) => { return Err(Error::new(ErrorKind::Other, "??")); }
        (Some(_), None) => {
            res.push(ElementCompare::OnlyLeft(left_ele.take().unwrap()));
            //Ok((None,None))
        }
        
        (None, Some(_)) => {
            res.push(ElementCompare::OnlyRight(right_ele.take().unwrap()));
            //Ok((None,None))
        }
        
        (Some(left), Some(right)) => {
            
            
            
            match left.partial_cmp(&right) {
                
                None => { return Err(Error::new(ErrorKind::Other, format!("?? {:?} {:?}", left, right))); },
                Some(Ordering::Less) => {
                    res.push(ElementCompare::OnlyLeft(left_ele.take().unwrap()));
                    //Ok((None,Some(right)))
                },
                Some(Ordering::Equal) => {
                    match element_compare(Some(left_ele.take().unwrap()),Some(right_ele.take().unwrap()))? {
                        ElementCompare::Same => {},
                        ElementCompare::ChangedUserName(ln,rn) => {
                            changed_users.insert((ln,rn));
                        },
                        x => { res.push(x); }
                    }
                    //Ok((None,None))
                },
                Some(Ordering::Greater) => {
                    res.push(ElementCompare::OnlyRight(right_ele.take().unwrap()));
                    //Ok((Some(left),None))
                }
            }
        }
    }
    Ok(())
}

pub fn combine_element_iters<T: Iterator<Item=Element>>(mut left_iter: T, mut right_iter: T, max_result_len: usize) -> Result<(Vec<ElementCompare>,HashSet<(String,String)>)> {
    
    let mut left_ele = left_iter.next();
    let mut right_ele = right_iter.next();
    
    let mut res: Vec<ElementCompare> = Vec::new();
    
    let mut changed_users = HashSet::new();
    
    
    loop {
        if left_ele.is_none() && right_ele.is_none() {
            break;
        }
        
        check_left_right(&mut left_ele, &mut right_ele,&mut res, &mut changed_users)?;
        
        if left_ele.is_none() { left_ele = left_iter.next(); }
        if right_ele.is_none() { right_ele = right_iter.next(); }
        if res.len() > max_result_len {
            break;
        }
    }
                    
    Ok((res, changed_users))
                    
}
                
    
    
        
        
