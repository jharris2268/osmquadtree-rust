use std::io::{Error,ErrorKind,Result,Write};
use std::cmp::{Ordering};
use crate::elements::{Element,Node,Way,Relation,Tag,Info,PrimitiveBlock};
use crate::geometry::{
    ComplicatedPolygonGeometry, LinestringGeometry, PointGeometry, SimplePolygonGeometry,
};
use std::fs::File;
use crate::message;

#[derive(Debug,serde::Serialize)]
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
fn tags_different(left: &Vec<Tag>, right: &Vec<Tag>) -> bool {
    if left == right { return false; }
    if left.len() != right.len() { return true; }
    
    for l in left {
        if !right.contains(&l) {
            return true;
        }
    }
    return false;
}   

fn node_compare(left: Node, right: Node) -> Result<ElementCompare> {
    
    if left.id != right.id {
        Err(Error::new(ErrorKind::Other, "different elements"))
    } else if different_info(&left.info, &right.info) {
        Ok(ElementCompare::DifferentInfo(Element::Node(left), Element::Node(right)))
    } else if tags_different(&left.tags, &right.tags) {
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
    } else if tags_different(&left.tags, &right.tags) {
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
    } else if tags_different(&left.tags, &right.tags) {
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


use std::collections::HashSet;

fn check_left_right(left_ele: &mut Option<Element>, right_ele: &mut Option<Element>) -> Result<ElementCompare> {
    match (&left_ele, &right_ele) {
        (None, None) => Err(Error::new(ErrorKind::Other, "??")),
        (Some(_), None) => Ok(ElementCompare::OnlyLeft(left_ele.take().unwrap())),
        (None, Some(_)) => Ok(ElementCompare::OnlyRight(right_ele.take().unwrap())),
        
        (Some(left), Some(right)) => {
            
            
            
            match left.partial_cmp(&right) {
                
                None => { return Err(Error::new(ErrorKind::Other, format!("?? {:?} {:?}", left, right))); },
                Some(Ordering::Less) => {
                    Ok(ElementCompare::OnlyLeft(left_ele.take().unwrap()))
                    
                },
                Some(Ordering::Equal) => {
                    element_compare(Some(left_ele.take().unwrap()),Some(right_ele.take().unwrap()))
                }
                Some(Ordering::Greater) => {
                    Ok(ElementCompare::OnlyRight(right_ele.take().unwrap()))
                    
                }
            }
        }
    }
    
}

pub fn compare_element_iters<T: Iterator<Item=Element>>(mut left_iter: T, mut right_iter: T, max_result_len: usize) -> Result<(Vec<ElementCompare>,HashSet<(String,String)>,usize)> {
    
    let mut left_ele = left_iter.next();
    let mut right_ele = right_iter.next();
    
    let mut res: Vec<ElementCompare> = Vec::new();
    
    let mut changed_users = HashSet::new();
    let mut count=0;
    
    loop {
        if left_ele.is_none() && right_ele.is_none() {
            break;
        }
        
        match check_left_right(&mut left_ele, &mut right_ele)? {
            ElementCompare::Same => {},
            ElementCompare::ChangedUserName(ln,rn) => { changed_users.insert((ln,rn)); },
            p => {
                if count < max_result_len {
                    res.push(p);
                } else if count == max_result_len {
                    message!("found {} diffs: {:?}", res.len(), p);
                } else if (count % 100000) == 0 {
                    message!("found {} diffs: {:?}", res.len(), p);
                    //pass
                }
                count+=1;
            }
        }   
        
        if left_ele.is_none() { left_ele = left_iter.next(); }
        if right_ele.is_none() { right_ele = right_iter.next(); }
        
    }
    
    Ok((res, changed_users, count))
                    
}
                
pub fn compare_element_iters_json<T: Iterator<Item=Element>>(mut left_iter: T, mut right_iter: T, outfn: &str) -> Result<(HashSet<(String,String)>,usize)> {
    
    let mut outf = File::create(outfn)?;
    
    let mut left_ele = left_iter.next();
    let mut right_ele = right_iter.next();
    let mut changed_users = HashSet::new();
    let mut count=0;
    
    loop {
        if left_ele.is_none() && right_ele.is_none() {
            break;
        }
        
        match check_left_right(&mut left_ele, &mut right_ele)? {
            ElementCompare::Same => {},
            ElementCompare::ChangedUserName(ln,rn) => { changed_users.insert((ln,rn)); },
            p => {
                let mut x = serde_json::to_vec(&p).expect("?");
                x.push(32);
                outf.write_all(&x).expect("?");
                
                count+=1;
            }
        }   
        
        if left_ele.is_none() { left_ele = left_iter.next(); }
        if right_ele.is_none() { right_ele = right_iter.next(); }
        
    }
    
    Ok((changed_users, count))
                    
}
    
pub fn compare_primitiveblock(left: PrimitiveBlock, right: PrimitiveBlock) -> Result<(Vec<ElementCompare>,HashSet<(String,String)>,usize)> {
    let mrs=100000000000;
    let left_iter = left.into_iter();
    let right_iter = right.into_iter();
    
    
    compare_element_iters(left_iter, right_iter,mrs)
}
        
