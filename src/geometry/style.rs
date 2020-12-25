use crate::geometry::default_style::DEFAULT_GEOMETRY_STYLE;
use crate::elements::Tag;

use std::collections::{BTreeMap,BTreeSet};
use serde::{Serialize,Deserialize};

use std::fs::File;
use std::io::{BufReader,Result,Error,ErrorKind};

#[derive(Serialize,Deserialize, Debug, Clone)]
#[serde(rename_all="lowercase")]
pub enum PolyTagSpec {
    Exclude(Vec<String>),
    Include(Vec<String>),
    All
}



#[derive(Serialize,Deserialize, Debug, Clone)]
pub struct ParentTagSpec {
    pub node_keys: Vec<String>,
    pub way_key: String,
    pub way_priority: BTreeMap<String, i64>
}


#[derive(Serialize,Deserialize, Debug, Clone)]
pub struct RelationTagSpec {
    source_filter: BTreeMap<String,String>,
    source_key: String,
    target_key: String,
    
    #[serde(rename(serialize="type", deserialize="type"))]
    op_type: String
    
}

fn get_zorder_value(t: &Tag) -> i64 {
    if t.key == "highway" {
        if t.val == "motorway" { return 380; }
        if t.val == "trunk" {return 370;}
        if t.val == "primary" {return 360;}
        if t.val == "secondary" { return 350;}
        if t.val == "tertiary" { return 340;}
        if t.val == "residential" { return 330;}
        if t.val == "unclassified" { return 330;}
        if t.val == "road" { return 330;}
        if t.val == "living_street" { return 320;}
        if t.val == "pedestrian" { return 310;}
        if t.val == "raceway" { return 300;}
        if t.val == "motorway_link" { return 240;}
        if t.val == "trunk_link" { return 230;}
        if t.val == "primary_link" { return 220;}
        if t.val == "secondary_link" { return 210;}
        if t.val == "tertiary_link" { return 200;}
        if t.val == "service" { return 150;}
        if t.val == "track" { return 110;}
        if t.val == "path" { return 100;}
        if t.val == "footway" { return 100;}
        if t.val == "bridleway" { return 100;}
        if t.val == "cycleway" { return 100;}
        if t.val == "steps" { return 90;}
        if t.val == "platform" { return 90;}
        if t.val == "construction" { return 10;}
        return 0;
    }
    
    if t.key=="railway" {
        if t.val == "rail" { return 440; }
        if t.val == "subway" { return 420; }
        if t.val == "narrow_gauge" { return 420; }
        if t.val == "light_rail" { return 420; }
        if t.val == "funicular" { return 420; }
        if t.val == "preserved" { return 420; }
        if t.val == "monorail" { return 420; }
        if t.val == "miniature" { return 420; }
        if t.val == "turntable" { return 420; }
        if t.val == "tram" { return 410; }
        if t.val == "disused" { return 400; }
        if t.val == "construction" { return 400; }
        if t.val == "platform" { return 90; }
        return 0;
    }
    
    if t.key=="aeroway" {
        if t.val == "runway" { return 60; }
        if t.val == "taxiway" { return 50; }
        return 0;
    }
    return 0;
}

    


#[derive(Serialize, Deserialize, Debug)]
pub struct GeometryStyle {

    pub feature_keys: BTreeSet<String>,
    pub other_keys: Option<BTreeSet<String>>,
    pub polygon_tags: BTreeMap<String, PolyTagSpec>,
    pub parent_tags: BTreeMap<String, ParentTagSpec>,
    pub relation_tag_spec: Vec<RelationTagSpec>,
    pub multipolygons: bool,
    pub boundary_relations: bool
}


impl GeometryStyle {
    pub fn default() -> GeometryStyle {
        serde_json::from_str(&DEFAULT_GEOMETRY_STYLE).expect("!!")
    }
    
    pub fn from_file(infn: &str) -> Result<GeometryStyle> {
        let ff = File::open(infn)?;
        let mut fbuf = BufReader::new(ff);
        match serde_json::from_reader(&mut fbuf) {
            Ok(p) => Ok(p),
            Err(e) => Err(Error::new(ErrorKind::Other, e.to_string()))
        }
    }
    fn has_feature_key(&self, tags: &[Tag]) -> bool {
        for t in tags {
            if self.feature_keys.contains(&t.key) {
                return true;
            }
        }
        false
    }
    fn has_key(&self, k: &str) -> bool {
        match &self.other_keys {
            None => { return true; },
            Some(o) => {
                if o.contains(k) {
                    return true;
                }
            },
        }
        
        self.feature_keys.contains(k)
    }
        
        
    
    fn filter_tags(&self, tags: &[Tag]) -> (Vec<Tag>, i64, i64) {
        
        let mut res=Vec::new();
        let mut z_order=0;
        let mut layer =0;
        for t in tags {
            if self.has_key(&t.key) {
                res.push(t.clone());
            }
            
            if t.key == "layer" {
                match t.val.parse::<i64>() {
                    Ok(l) => { layer=l; },
                    Err(_) => {},
                }
            }
            
            z_order = i64::max(z_order, get_zorder_value(&t));
            
        }
        (res, z_order, layer)
    }
    
    fn check_polygon_tags(&self, tags: &[Tag]) -> bool {
        
        for t in tags {
            match self.polygon_tags.get(&t.key) {
                None => {},
                Some(pt) => {
                    match pt {
                        PolyTagSpec::All => {
                            return true;
                        },
                        PolyTagSpec::Exclude(exc) => {
                            if !exc.contains(&t.val) {
                                return true;
                            }
                        },
                        PolyTagSpec::Include(inc) => {
                            if inc.contains(&t.val) {
                                return true;
                            }
                        },
                    }
                }
            }
        }
        return false;
        
    }
    
    
    pub fn process_multipolygon_relation(&self, tags: &[Tag]) -> Result<(Vec<Tag>, i64, i64)> {
        
        if !self.has_feature_key(&tags) {
            return Err(Error::new(ErrorKind::Other, "not a feature"));
        }
        
        /*if !self.check_polygon_tags(&tags) {
            return Err(Error::new(ErrorKind::Other, "not a polygon feature"));
        }*/
        
        Ok(self.filter_tags(tags))
    }
        
    pub fn process_way(&self, tags: &[Tag], is_ring: bool) -> Result<(bool, Vec<Tag>, i64, i64)> {
        
        if !self.has_feature_key(&tags) {
            return Err(Error::new(ErrorKind::Other, "not a feature"));
        }
        let is_poly = is_ring && self.check_polygon_tags(&tags);
        
        let (t,l,z) = self.filter_tags(tags);
        Ok((is_poly,t,l,z))
    }
    
    pub fn process_node(&self, tags: &[Tag]) -> Result<(Vec<Tag>, i64)> {
        if !self.has_feature_key(&tags) {
            return Err(Error::new(ErrorKind::Other, "not a feature"));
        }
     
        
        let (t,l,_) = self.filter_tags(tags);
        Ok((t,l))
    }
    
}


