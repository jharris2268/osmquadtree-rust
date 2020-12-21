use crate::geometry::default_style::DEFAULT_GEOMETRY_STYLE;

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
    
}


