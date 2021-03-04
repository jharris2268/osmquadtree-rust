#[derive(Debug, Eq, PartialEq, Clone, serde::Serialize, Ord, PartialOrd)]
pub struct Tag {
    pub key: String,
    pub val: String,
}

impl Tag {
    pub fn new(key: String, val: String) -> Tag {
        Tag { key, val }
    }
}
