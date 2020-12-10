#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Tag {
    pub key: String,
    pub val: String,
}

impl Tag {
    pub fn new(key: String, val: String) -> Tag {
        Tag { key, val }
    }
}
