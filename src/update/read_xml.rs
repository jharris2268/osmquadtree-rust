use std::collections::BTreeMap;
use std::io::{BufRead, Error, ErrorKind, Result};

use crate::elements::{Changetype, ElementType, Info, Member, Node, Relation, Tag, Way};
use crate::utils::Checktime;

use chrono::NaiveDateTime;
use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

const TIMEFORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

fn read_timestamp(ts: &str) -> Result<i64> {
    //println!("ts: {}, TIMEFORMAT: {}, TIMEFORMAT_ALT: {}", ts, TIMEFORMAT, TIMEFORMAT_ALT);
    match NaiveDateTime::parse_from_str(ts, TIMEFORMAT) {
        Ok(tm) => {
            return Ok(tm.timestamp());
        }
        Err(e) => {
            println!("{:?}", e)
        }
    }

    return Err(Error::new(
        ErrorKind::Other,
        format!("timestamp not in format \"{}\"", TIMEFORMAT),
    ));
}

fn ele_str(w: &str, e: &BytesStart) -> quick_xml::Result<String> {
    let n = std::str::from_utf8(e.name())?;
    let mut sl: Vec<String> = Vec::new();
    for a in e.attributes() {
        sl.push(format!(
            "{} = \"{}\"",
            std::str::from_utf8(&a.as_ref().expect("?").key).expect("?"),
            std::str::from_utf8(&a.as_ref().expect("?").unescaped_value()?).expect("?")
        ));
    }
    Ok(format!("{} {}: {}", w, n, sl.join("; ")))
}

fn all_whitespace(s: &[u8]) -> bool {
    for c in s {
        if !c.is_ascii_whitespace() {
            return false;
        }
    }
    return true;
}

fn as_int(v: f64) -> i32 {
    if v < 0.0 {
        return ((v * 10000000.0) - 0.5) as i32;
    }

    return ((v * 10000000.0) + 0.5) as i32;
}

fn read_node<T: BufRead>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    e: &BytesStart,
    ct: Option<Changetype>,
    has_children: bool,
) -> Result<Node> {
    let mut n = Node::new(
        0,
        match ct {
            Some(c) => c,
            None => Changetype::Normal,
        },
    );
    let mut info = Info::new();
    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"id" => {
                        n.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = read_timestamp(&val)?;
                    }
                    b"changeset" => {
                        info.changeset = val.parse().expect("not an int");
                    }
                    b"version" => {
                        info.version = val.parse().expect("not an int");
                    }
                    b"uid" => {
                        info.user_id = val.parse().expect("not an int");
                    }
                    b"user" => {
                        info.user = val.to_string();
                    }
                    b"lon" => {
                        n.lon = as_int(val.parse().expect("not a float"));
                    }
                    b"lat" => {
                        n.lat = as_int(val.parse().expect("not a float"));
                    }
                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ));
            }
        }
    }
    if info.timestamp != 0 {
        n.info = Some(info);
    }
    if has_children {
        loop {
            match reader.read_event(buf) {
                Ok(Event::Empty(ref e)) => match e.name() {
                    b"tag" => {
                        n.tags.push(read_tag(e)?);
                    }
                    n => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("unexpected empty {}", std::str::from_utf8(n).expect("?")),
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name() {
                    b"node" => {
                        break;
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected end {}",
                                std::str::from_utf8(e.name()).expect("?")
                            ),
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.escaped()) {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected text {}: {}",
                                reader.buffer_position(),
                                e.unescape_and_decode(&reader).unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("failed to node children"),
                    ));
                }
            }
        }
    }

    Ok(n)
}

fn read_tag(e: &BytesStart) -> Result<Tag> {
    let mut k: Option<String> = None;
    let mut v: Option<String> = None;

    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"k" => {
                        k = Some(val);
                    }
                    b"v" => {
                        v = Some(val);
                    }
                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ));
            }
        }
    }

    if k == None {
        return Err(Error::new(ErrorKind::Other, "tag missing key"));
    }
    if v == None {
        return Err(Error::new(
            ErrorKind::Other,
            format!("tag missing val [key={}]", k.unwrap()),
        ));
    }

    Ok(Tag::new(k.unwrap(), v.unwrap()))
}

fn read_ref(e: &BytesStart) -> Result<i64> {
    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"ref" => {
                        return Ok(val.parse().expect("not an int"));
                    }
                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ));
            }
        }
    }
    return Err(Error::new(ErrorKind::Other, "no ref attribute"));
}

fn read_member(e: &BytesStart) -> Result<Member> {
    let mut role = String::new();
    let mut mem_type = ElementType::Node;
    let mut mem_ref = 0;

    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"role" => {
                        role = val;
                    }
                    b"ref" => {
                        mem_ref = val.parse().expect("not an int");
                    }
                    b"type" => match val.as_str() {
                        "node" => {
                            mem_type = ElementType::Node;
                        }
                        "way" => {
                            mem_type = ElementType::Way;
                        }
                        "relation" => {
                            mem_type = ElementType::Relation;
                        }
                        t => {
                            return Err(Error::new(
                                ErrorKind::Other,
                                format!("unexpected member type {}", t),
                            ));
                        }
                    },
                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ));
            }
        }
    }
    Ok(Member::new(role, mem_type, mem_ref))
}

fn read_way<T: BufRead>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    e: &BytesStart,
    ct: Option<Changetype>,
    has_children: bool,
) -> Result<Way> {
    let mut w = Way::new(
        0,
        match ct {
            Some(c) => c,
            None => Changetype::Normal,
        },
    );
    let mut info = Info::new();
    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"id" => {
                        w.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = read_timestamp(&val)?;
                    }
                    b"changeset" => {
                        info.changeset = val.parse().expect("not an int");
                    }
                    b"version" => {
                        info.version = val.parse().expect("not an int");
                    }
                    b"uid" => {
                        info.user_id = val.parse().expect("not an int");
                    }
                    b"user" => {
                        info.user = val.to_string();
                    }

                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ))
            }
        }
    }
    if info.timestamp != 0 {
        w.info = Some(info);
    }
    if has_children {
        loop {
            match reader.read_event(buf) {
                Ok(Event::Empty(ref e)) => match e.name() {
                    b"tag" => {
                        w.tags.push(read_tag(e)?);
                    }
                    b"nd" => {
                        w.refs.push(read_ref(e)?);
                    }
                    n => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("unexpected empty {}", std::str::from_utf8(n).expect("?")),
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name() {
                    b"way" => {
                        break;
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected end {}",
                                std::str::from_utf8(e.name()).expect("?")
                            ),
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.escaped()) {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected text {}: {}",
                                reader.buffer_position(),
                                e.unescape_and_decode(&reader).unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("failed to node children"),
                    ))
                }
            }
        }
    }

    Ok(w)
}

fn read_relation<T: BufRead>(
    reader: &mut Reader<T>,
    buf: &mut Vec<u8>,
    e: &BytesStart,
    ct: Option<Changetype>,
    has_children: bool,
) -> Result<Relation> {
    let mut r = Relation::new(
        0,
        match ct {
            Some(c) => c,
            None => Changetype::Normal,
        },
    );
    let mut info = Info::new();
    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = String::from(
                    std::str::from_utf8(&kv.unescaped_value().expect("?")).expect("not a string"),
                );
                match kv.key {
                    b"id" => {
                        r.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = read_timestamp(&val)?;
                    }
                    b"changeset" => {
                        info.changeset = val.parse().expect("not an int");
                    }
                    b"version" => {
                        info.version = val.parse().expect("not an int");
                    }
                    b"uid" => {
                        info.user_id = val.parse().expect("not an int");
                    }
                    b"user" => {
                        info.user = val.to_string();
                    }

                    k => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected attribute {} {}",
                                std::str::from_utf8(k).expect("?"),
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to read attribute"),
                ))
            }
        }
    }
    if info.timestamp != 0 {
        r.info = Some(info);
    }

    if has_children {
        loop {
            match reader.read_event(buf) {
                Ok(Event::Empty(ref e)) => match e.name() {
                    b"tag" => {
                        r.tags.push(read_tag(e)?);
                    }
                    b"member" => {
                        r.members.push(read_member(e)?);
                    }
                    n => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("unexpected empty {}", std::str::from_utf8(n).expect("?")),
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name() {
                    b"relation" => {
                        break;
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected end {}",
                                std::str::from_utf8(e.name()).expect("?")
                            ),
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.escaped()) {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected text {}: {}",
                                reader.buffer_position(),
                                e.unescape_and_decode(&reader).unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("failed to node children"),
                    ))
                }
            }
        }
    }

    Ok(r)
}

pub struct ChangeBlock {
    pub nodes: BTreeMap<i64, Node>,
    pub ways: BTreeMap<i64, Way>,
    pub relations: BTreeMap<i64, Relation>,
}
impl ChangeBlock {
    pub fn new() -> ChangeBlock {
        ChangeBlock {
            nodes: BTreeMap::new(),
            ways: BTreeMap::new(),
            relations: BTreeMap::new(),
        }
    }

    pub fn add_node(&mut self, n: Node) -> bool {
        if !self.nodes.contains_key(&n.id) {
            self.nodes.insert(n.id, n);
            return true;
        }

        let curr = self.nodes.get_mut(&n.id).unwrap();
        if n.info.as_ref().unwrap().version > curr.info.as_ref().unwrap().version {
            *curr = n;
        }
        false
    }

    pub fn add_way(&mut self, w: Way) -> bool {
        if !self.ways.contains_key(&w.id) {
            self.ways.insert(w.id, w);
            return true;
        }

        let curr = self.ways.get_mut(&w.id).unwrap();
        if w.info.as_ref().unwrap().version > curr.info.as_ref().unwrap().version {
            *curr = w;
        }
        false
    }

    pub fn add_relation(&mut self, r: Relation) -> bool {
        if !self.relations.contains_key(&r.id) {
            self.relations.insert(r.id, r);
            return true;
        }

        let curr = self.relations.get_mut(&r.id).unwrap();
        if r.info.as_ref().unwrap().version > curr.info.as_ref().unwrap().version {
            *curr = r;
        }
        false
    }
}

pub fn read_xml_change<T: BufRead>(inf: &mut T) -> Result<ChangeBlock> {
    let mut res = ChangeBlock::new();

    let mut reader = Reader::from_reader(inf);

    let mut buf = Vec::new();
    let mut buf2 = Vec::new();
    let mut ct: Option<Changetype> = None;
    let mut cktm = Checktime::new();

    loop {
        match reader.read_event(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match cktm.checktime() {
                    Some(d) => {
                        println!(
                            "{:5.1}s {} {}",
                            d,
                            reader.buffer_position(),
                            ele_str("start", e).expect("?")
                        );
                    }
                    None => {}
                }
                match e.name() {
                    b"osmChange" => {}
                    b"delete" => {
                        ct = Some(Changetype::Delete);
                    }
                    b"modify" => {
                        ct = Some(Changetype::Modify);
                    }
                    b"create" => {
                        ct = Some(Changetype::Create);
                    }
                    b"node" => {
                        res.add_node(read_node(&mut reader, &mut buf2, e, ct, true)?);
                    }
                    b"way" => {
                        res.add_way(read_way(&mut reader, &mut buf2, e, ct, true)?);
                    }
                    b"relation" => {
                        res.add_relation(read_relation(&mut reader, &mut buf2, e, ct, true)?);
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected start tag {} {}",
                                std::str::from_utf8(e.name()).expect("?"),
                                reader.buffer_position()
                            ),
                        ));
                    }
                }
            }
            Ok(Event::Empty(ref e)) => {
                match cktm.checktime() {
                    Some(d) => {
                        println!(
                            "{:5.1}s {} {}",
                            d,
                            reader.buffer_position(),
                            ele_str("empty", e).expect("?")
                        );
                    }
                    None => {}
                }
                match e.name() {
                    b"node" => {
                        res.add_node(read_node(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    b"way" => {
                        res.add_way(read_way(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    b"relation" => {
                        res.add_relation(read_relation(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!(
                                "unexpected empty tag {} {}",
                                std::str::from_utf8(e.name()).expect("?"),
                                reader.buffer_position()
                            ),
                        ));
                    }
                }
            }
            Ok(Event::End(ref e)) => match e.name() {
                b"osmChange" => {}
                b"delete" => {
                    ct = None;
                }
                b"modify" => {
                    ct = None;
                }
                b"create" => {
                    ct = None;
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!(
                            "unexpected end tag {} {}",
                            std::str::from_utf8(e.name()).expect("?"),
                            reader.buffer_position()
                        ),
                    ));
                }
            },

            Ok(Event::Eof) => {
                break;
            }

            Ok(Event::Text(e)) => {
                if !all_whitespace(e.escaped()) {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!(
                            "unexpected text {}: {}",
                            reader.buffer_position(),
                            e.unescape_and_decode(&reader).unwrap()
                        ),
                    ));
                }
            }
            Ok(Event::CData(e)) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "unexpected cdata {}: {}",
                        reader.buffer_position(),
                        e.unescape_and_decode(&reader).unwrap()
                    ),
                ));
            }
            Ok(_) => {}

            Err(e) => {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("Error at position {}: {:?}", reader.buffer_position(), e),
                ));
            }
        }
        buf.clear();
    }
    println!(
        "{:5.1}s: {} nodes, {} ways, {} relations",
        cktm.gettime(),
        res.nodes.len(),
        res.ways.len(),
        res.relations.len()
    );
    Ok(res)
}
