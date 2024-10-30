use std::collections::BTreeMap;
use std::io::BufRead;


use crate::elements::{Changetype, ElementType, Info, Member, Node, Relation, Tag, Way};
use crate::utils::{as_int, Checktime, parse_timestamp, Error, Result};
use crate::message;

use quick_xml::events::{BytesStart, Event};
use quick_xml::Reader;

/*
const TIMEFORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

fn read_timestamp(ts: &str) -> Result<i64> {
    //message!("ts: {}, TIMEFORMAT: {}, TIMEFORMAT_ALT: {}", ts, TIMEFORMAT, TIMEFORMAT_ALT);
    match DateTime::parse_from_str(ts, TIMEFORMAT) {
        Ok(tm) => {
            Ok(tm.to_utc().timestamp())
        }
        Err(_) => {
            Err(Error::new(
                ErrorKind::Other,
                format!("timestamp {} not in format \"{}\"", ts, TIMEFORMAT),
            ))
        }
    }

}
*/

fn get_key<'a>(kv: &quick_xml::events::attributes::Attribute<'a>) -> String {
    String::from(std::str::from_utf8(kv.key.as_ref()).expect("?"))
}
fn get_value<'a>(kv: &quick_xml::events::attributes::Attribute<'a>) -> String {
    String::from(kv.unescape_value().as_ref().expect("?").as_ref())
}


fn ele_str(w: &str, e: &BytesStart) -> Result<String> {
    let n = String::from(std::str::from_utf8(e.name().as_ref())?);
    let mut sl: Vec<String> = Vec::new();
    for a in e.attributes() {
        sl.push(format!(
            "{} = \"{}\"",
            get_key(&a.as_ref().expect("?")),
            get_value(&a.as_ref().expect("?"))
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
                let val = get_value(&kv);
                match kv.key.as_ref() {
                    b"id" => {
                        n.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = parse_timestamp(&val)?;
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
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected attribute {:?} {}",
                                k,
                                val
                            ),
                        ));
                    }
                }
            }
            Err(_e) => {
                return Err(Error::XmlDataError(
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
            match reader.read_event_into(buf) {
                Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                    b"tag" => {
                        n.tags.push(read_tag(e)?);
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!("unexpected empty {}", std::str::from_utf8(n).expect("?")),
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name().as_ref() {
                    b"node" => {
                        break;
                    }
                    x => {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected end {}",
                                std::str::from_utf8(x).expect("?")
                            ),
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.as_ref()) {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected text {}: {:?}",
                                reader.buffer_position(),
                                //e.unescape_and_decode(&reader).unwrap()
                                e.unescape().unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::XmlDataError(
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::XmlDataError(
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
                let val = get_value(&kv);
                match kv.key.as_ref() {
                    b"k" => {
                        k = Some(val);
                    }
                    b"v" => {
                        v = Some(val);
                    }
                    k => {
                        return Err(Error::XmlDataError(
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
                return Err(Error::XmlDataError(
                    format!("failed to read attribute"),
                ));
            }
        }
    }

    if k == None {
        return Err(Error::XmlDataError("tag missing key".to_string()));
    }
    if v == None {
        return Err(Error::XmlDataError(
            format!("tag missing val [key={}]", k.unwrap()),
        ));
    }

    Ok(Tag::new(k.unwrap(), v.unwrap()))
}

fn read_ref(e: &BytesStart) -> Result<i64> {
    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = get_value(&kv);
                match kv.key.as_ref() {
                    b"ref" => {
                        return Ok(val.parse().expect("not an int"));
                    }
                    k => {
                        return Err(Error::XmlDataError(
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
                return Err(Error::XmlDataError(
                    format!("failed to read attribute"),
                ));
            }
        }
    }
    return Err(Error::XmlDataError("no ref attribute".to_string()));
}

fn read_member(e: &BytesStart) -> Result<Member> {
    let mut role = String::new();
    let mut mem_type = ElementType::Node;
    let mut mem_ref = 0;

    for a in e.attributes() {
        match a {
            Ok(kv) => {
                let val = get_value(&kv);
                match kv.key.as_ref() {
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
                            return Err(Error::XmlDataError(
                                format!("unexpected member type {}", t),
                            ));
                        }
                    },
                    k => {
                        return Err(Error::XmlDataError(
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
                return Err(Error::XmlDataError(
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
                let val = get_value(&kv);
                match kv.key.as_ref() {
                    b"id" => {
                        w.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = parse_timestamp(&val)?;
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
                        return Err(Error::XmlDataError(
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
                return Err(Error::XmlDataError(
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
            match reader.read_event_into(buf) {
                Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                    b"tag" => {
                        w.tags.push(read_tag(e)?);
                    }
                    b"nd" => {
                        w.refs.push(read_ref(e)?);
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!("unexpected empty {:?}",n)
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name().as_ref() {
                    b"way" => {
                        break;
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!("unexpected end {:?}", n)
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.as_ref()) {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected text {}: {:?}",
                                reader.buffer_position(),
                                e.unescape().unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::XmlDataError(
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::XmlDataError(
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
                let val = get_value(&kv);
                match kv.key.as_ref() {
                    b"id" => {
                        r.id = val.parse().expect("not an int");
                    }
                    b"timestamp" => {
                        info.timestamp = parse_timestamp(&val)?;
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
                        return Err(Error::XmlDataError(
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
                return Err(Error::XmlDataError(
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
            match reader.read_event_into(buf) {
                Ok(Event::Empty(ref e)) => match e.name().as_ref() {
                    b"tag" => {
                        r.tags.push(read_tag(e)?);
                    }
                    b"member" => {
                        r.members.push(read_member(e)?);
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!("unexpected empty {:?}", n)
                        ));
                    }
                },
                Ok(Event::End(ref e)) => match e.name().as_ref() {
                    b"relation" => {
                        break;
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!("unexpected end {:?}", n)
                        ));
                    }
                },
                Ok(Event::Text(e)) => {
                    if !all_whitespace(e.as_ref()) {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected text {}: {}",
                                reader.buffer_position(),
                                e.unescape().unwrap()
                            ),
                        ));
                    }
                }
                Ok(e) => {
                    return Err(Error::XmlDataError(
                        format!("unexpected tag {:?}", e),
                    ));
                }
                Err(_e) => {
                    return Err(Error::XmlDataError(
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
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e)) => {
                match cktm.checktime() {
                    Some(d) => {
                        message!(
                            "{:5.1}s {} {}",
                            d,
                            reader.buffer_position(),
                            ele_str("start", e).expect("?")
                        );
                    }
                    None => {}
                }
                match e.name().as_ref() {
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
                    n => {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected start tag {} {}",
                                std::str::from_utf8(n).expect("?"),
                                reader.buffer_position()
                            ),
                        ));
                    }
                }
            }
            Ok(Event::Empty(ref e)) => {
                match cktm.checktime() {
                    Some(d) => {
                        message!(
                            "{:5.1}s {} {}",
                            d,
                            reader.buffer_position(),
                            ele_str("empty", e).expect("?")
                        );
                    }
                    None => {}
                }
                match e.name().as_ref() {
                    b"node" => {
                        res.add_node(read_node(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    b"way" => {
                        res.add_way(read_way(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    b"relation" => {
                        res.add_relation(read_relation(&mut reader, &mut buf2, e, ct, false)?);
                    }
                    n => {
                        return Err(Error::XmlDataError(
                            format!(
                                "unexpected empty tag {} {}",
                                std::str::from_utf8(n).expect("?"),
                                reader.buffer_position()
                            ),
                        ));
                    }
                }
            }
            Ok(Event::End(ref e)) => match e.name().as_ref() {
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
                n => {
                    return Err(Error::XmlDataError(
                        format!(
                            "unexpected end tag {} {}",
                            std::str::from_utf8(n).expect("?"),
                            reader.buffer_position()
                        ),
                    ));
                }
            },

            Ok(Event::Eof) => {
                break;
            }

            Ok(Event::Text(e)) => {
                if !all_whitespace(e.as_ref()) {
                    return Err(Error::XmlDataError(
                        format!(
                            "unexpected text {}: {}",
                            reader.buffer_position(),
                            e.unescape().unwrap()
                        ),
                    ));
                }
            }
            Ok(Event::CData(e)) => {
                return Err(Error::XmlDataError(
                    format!(
                        "unexpected cdata {}: {:?}",
                        reader.buffer_position(),
                        e.as_ref()
                    ),
                ));
            }
            Ok(_) => {}

            Err(e) => {
                return Err(Error::XmlDataError(
                    format!("Error at position {}: {:?}", reader.buffer_position(), e),
                ));
            }
        }
        buf.clear();
    }
    message!(
        "{:5.1}s: {} nodes, {} ways, {} relations",
        cktm.gettime(),
        res.nodes.len(),
        res.ways.len(),
        res.relations.len()
    );
    Ok(res)
}
