use simple_protocolbuffers::{
    data_length, pack_data, pack_value, read_tag, un_zig_zag, value_length, zig_zag, IterTags,
    PbfTag,
};

use crate::elements::Quadtree;

pub use crate::elements::idset::IdSet;
pub use crate::elements::info::Info;
pub use crate::elements::node::Node;
pub use crate::elements::relation::{Member, Relation};
pub use crate::elements::tags::Tag;
pub use crate::elements::way::Way;

use crate::elements::common::PackStringTable;
use crate::elements::dense::Dense;
use crate::elements::traits::{Changetype, Element, ElementType, WithChangetype};

use std::io::{Error, ErrorKind, Result};

pub trait Block {
    fn with_quadtree(q: Quadtree) -> Self;
    fn get_index(&self) -> i64;
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree;
    fn get_end_date(&self) -> i64;

    fn len(&self) -> usize;
    fn weight(&self) -> usize;

    fn add_object(&mut self, ele: Element) -> Result<()>;
    fn sort(&mut self);
}

pub trait PackableBlock: Block {
    fn pack(&self) -> Result<Vec<u8>>;
    fn unpack(index: i64, data: &[u8]) -> Result<Self>
    where
        Self: Sized;
}


pub struct PrimitiveBlock {
    pub index: i64,
    pub location: u64,
    pub quadtree: Quadtree,
    pub start_date: i64,
    pub end_date: i64,
    pub nodes: Vec<Node>,
    pub ways: Vec<Way>,
    pub relations: Vec<Relation>,
}

impl std::fmt::Debug for PrimitiveBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PrimitieBlock {} at {} [{} nodes, {} ways, {} relations]", self.index, self.location, self.nodes.len(), self.ways.len(), self.relations.len())
    }
}


pub fn read_stringtable(data: &[u8]) -> Result<Vec<String>> {
    let mut res = Vec::new();
    for x in IterTags::new(&data) {
        match x {
            PbfTag::Data(1, d) => {
                let s = std::str::from_utf8(d).unwrap().to_string();
                res.push(s);
            }

            _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
        }
    }
    Ok(res)
}

impl WithChangetype for Node {
    fn get_changetype(&self) -> Changetype {
        return self.changetype;
    }
}
impl WithChangetype for Way {
    fn get_changetype(&self) -> Changetype {
        return self.changetype;
    }
}
impl WithChangetype for Relation {
    fn get_changetype(&self) -> Changetype {
        return self.changetype;
    }
}

fn find_splits<O: WithChangetype>(objs: &Vec<O>) -> Vec<(Changetype, usize, usize)> {
    let mut res = Vec::new();
    if objs.is_empty() {
        return res;
    }
    let mut c = objs[0].get_changetype();
    let mut li = 0;
    for (i, o) in objs.iter().enumerate() {
        if i != 0 {
            let nc = o.get_changetype();
            if c != nc {
                res.push((c, li, i));
                c = nc;
                li = i;
            }
        }
    }
    res.push((c, li, objs.len()));
    res
}

impl Block for PrimitiveBlock {
    fn with_quadtree(q: Quadtree) -> Self {
        let mut b = PrimitiveBlock::new(0, 0);
        b.quadtree = q;
        b
    }
    fn get_index(&self) -> i64 {
        self.index
    }
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
    fn get_end_date(&self) -> i64 {
        self.end_date
    }

    fn len(&self) -> usize {
        self.nodes.len() + self.ways.len() + self.relations.len()
    }
    fn weight(&self) -> usize {
        self.nodes.len() + 8 * self.ways.len() + 20 * self.relations.len()
    }

    fn add_object(&mut self, ele: Element) -> Result<()> {
        match ele {
            Element::Node(n) => {
                self.nodes.push(n);
                Ok(())
            }
            Element::Way(w) => {
                self.ways.push(w);
                Ok(())
            }
            Element::Relation(r) => {
                self.relations.push(r);
                Ok(())
            }
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("wrong element type {:?}", ele),
            )),
        }
    }
    fn sort(&mut self) {
        self.nodes.sort();
        self.ways.sort();
        self.relations.sort();
    }
}

impl From<Node> for Element {
    fn from(n: Node) -> Element {
        Element::Node(n)
    }
}
impl From<Way> for Element {
    fn from(n: Way) -> Element {
        Element::Way(n)
    }
}
impl From<Relation> for Element {
    fn from(n: Relation) -> Element {
        Element::Relation(n)
    }
}

impl IntoIterator for PrimitiveBlock {
    type Item = Element;
    type IntoIter = Box<dyn Iterator<Item = Element>>;
    fn into_iter(self: Self) -> Self::IntoIter {
        Box::new(
            self.nodes
                .into_iter()
                .map(Element::from)
                .chain(self.ways.into_iter().map(Element::from))
                .chain(self.relations.into_iter().map(Element::from)),
        )
    }
}

pub struct SortablePrimitiveBlock(PrimitiveBlock);
impl Block for SortablePrimitiveBlock {
    fn with_quadtree(q: Quadtree) -> Self {
        SortablePrimitiveBlock(PrimitiveBlock::with_quadtree(q))
    }
    fn get_index(&self) -> i64 {
        self.0.get_index()
    }
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.0.get_quadtree()
    }
    fn get_end_date(&self) -> i64 {
        self.0.get_end_date()
    }

    fn len(&self) -> usize {
        self.0.len()
    }
    fn weight(&self) -> usize {
        self.0.weight()
    }

    fn add_object(&mut self, ele: Element) -> Result<()> {
        self.0.add_object(ele)
    }

    fn sort(&mut self) {
        self.0.sort();
    }
}
impl PackableBlock for SortablePrimitiveBlock {
    fn pack(&self) -> Result<Vec<u8>> {
        self.0.pack(true, true)
    }

    fn unpack(index: i64, data: &[u8]) -> Result<Self> {
        Ok(SortablePrimitiveBlock(PrimitiveBlock::read(
            index, 0, data, false, false,
        )?))
    }
}

impl PrimitiveBlock {
    pub fn new(index: i64, location: u64) -> PrimitiveBlock {
        PrimitiveBlock {
            index: index,
            location: location,
            quadtree: Quadtree::new(0),
            start_date: 0,
            end_date: 0,
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
        }
    }

    pub fn extend(&mut self, mut other: PrimitiveBlock) {
        self.nodes.extend(std::mem::take(&mut other.nodes));
        self.ways.extend(std::mem::take(&mut other.ways));
        self.relations.extend(std::mem::take(&mut other.relations));
    }

    pub fn read(
        index: i64,
        location: u64,
        data: &[u8],
        ischange: bool,
        minimal: bool,
    ) -> Result<PrimitiveBlock> {
        Self::read_check_ids(index, location, data, ischange, minimal, None)
    }

    pub fn read_check_ids(
        index: i64,
        location: u64,
        data: &[u8],
        ischange: bool,
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<PrimitiveBlock> {
        let mut res = PrimitiveBlock::new(index, location);

        let mut strings = Vec::new();
        let mut groups = Vec::new();
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, d) => {
                    if !minimal {
                        strings = read_stringtable(&d)?
                    }
                }
                PbfTag::Data(2, d) => groups.push(d),

                PbfTag::Value(32, qt) => res.quadtree = Quadtree::new(un_zig_zag(qt)),
                PbfTag::Value(33, sd) => res.start_date = sd as i64,
                PbfTag::Value(34, ed) => res.end_date = ed as i64,

                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }

        for g in groups {
            let ct = PrimitiveBlock::find_changetype(&g, ischange);
            res.read_group(&strings, ct, &g, minimal, idset)?;
            drop(g);
        }
        drop(strings);

        Ok(res)
    }

    fn find_changetype(data: &[u8], ischange: bool) -> Changetype {
        if !ischange {
            return Changetype::Normal;
        }
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(10, ct) => {
                    return Changetype::from_int(ct);
                }
                _ => {}
            }
        }
        Changetype::Normal
    }

    fn read_group(
        &mut self,
        strings: &Vec<String>,
        changetype: Changetype,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<u64> {
        let mut count = 0;
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, d) => {
                    count += self.read_node(strings, changetype, &d, minimal, idset)?
                }
                PbfTag::Data(2, d) => {
                    count += self.read_dense(strings, changetype, &d, minimal, idset)?
                }
                PbfTag::Data(3, d) => {
                    count += self.read_way(strings, changetype, &d, minimal, idset)?
                }
                PbfTag::Data(4, d) => {
                    count += self.read_relation(strings, changetype, &d, minimal, idset)?
                }
                PbfTag::Value(10, _) => {}
                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }
        Ok(count)
    }

    fn read_node(
        &mut self,
        strings: &Vec<String>,
        changetype: Changetype,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Node, get_id(&data)) {
                    return Ok(0);
                }
            }
            None => {}
        }
        let n = Node::read(changetype, &strings, &data, minimal)?;
        self.nodes.push(n);
        Ok(1)
    }
    fn read_way(
        &mut self,
        strings: &Vec<String>,
        changetype: Changetype,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Way, get_id(&data)) {
                    return Ok(0);
                }
            }
            None => {}
        }
        let w = Way::read(changetype, &strings, &data, minimal)?;
        self.ways.push(w);
        Ok(1)
    }
    fn read_relation(
        &mut self,
        strings: &Vec<String>,
        changetype: Changetype,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<u64> {
        match idset {
            Some(idset) => {
                if !idset.contains(ElementType::Relation, get_id(&data)) {
                    return Ok(0);
                }
            }
            None => {}
        }
        let r = Relation::read(changetype, &strings, &data, minimal)?;
        self.relations.push(r);
        Ok(1)
    }
    fn read_dense(
        &mut self,
        strings: &Vec<String>,
        changetype: Changetype,
        data: &[u8],
        minimal: bool,
        idset: Option<&dyn IdSet>,
    ) -> Result<u64> {
        let nn = Dense::read(changetype, &strings, &data, minimal, idset)?;
        let nl = nn.len() as u64;
        for n in nn {
            self.nodes.push(n);
        }

        Ok(nl)
    }

    pub fn pack(&self, include_qts: bool, as_change: bool) -> Result<Vec<u8>> {
        let mut pack_strings = Box::new(PackStringTable::new());

        let mut groups = Vec::new();
        if self.nodes.len() > 0 {
            groups.extend(self.pack_nodes(&mut pack_strings, include_qts, as_change)?);
        }
        if self.ways.len() > 0 {
            groups.extend(self.pack_ways(&mut pack_strings, include_qts, as_change)?);
        }
        if self.relations.len() > 0 {
            groups.extend(self.pack_relations(&mut pack_strings, include_qts, as_change)?);
        }

        let pp = pack_strings.pack();
        let mut outl = data_length(1, pp.len());
        for g in &groups {
            outl += data_length(2, g.len());
        }

        if include_qts {
            outl += value_length(32, zig_zag(self.quadtree.as_int()));
            outl += value_length(33, self.start_date as u64);
            outl += value_length(34, self.end_date as u64);
        }

        let mut res = Vec::with_capacity(outl);
        pack_data(&mut res, 1, &pp);
        for g in groups {
            pack_data(&mut res, 2, &g);
        }
        if include_qts {
            pack_value(&mut res, 32, zig_zag(self.quadtree.as_int()));
            if self.start_date != 0 {
                pack_value(&mut res, 33, self.start_date as u64);
            }
            if self.end_date != 0 {
                pack_value(&mut res, 34, self.end_date as u64);
            }
        }
        Ok(res)
    }

    fn pack_nodes(
        &self,
        prep_strings: &mut Box<PackStringTable>,
        include_qts: bool,
        as_change: bool,
    ) -> Result<Vec<Vec<u8>>> {
        if as_change {
            let mut pp = Vec::new();
            for (a, b, c) in find_splits(&self.nodes) {
                let mut res = Vec::new();
                pack_data(
                    &mut res,
                    2,
                    &Dense::pack(&self.nodes[b..c], prep_strings, include_qts)?,
                );
                pack_value(&mut res, 10, a.as_int());
                pp.push(res);
            }
            return Ok(pp);
        }

        let mut res = Vec::new();
        pack_data(
            &mut res,
            2,
            &Dense::pack(&self.nodes, prep_strings, include_qts)?,
        );
        return Ok(vec![res]);
    }

    fn pack_ways(
        &self,
        prep_strings: &mut Box<PackStringTable>,
        include_qts: bool,
        as_change: bool,
    ) -> Result<Vec<Vec<u8>>> {
        if as_change {
            let mut pp = Vec::new();
            for (a, b, c) in find_splits(&self.ways) {
                let mut res = Vec::new();
                for w in &self.ways[b..c] {
                    pack_data(&mut res, 3, &w.pack(prep_strings, include_qts)?);
                }
                pack_value(&mut res, 10, a.as_int());
                pp.push(res);
            }
            return Ok(pp);
        }

        let mut res = Vec::new();
        for w in &self.ways {
            pack_data(&mut res, 3, &w.pack(prep_strings, include_qts)?);
        }
        return Ok(vec![res]);
    }

    fn pack_relations(
        &self,
        prep_strings: &mut Box<PackStringTable>,
        include_qts: bool,
        as_change: bool,
    ) -> Result<Vec<Vec<u8>>> {
        if as_change {
            let mut pp = Vec::new();
            for (a, b, c) in find_splits(&self.relations) {
                let mut res = Vec::new();
                for r in &self.relations[b..c] {
                    pack_data(&mut res, 4, &r.pack(prep_strings, include_qts)?);
                }
                pack_value(&mut res, 10, a.as_int());
                pp.push(res);
            }
            return Ok(pp);
        }
        let mut res = Vec::new();
        for r in &self.relations {
            pack_data(&mut res, 4, &r.pack(prep_strings, include_qts)?);
        }
        return Ok(vec![res]);
    }
}

fn get_id(data: &[u8]) -> i64 {
    match read_tag(data, 0) {
        (PbfTag::Value(1, i), _) => i as i64,
        _ => 0,
    }
}
