use crate::elements::quadtree;
use crate::elements::quadtree::Quadtree;
use crate::elements::traits::*;
use simple_protocolbuffers::{
    data_length, pack_data, pack_delta_int, pack_delta_int_ref, pack_value, read_delta_packed_int,
    read_packed_int, un_zig_zag, value_length, zig_zag, IterTags, PbfTag,
};

use std::cmp::Ordering;
use std::io;
use std::io::{Error, ErrorKind};

#[derive(Debug, Clone, Eq)]
pub struct MinimalNode {
    pub changetype: Changetype,
    pub id: i64,
    pub version: u32,
    pub timestamp: i64,
    pub quadtree: Quadtree,
    pub lon: i32,
    pub lat: i32,
}

impl MinimalNode {
    pub fn new() -> MinimalNode {
        MinimalNode {
            changetype: Changetype::Normal,
            id: 0,
            version: 0,
            timestamp: 0,
            quadtree: Quadtree::empty(),
            lon: 0,
            lat: 0,
        }
    }
}

impl WithType for MinimalNode {
    fn get_type(&self) -> ElementType {
        ElementType::Node
    }
}

impl WithId for MinimalNode {
    fn get_id(&self) -> i64 {
        self.id
    }
}
impl WithTimestamp for MinimalNode {
    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
}
impl WithVersion for MinimalNode {
    fn get_version(&self) -> i64 {
        self.version as i64
    }
}

impl WithQuadtree for MinimalNode {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl Ord for MinimalNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.id.cmp(&other.id);
        if a != Ordering::Equal {
            return a;
        }

        let b = self.version.cmp(&other.version);
        if b != Ordering::Equal {
            return b;
        }

        self.changetype.cmp(&other.changetype)
    }
}

impl PartialOrd for MinimalNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl PartialEq for MinimalNode {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.version == other.version && self.changetype == other.changetype
    }
}

#[derive(Debug, Eq)]
pub struct MinimalWay {
    pub changetype: Changetype,
    pub id: i64,
    pub version: u32,
    pub timestamp: i64,
    pub quadtree: Quadtree,
    pub refs_data: Vec<u8>,
}
impl MinimalWay {
    pub fn new() -> MinimalWay {
        MinimalWay {
            changetype: Changetype::Normal,
            id: 0,
            version: 0,
            timestamp: 0,
            quadtree: Quadtree::empty(),
            refs_data: Vec::new(),
        }
    }
}

impl WithType for MinimalWay {
    fn get_type(&self) -> ElementType {
        ElementType::Way
    }
}

impl WithId for MinimalWay {
    fn get_id(&self) -> i64 {
        self.id
    }
}
impl WithTimestamp for MinimalWay {
    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
}
impl WithVersion for MinimalWay {
    fn get_version(&self) -> i64 {
        self.version as i64
    }
}

impl WithQuadtree for MinimalWay {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl Ord for MinimalWay {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.id.cmp(&other.id);
        if a != Ordering::Equal {
            return a;
        }

        let b = self.version.cmp(&other.version);
        if b != Ordering::Equal {
            return b;
        }

        self.changetype.cmp(&other.changetype)
    }
}

impl PartialOrd for MinimalWay {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl PartialEq for MinimalWay {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.version == other.version && self.changetype == other.changetype
    }
}

#[derive(Debug, Eq)]
pub struct MinimalRelation {
    pub changetype: Changetype,
    pub id: i64,
    pub version: u32,
    pub timestamp: i64,
    pub quadtree: Quadtree,
    pub types_data: Vec<u8>,
    pub refs_data: Vec<u8>,
}

impl MinimalRelation {
    pub fn new() -> MinimalRelation {
        MinimalRelation {
            changetype: Changetype::Normal,
            id: 0,
            version: 0,
            timestamp: 0,
            quadtree: Quadtree::empty(),
            types_data: Vec::new(),
            refs_data: Vec::new(),
        }
    }
}

impl WithType for MinimalRelation {
    fn get_type(&self) -> ElementType {
        ElementType::Relation
    }
}

impl WithId for MinimalRelation {
    fn get_id(&self) -> i64 {
        self.id
    }
}
impl WithTimestamp for MinimalRelation {
    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }
}
impl WithVersion for MinimalRelation {
    fn get_version(&self) -> i64 {
        self.version as i64
    }
}

impl WithQuadtree for MinimalRelation {
    fn get_quadtree<'a>(&'a self) -> &'a Quadtree {
        &self.quadtree
    }
}

impl Ord for MinimalRelation {
    fn cmp(&self, other: &Self) -> Ordering {
        let a = self.id.cmp(&other.id);
        if a != Ordering::Equal {
            return a;
        }

        let b = self.version.cmp(&other.version);
        if b != Ordering::Equal {
            return b;
        }

        self.changetype.cmp(&other.changetype)
    }
}

impl PartialOrd for MinimalRelation {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
    }
}

impl PartialEq for MinimalRelation {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.version == other.version && self.changetype == other.changetype
    }
}
#[derive(Debug)]
pub struct MinimalBlock {
    pub index: i64,
    pub location: u64,
    pub quadtree: quadtree::Quadtree,
    pub start_date: i64,
    pub end_date: i64,
    pub nodes: Vec<MinimalNode>,
    pub ways: Vec<MinimalWay>,
    pub relations: Vec<MinimalRelation>,
}

impl MinimalBlock {
    pub fn new() -> MinimalBlock {
        MinimalBlock {
            index: 0,
            location: 0,
            quadtree: quadtree::Quadtree::new(-2),
            start_date: 0,
            end_date: 0,
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
        }
    }

    pub fn read(
        index: i64,
        location: u64,
        data: &[u8],
        ischange: bool,
    ) -> Result<MinimalBlock, Error> {
        MinimalBlock::read_parts(index, location, data, ischange, true, true, true)
    }

    pub fn len(&self) -> usize {
        self.nodes.len() + self.ways.len() + self.relations.len()
    }
    pub fn read_parts(
        index: i64,
        location: u64,
        data: &[u8],
        ischange: bool,
        readnodes: bool,
        readways: bool,
        readrelations: bool,
    ) -> Result<MinimalBlock, Error> {
        let mut res = MinimalBlock::new();
        res.index = index;
        res.location = location;

        let mut groups = Vec::new();
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, _) => {}
                PbfTag::Data(2, d) => groups.push(d),

                PbfTag::Value(32, qt) => res.quadtree = quadtree::Quadtree::new(un_zig_zag(qt)),
                PbfTag::Value(33, sd) => res.start_date = sd as i64,
                PbfTag::Value(34, ed) => res.end_date = ed as i64,

                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }

        for g in groups {
            let ct = MinimalBlock::find_changetype(&g, ischange);
            res.read_group(ct, &g, readnodes, readways, readrelations)?;
            drop(g);
        }

        Ok(res)
    }

    fn find_changetype(data: &[u8], ischange: bool) -> Changetype {
        if !ischange {
            return Changetype::Normal;
        }
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(10, ct) => return Changetype::from_int(ct),
                _ => {}
            }
        }
        Changetype::Normal
    }

    fn read_group(
        &mut self,
        changetype: Changetype,
        data: &[u8],
        readnodes: bool,
        readways: bool,
        readrelations: bool,
    ) -> Result<u64, Error> {
        let mut count = 0;
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, d) => {
                    if readnodes {
                        count += self.read_node(changetype, &d)?;
                    }
                }
                PbfTag::Data(2, d) => {
                    if readnodes {
                        count += self.read_dense(changetype, &d)?;
                    }
                }
                PbfTag::Data(3, d) => {
                    if readways {
                        count += self.read_way(changetype, &d)?;
                    }
                }
                PbfTag::Data(4, d) => {
                    if readrelations {
                        count += self.read_relation(changetype, &d)?;
                    }
                }
                PbfTag::Value(10, _) => {}
                _ => return Err(Error::new(ErrorKind::Other, "unexpected item")),
            }
        }
        Ok(count)
    }

    fn read_node(&mut self, changetype: Changetype, data: &[u8]) -> Result<u64, Error> {
        let mut nd = MinimalNode::new();
        nd.changetype = changetype;
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(1, i) => nd.id = i as i64,
                PbfTag::Data(4, info_data) => {
                    for y in IterTags::new(&info_data) {
                        match y {
                            PbfTag::Value(1, v) => nd.version = v as u32,
                            PbfTag::Value(2, v) => nd.timestamp = v as i64,
                            _ => {}
                        }
                    }
                }
                PbfTag::Value(7, i) => nd.lat = i as i32,
                PbfTag::Value(8, i) => nd.lon = i as i32,
                PbfTag::Value(20, i) => nd.quadtree = Quadtree::new(un_zig_zag(i)),
                _ => {}
            }
        }

        self.nodes.push(nd);
        Ok(1)
    }
    fn read_way(&mut self, changetype: Changetype, data: &[u8]) -> Result<u64, Error> {
        let mut wy = MinimalWay::new();
        wy.changetype = changetype;
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(1, i) => wy.id = i as i64,
                PbfTag::Data(4, info_data) => {
                    for y in IterTags::new(&info_data) {
                        match y {
                            PbfTag::Value(1, v) => wy.version = v as u32,
                            PbfTag::Value(2, v) => wy.timestamp = v as i64,
                            _ => {}
                        }
                    }
                }
                PbfTag::Data(8, d) => wy.refs_data = d.to_vec(),
                PbfTag::Value(20, i) => wy.quadtree = Quadtree::new(un_zig_zag(i)),
                _ => {}
            }
        }

        self.ways.push(wy);
        Ok(1)
    }
    fn read_relation(&mut self, changetype: Changetype, data: &[u8]) -> Result<u64, Error> {
        let mut rl = MinimalRelation::new();
        rl.changetype = changetype;
        for x in IterTags::new(&data) {
            match x {
                PbfTag::Value(1, i) => rl.id = i as i64,
                PbfTag::Data(4, info_data) => {
                    for y in IterTags::new(&info_data) {
                        match y {
                            PbfTag::Value(1, v) => rl.version = v as u32,
                            PbfTag::Value(2, v) => rl.timestamp = v as i64,
                            _ => {}
                        }
                    }
                }
                PbfTag::Data(9, d) => rl.refs_data = d.to_vec(),
                PbfTag::Data(10, d) => rl.types_data = d.to_vec(),
                PbfTag::Value(20, i) => rl.quadtree = Quadtree::new(un_zig_zag(i)),
                _ => {}
            }
        }

        self.relations.push(rl);
        Ok(1)
    }
    fn read_dense(&mut self, changetype: Changetype, data: &[u8]) -> Result<u64, Error> {
        let mut ids = Vec::new();
        let mut lons = Vec::new();
        let mut lats = Vec::new();

        let mut qts = Vec::new();
        let mut vs = Vec::new();
        let mut ts = Vec::new();

        for x in IterTags::new(&data) {
            match x {
                PbfTag::Data(1, d) => ids = read_delta_packed_int(&d),
                PbfTag::Data(5, d) => {
                    for y in IterTags::new(&d) {
                        match y {
                            PbfTag::Data(1, d) => vs = read_packed_int(&d), //version NOT delta packed
                            PbfTag::Data(2, d) => ts = read_delta_packed_int(&d),

                            _ => {}
                        }
                    }
                }
                PbfTag::Data(8, d) => lats = read_delta_packed_int(&d),
                PbfTag::Data(9, d) => lons = read_delta_packed_int(&d),

                PbfTag::Data(20, d) => qts = read_delta_packed_int(&d),
                _ => {}
            }
        }

        if ids.len() == 0 {
            return Ok(0);
        }
        if lats.len() > 0 && lats.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} lats", ids.len(), lats.len()),
            ));
        }
        if lons.len() > 0 && lons.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} lons", ids.len(), lons.len()),
            ));
        }
        if qts.len() > 0 && qts.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} qts", ids.len(), qts.len()),
            ));
        }

        if vs.len() > 0 && vs.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} infos", ids.len(), vs.len()),
            ));
        }
        if ts.len() > 0 && ts.len() != ids.len() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("dense nodes: {} ids but {} timestamps", ids.len(), ts.len()),
            ));
        }

        self.nodes.reserve(self.nodes.len() + ids.len());

        for i in 0..ids.len() {
            let mut nd = MinimalNode::new();
            nd.changetype = changetype;
            nd.id = ids[i] as i64;

            if lats.len() > 0 {
                nd.lat = lats[i] as i32;
            }
            if lons.len() > 0 {
                nd.lon = lons[i] as i32;
            }
            if qts.len() > 0 {
                nd.quadtree = Quadtree::new(qts[i]);
            }

            if vs.len() > 0 {
                nd.version = vs[i] as u32;
            }
            if ts.len() > 0 {
                nd.timestamp = ts[i] as i64;
            }

            self.nodes.push(nd);
        }

        Ok(ids.len() as u64)
    }
}

pub struct QuadtreeBlock {
    pub idx: i64,
    pub loc: u64,
    pub nodes: Vec<(i64, Quadtree)>,
    pub ways: Vec<(i64, Quadtree)>,
    pub relations: Vec<(i64, Quadtree)>,
}

fn unpack_id_qt(data: &[u8]) -> io::Result<(i64, Quadtree)> {
    let mut i = 0;
    let mut qt = -1;
    for t in IterTags::new(data) {
        match t {
            PbfTag::Value(1, x) => {
                i = x as i64;
            }
            PbfTag::Value(20, x) => {
                qt = un_zig_zag(x);
            }
            _ => {}
        }
    }
    if i == 0 {
        return Err(Error::new(ErrorKind::Other, "no id"));
    }
    if qt == -1 {
        return Err(Error::new(ErrorKind::Other, "no qt"));
    }
    Ok((i, Quadtree::new(qt)))
}

fn unpack_dense(nodes: &mut Vec<(i64, Quadtree)>, data: &[u8]) -> io::Result<()> {
    let mut nn = Vec::new();
    let mut qq = Vec::new();

    for t in IterTags::new(data) {
        match t {
            PbfTag::Data(1, x) => {
                nn = read_delta_packed_int(x);
            }
            PbfTag::Data(20, x) => {
                qq = read_delta_packed_int(x);
            }
            _ => {}
        }
    }
    if nn.is_empty() {
        return Err(Error::new(ErrorKind::Other, "no id"));
    }
    if qq.is_empty() {
        return Err(Error::new(ErrorKind::Other, "no qt"));
    }
    if nn.len() != qq.len() {
        return Err(Error::new(ErrorKind::Other, "id.len()!=qt.len()"));
    }
    nodes.reserve(nodes.len() + nn.len());
    nodes.extend(nn.iter().zip(qq).map(|(a, b)| (*a, Quadtree::new(b))));
    Ok(())
}

impl QuadtreeBlock {
    pub fn new() -> QuadtreeBlock {
        QuadtreeBlock {
            idx: 0,
            loc: 0,
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
        }
    }
    pub fn with_capacity(cap: usize) -> QuadtreeBlock {
        QuadtreeBlock {
            idx: 0,
            loc: 0,
            nodes: Vec::with_capacity(cap),
            ways: Vec::with_capacity(cap),
            relations: Vec::with_capacity(cap),
        }
    }
    pub fn len(&self) -> usize {
        self.nodes.len() + self.ways.len() + self.relations.len()
    }
    pub fn clear(&mut self) {
        self.nodes.clear();
        self.ways.clear();
        self.relations.clear();
    }

    pub fn add_node(&mut self, n: i64, q: Quadtree) {
        self.nodes.push((n, q));
    }
    pub fn add_way(&mut self, n: i64, q: Quadtree) {
        self.ways.push((n, q));
    }
    pub fn add_relation(&mut self, n: i64, q: Quadtree) {
        self.relations.push((n, q));
    }

    pub fn pack(&mut self) -> io::Result<Vec<u8>> {
        let mut res = Vec::new();
        if !self.nodes.is_empty() {
            self.nodes.sort_by_key(|(x, _)| *x);
            pack_data(&mut res, 2, &self.pack_nodes());
        }
        if !self.ways.is_empty() {
            self.ways.sort_by_key(|(x, _)| *x);
            pack_data(&mut res, 2, &self.pack_ways());
        }
        if !self.relations.is_empty() {
            self.relations.sort_by_key(|(x, _)| *x);
            pack_data(&mut res, 2, &self.pack_relations());
        }

        Ok(res)
    }

    fn pack_nodes(&self) -> Vec<u8> {
        let nn = pack_delta_int_ref(self.nodes.iter().map(|(x, _)| x));
        let qq = pack_delta_int(self.nodes.iter().map(|(_, q)| q.as_int()));
        let ll = pack_delta_int(self.nodes.iter().map(|_| 0));

        let l = data_length(1, nn.len())
            + data_length(8, ll.len())
            + data_length(9, ll.len())
            + data_length(20, qq.len());
        let mut r = Vec::with_capacity(l);
        pack_data(&mut r, 1, &nn);
        pack_data(&mut r, 8, &ll);
        pack_data(&mut r, 9, &ll);
        pack_data(&mut r, 20, &qq);

        let mut r2 = Vec::with_capacity(data_length(2, l));
        pack_data(&mut r2, 2, &r);
        r2
    }

    fn pack_ways(&self) -> Vec<u8> {
        let mut l = 0;
        for (w, q) in &self.ways {
            l += data_length(
                2,
                value_length(1, *w as u64) + value_length(20, zig_zag(q.as_int())),
            );
        }

        let mut r2 = Vec::with_capacity(l);
        for (w, q) in &self.ways {
            let mut r = Vec::with_capacity(
                value_length(1, *w as u64) + value_length(20, zig_zag(q.as_int())),
            );
            pack_value(&mut r, 1, *w as u64);
            pack_value(&mut r, 20, zig_zag(q.as_int()));

            pack_data(&mut r2, 3, &r);
        }
        r2
    }

    fn pack_relations(&self) -> Vec<u8> {
        let mut l = 0;
        for (w, q) in &self.ways {
            l += data_length(
                2,
                value_length(1, *w as u64) + value_length(20, zig_zag(q.as_int())),
            );
        }

        let mut r2 = Vec::with_capacity(l);
        for (w, q) in &self.relations {
            let mut r = Vec::with_capacity(
                value_length(1, *w as u64) + value_length(20, zig_zag(q.as_int())),
            );
            pack_value(&mut r, 1, *w as u64);
            pack_value(&mut r, 20, zig_zag(q.as_int()));

            pack_data(&mut r2, 4, &r);
        }
        r2
    }

    pub fn unpack(i: i64, loc: u64, data: &[u8]) -> io::Result<QuadtreeBlock> {
        let mut r = QuadtreeBlock {
            idx: i,
            loc: loc,
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
        };

        for t in IterTags::new(data) {
            match t {
                PbfTag::Data(2, d2) => {
                    for t2 in IterTags::new(d2) {
                        match t2 {
                            PbfTag::Data(1, d3) => {
                                r.nodes.push(unpack_id_qt(d3)?);
                            }
                            PbfTag::Data(2, d3) => {
                                unpack_dense(&mut r.nodes, d3)?;
                            }
                            PbfTag::Data(3, d3) => {
                                r.ways.push(unpack_id_qt(d3)?);
                            }
                            PbfTag::Data(4, d3) => r.relations.push(unpack_id_qt(d3)?),
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(r)
    }
}

impl std::fmt::Display for QuadtreeBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "QuadtreeBlock[ {} nodes, {} ways, {} relations]",
            self.nodes.len(),
            self.ways.len(),
            self.relations.len()
        )
    }
}
