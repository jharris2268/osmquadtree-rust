use crate::callback::CallFinish;
use crate::elements::{ElementType, Relation, Tag, Way};
use crate::geometry::style::{OpType, RelationTagSpec};
use crate::geometry::{GeometryStyle, OtherData, Timings, WorkingBlock};
use crate::utils::ThreadTimer;

use std::collections::BTreeMap;
use std::sync::Arc;

pub struct AddRelationTags<T: ?Sized> {
    out: Box<T>,
    pending: BTreeMap<i64, Vec<(usize, String)>>,
    style: Arc<GeometryStyle>,

    tm: f64,
    num_rels: usize,
    num_ways: usize,
    num_tags: usize,
}

fn match_tag_spec(spec: &RelationTagSpec, tgs: &Vec<Tag>) -> Option<String> {
    let mut has_tags = 0;
    let mut val: Option<&str> = None;
    for t in tgs {
        match spec.source_filter.get(&t.key) {
            None => {}
            Some(v) => {
                if &t.val == v {
                    has_tags += 1;
                }
            }
        }
        if t.key == spec.source_key {
            val = Some(&t.val);
        }
    }
    if has_tags == spec.source_filter.len() && !val.is_none() {
        Some(val.unwrap().to_string())
    } else {
        None
    }
}

fn collect_vals_min(i: usize, vals: &Vec<(usize, String)>) -> Option<String> {
    let mut res: Option<i64> = None;
    for (a, b) in vals {
        if &i == a {
            match b.parse::<i64>() {
                Ok(p) => match res.as_mut() {
                    None => {
                        res = Some(p);
                    }
                    Some(q) => {
                        res = Some(i64::min(p, *q));
                    }
                },
                Err(_) => {}
            }
        }
    }
    match res {
        None => None,
        Some(p) => Some(p.to_string()),
    }
}
fn collect_vals_max(i: usize, vals: &Vec<(usize, String)>) -> Option<String> {
    let mut res: Option<i64> = None;
    for (a, b) in vals {
        if &i == a {
            match b.parse::<i64>() {
                Ok(p) => match res.as_mut() {
                    None => {
                        res = Some(p);
                    }
                    Some(q) => {
                        res = Some(i64::max(p, *q));
                    }
                },
                Err(_) => {}
            }
        }
    }
    match res {
        None => None,
        Some(p) => Some(p.to_string()),
    }
}

fn collect_vals_list(i: usize, vals: &Vec<(usize, String)>) -> Option<String> {
    let mut res = String::new();
    let mut prev = &vals[0].1;
    for (a, b) in vals {
        if &i == a {
            if !res.is_empty() && b == prev {
                continue;
            }

            if !res.is_empty() {
                res += "; ";
            }
            res += b;
            prev = b;
        }
    }
    if res.is_empty() {
        None
    } else {
        Some(res)
    }
}

fn collect_vals(op_type: &OpType, i: usize, vals: &Vec<(usize, String)>) -> Option<String> {
    match op_type {
        OpType::Min => collect_vals_min(i, vals),
        OpType::Max => collect_vals_max(i, vals),
        OpType::List => collect_vals_list(i, vals),
    }
}

impl<T> AddRelationTags<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings> + ?Sized,
{
    pub fn new(out: Box<T>, style: Arc<GeometryStyle>) -> AddRelationTags<T> {
        AddRelationTags {
            out: out,
            style: style,
            pending: BTreeMap::new(),
            tm: 0.0,
            num_rels: 0,
            num_tags: 0,
            num_ways: 0,
        }
    }

    fn process_relation(&mut self, rel: &Relation) {
        let mut added = false;
        for (i, sp) in self.style.relation_tag_spec.iter().enumerate() {
            match match_tag_spec(sp, &rel.tags) {
                None => {}
                Some(val) => {
                    for m in &rel.members {
                        if m.mem_type == ElementType::Way {
                            match self.pending.get_mut(&m.mem_ref) {
                                None => {
                                    self.pending.insert(m.mem_ref, vec![(i, val.clone())]);
                                }
                                Some(xx) => {
                                    xx.push((i, val.clone()));
                                }
                            }
                        }
                    }
                    added = true;
                }
            }
        }
        if added {
            self.num_rels += 1;
        }
    }

    fn process_way(&mut self, w: &mut Way) {
        let mut added_tag = false;
        match self.pending.remove(&w.id) {
            None => {
                return;
            }
            Some(mut xx) => {
                xx.sort();
                for (i, sp) in self.style.relation_tag_spec.iter().enumerate() {
                    match collect_vals(&sp.op_type, i, &xx) {
                        None => {}
                        Some(v) => {
                            w.tags.push(Tag::new(sp.target_key.clone(), v));
                            added_tag = true;
                            self.num_tags += 1;
                        }
                    }
                }
            }
        }
        if added_tag {
            self.num_ways += 1;
        }
    }
}

impl<T> CallFinish for AddRelationTags<T>
where
    T: CallFinish<CallType = WorkingBlock, ReturnType = Timings> + ?Sized,
{
    type CallType = WorkingBlock;
    type ReturnType = Timings;

    fn call(&mut self, mut bl: WorkingBlock) {
        let tx = ThreadTimer::new();
        for r in bl.pending_relations.iter_mut() {
            self.process_relation(r);
        }

        for (w, _) in bl.pending_ways.iter_mut() {
            self.process_way(w);
        }

        self.tm += tx.since();
        self.out.call(bl);
    }

    fn finish(&mut self) -> std::io::Result<Timings> {
        let mut tms = self.out.finish()?;

        let m = format!(
            "added {} tags to {} ways from {} rels. {} ways not found",
            self.num_tags,
            self.num_ways,
            self.num_rels,
            self.pending.len()
        );

        tms.add("AddRelationTags", self.tm);
        tms.add_other("AddRelationTags", OtherData::Messages(vec![m]));

        Ok(tms)
    }
}
