use crate::sortblocks::{QuadtreeTree, WriteTempData, WriteTempFile, WriteTempFileSplit, TempData,CollectTemp,SortBlocks,read_temp_data};
use channelled_callbacks::{CallFinish, Callback, CallbackMerge, CallbackSync, CallAll, MergeTimings, ReplaceNoneWithTimings};
use crate::utils::ThreadTimer;
use crate::pbfformat::{HeaderType, FileBlock, WriteFile, pack_file_block,};
use crate::update::ParallelFileLocs;
use crate::geometry::{GeometryBlock,CallFinishGeometryBlock,Timings,OtherData};
use crate::elements::{Bbox,};
use crate::message;
use std::io::Result;
use std::sync::Arc;



fn pack_geom(bl: GeometryBlock) -> Vec<(i64, Vec<u8>)> {
    let p = bl.pack().expect("!");
    let q = pack_file_block("OSMData", &p, true).expect("?");

    vec![(bl.quadtree.as_int(), q)]
}

struct WrapWriteFile(WriteFile);

impl CallFinish for WrapWriteFile {
    type CallType = Vec<(i64, Vec<u8>)>;
    type ReturnType = Timings;

    fn call(&mut self, p: Self::CallType) {
        self.0.call(p);
    }

    fn finish(&mut self) -> Result<Timings> {
        let (t, _) = self.0.finish()?;
        let mut tms = Timings::new();
        tms.add("WriteFile", t);
        Ok(tms)
    }
}

pub(crate) fn prep_write_geometry_pbffile(ofn: &str, bbox: &Bbox, numchan: usize) -> Result<CallFinishGeometryBlock> {
    
    let wf = Box::new(WrapWriteFile(WriteFile::with_bbox(
            &ofn,
            HeaderType::NoLocs,
            Some(&bbox),
        )));
                
    if numchan == 0 {
        
        Ok(Box::new(CallAll::new(wf, "pack_geometry", Box::new(pack_geom))))
    } else {
        let wts = CallbackSync::new(wf,numchan);
        let mut pps: Vec<
            Box<dyn CallFinish<CallType = GeometryBlock, ReturnType = Timings>>,
        > = Vec::new();
        for wt in wts {
            let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
            pps.push(Box::new(Callback::new(Box::new(CallAll::new(
                wt2,
                "pack_geometry",
                Box::new(pack_geom),
            )))));
        }
        Ok(Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new()))))
    }
}


fn prep_groups_from_filelocs(filelocs: &ParallelFileLocs, max_minzoom: &Option<i64>) -> Result<Arc<QuadtreeTree>> {
    
    let mut groups = QuadtreeTree::new();
    
    for p in &filelocs.1 {
        let q = p.0.clone();
        match max_minzoom {
            None => { groups.add(&q,1); },
            Some(m) => {
                let mi = *m as usize;
                if mi <= q.depth() {
                    groups.add(&q,1);
                } else {
                    
                    groups.add(&q.round(mi),1);
                }
            }
        }
    }
    
    Ok(Arc::new(groups))
}
    
struct WrapWriteTemp<T> {
    out: Box<T>
}

impl<T> WrapWriteTemp<T>
    where T: CallFinish<CallType=Vec<(i64,Vec<u8>)>, ReturnType=crate::sortblocks::Timings>
{
    pub fn new(out: Box<T>) -> WrapWriteTemp<T> {
        WrapWriteTemp{out: out}
    }
}

impl<T> CallFinish for WrapWriteTemp<T>
    where T: CallFinish<CallType=Vec<(i64,Vec<u8>)>, ReturnType=crate::sortblocks::Timings>
{
    type CallType=Vec<(i64,Vec<u8>)>;
    type ReturnType = Timings;
    
    fn call(&mut self, t: Vec<(i64,Vec<u8>)>) {
        self.out.call(t);
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut orig = self.out.finish()?;
        let mut res = Timings::new();
        
        res.timings.extend(std::mem::take(&mut orig.timings));
        for (a,b) in std::mem::take(&mut orig.others) {
            match b {
                crate::sortblocks::OtherData::TempData(td) => {
                    res.add_other(&a,OtherData::TempData(td));
                },
                _ => { message!("!! {}", a); }
            }
        }
        Ok(res)
    }
        
}

struct CollectTempGeometry<T: ?Sized> {
    out: Box<T>,
    collect: CollectTemp<GeometryBlock>,
    tm: f64,
}

impl<T> CollectTempGeometry<T>
    where T: CallFinish<CallType=Vec<(i64,Vec<u8>)>,ReturnType=Timings> + ?Sized
{
    pub fn new(out: Box<T>, limit: usize, splitat: i64, groups: Arc<QuadtreeTree>) -> CollectTempGeometry<T> {
        CollectTempGeometry{out: out,  collect: CollectTemp::new(limit, splitat, groups), tm: 0.0}
    }
        
}

impl<T> CallFinish for CollectTempGeometry<T>
where
    T: CallFinish<CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings> + ?Sized
{
    type CallType = GeometryBlock;
    type ReturnType = Timings;

    fn call(&mut self, bl: GeometryBlock) {
        let tx = ThreadTimer::new();
        let mm = self.collect.add_all(bl.into_iter()).expect("?");
        let mut xx = Vec::new();
        for (a,b) in mm {
            let p = b.pack().expect("!");
            let q = pack_file_block("OSMData", &p, true).expect("?");
            xx.push((a,q));
        }
        self.tm += tx.since();

        self.out.call(xx);
    }

    fn finish(&mut self) -> Result<Timings> {
        
        let tx=ThreadTimer::new();
        let mm = self.collect.finish();
        let mut xx = Vec::new();
        for (a,b) in mm {
            let p = b.pack().expect("!");
            let q = pack_file_block("OSMData", &p, true).expect("?");
            xx.push((a,q));
        }
        let tf=tx.since();
        
        self.out.call(xx);

        let mut r = self.out.finish()?;
        r.add("CollectTempBlocks::call", self.tm);
        r.add("CollectTempBlocks::finish", tf);
        Ok(r)
    }
}

pub(crate) fn make_write_temp_geometry(outfn: &str, filelocs: &ParallelFileLocs, max_minzoom: &Option<i64>, numchan: usize) -> 
    Result<(CallFinishGeometryBlock, Arc<QuadtreeTree>)> {
    
    let groups = prep_groups_from_filelocs(&filelocs, &max_minzoom)?;
                
    let limit = 10000;
    let splitat = 32;
    let wt: Box<dyn CallFinish< CallType = Vec<(i64, Vec<u8>)>, ReturnType = Timings>> =
        if filelocs.2 < 100*1024*1024 {
            Box::new(WrapWriteTemp::new(Box::new(WriteTempData::new())))
        } else {
            let tempfn = format!("{}-temp.pbf", &outfn[..outfn.len()-4]);
            if filelocs.2 < 2048*1024*1024 {
                Box::new(WrapWriteTemp::new(Box::new(WriteTempFile::new(&tempfn))))
            } else {
                Box::new(WrapWriteTemp::new(Box::new(WriteTempFileSplit::new(&tempfn, filelocs.2 as i64 / 1024/1024/1024 ))))
            }
        };
    
        
    
    let ct: CallFinishGeometryBlock = if numchan == 0 {
        //let pc = Box::new(CallAll::new(wt, "pack_geometry", Box::new(pack_geom)));
        Box::new(CollectTempGeometry::new(wt, limit, splitat, groups.clone()))
    } else {
        let wts = CallbackSync::new(wt, numchan);

        let mut pcs: Vec<CallFinishGeometryBlock> =
            Vec::new();

        for wt in wts {
            let wt2 = Box::new(ReplaceNoneWithTimings::new(wt));
            //let pp = Box::new(CallAll::new(wt2, "pack_geometry", Box::new(pack_geom)));
            pcs.push(Box::new(Callback::new(Box::new(CollectTempGeometry::new(
                wt2,
                limit / numchan,
                splitat,
                groups.clone(),
            )))));
        }
        Box::new(CallbackMerge::new(pcs, Box::new(MergeTimings::new())))
    };
    
    Ok((ct, groups))
}


fn sort_temp_geoms(data: Vec<FileBlock>, groups: Arc<QuadtreeTree>) -> Result<Vec<(i64, Vec<u8>)>> {
    
    let mut blocks = SortBlocks::new(groups);
    
    for d in data {
        
        let g = GeometryBlock::unpack(0, &d.data())?;
        
        blocks.add_all(g.into_iter())?;
    }
    
    let mut res = Vec::new();
    for p in blocks.finish() {
        res.extend(pack_geom(p))
    }
    Ok(res)
}

struct SortBlocksTempGeometry<T> {
    
    out: Box<T>,
    groups: Arc<QuadtreeTree>,
    tm: f64
}
    
impl<T> SortBlocksTempGeometry<T>
    where T: CallFinish<CallType=Vec<(i64, Vec<u8>)>, ReturnType=Timings>
{    
    pub fn new(out: Box<T>, groups: Arc<QuadtreeTree>) -> SortBlocksTempGeometry<T> {
        SortBlocksTempGeometry{out: out, groups: groups, tm:0.0}
    }
    
}

impl<T> CallFinish for SortBlocksTempGeometry<T>
    where T: CallFinish<CallType=Vec<(i64, Vec<u8>)>, ReturnType=Timings>
{
    type CallType = (i64,Vec<FileBlock>);
    type ReturnType = Timings;
    
    fn call(&mut self, d: (i64,Vec<FileBlock>)) {
        let tx=ThreadTimer::new();
        let res = sort_temp_geoms(d.1, self.groups.clone()).expect("?");
        self.tm += tx.since();
        self.out.call(res);
    }

    fn finish(&mut self) -> Result<Timings> {
        
        let mut tt = self.out.finish()?;
        tt.add("CollectBlocksTempGeometry", self.tm);
        Ok(tt)
    }
}
        


pub(crate) fn write_temp_geometry(outfn: &str, bbox: &Bbox, tempdata: TempData, groups: Arc<QuadtreeTree>, numchan: usize) -> Result<()> {
    let wf = Box::new(WrapWriteFile(WriteFile::with_bbox(
            &outfn,
            HeaderType::ExternalLocs,
            Some(&bbox),
        )));

    let cq: Box<dyn CallFinish< CallType=(i64,Vec<FileBlock>), ReturnType=Timings>> = if numchan == 0 {
        Box::new(SortBlocksTempGeometry::new(wf, groups))
    } else {
        let wfs = CallbackSync::new(wf, numchan);
        let mut cqs: Vec<
            Box<dyn CallFinish<CallType = (i64, Vec<FileBlock>), ReturnType = Timings>>,
        > = Vec::new();
        for w in wfs {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            
            cqs.push(Box::new(Callback::new(Box::new(SortBlocksTempGeometry::new(
                w2,
                groups.clone()
            )))));
        }
        Box::new(CallbackMerge::new(cqs, Box::new(MergeTimings::new())))
    };
    
    let t = read_temp_data(tempdata, cq, true)?;
    
    message!("{}", t);
    Ok(())
    
}
