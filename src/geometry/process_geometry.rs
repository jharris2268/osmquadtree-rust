use crate::geometry::{WorkingBlock,Timings,CollectWayNodes,OtherData,GeometryStyle};
use crate::geometry::multipolygons::ProcessMultiPolygons;
use crate::utils::{LogTimes,MergeTimings,ReplaceNoneWithTimings,parse_timestamp};
use crate::callback::{Callback,CallbackMerge,CallbackSync,CallFinish};
use crate::update::get_file_locs;
use crate::pbfformat::read_file_block::{ProgBarWrap,read_all_blocks_parallel_prog,FileBlock};
use crate::pbfformat::convertblocks::make_read_primitive_blocks_combine_call_all;
use crate::mergechanges::inmem::read_filter;
use std::io::Result;
use std::sync::Arc;

struct CollectWorkingTiles {
    
    nn: usize,
    nw: usize,
    nl: usize,
    nr: usize
}

impl CollectWorkingTiles {
    pub fn new() -> CollectWorkingTiles {
        CollectWorkingTiles{nn:0,nw:0,nl:0,nr:0}
    }
}

impl CallFinish for CollectWorkingTiles {
    type CallType = WorkingBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, wb: WorkingBlock) {
        self.nn += wb.pending_nodes.len();
        self.nw += wb.pending_ways.len();
        self.nl += wb.pending_ways.iter().map(|(_,r,_)| { r.len() }).sum::<usize>(); 
        self.nr += wb.pending_relations.len();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tms = Timings::new();
        let m = format!("have {}, and {} ways (with {} locations), {} relations", self.nn,self.nw, self.nl,self.nr);
        tms.add_other("CollectWorkingTiles", OtherData::Messages(vec![m]));
        Ok(tms)
    }
}
    
        


pub fn process_geometry(prfx: &str, filter: Option<&str>, timestamp: Option<&str>, numchan: usize) -> Result<()> {


    let mut tx=LogTimes::new();
    let (bbox, poly) = read_filter(filter)?;
    
    println!("bbox={}, poly={:?}", bbox, poly);
    
    tx.add("read filter");
    let timestamp = match timestamp {
        None => None,
        Some(ts) => Some(parse_timestamp(ts)?)
    };
    
    let mut pfilelocs = get_file_locs(prfx, Some(bbox.clone()), timestamp)?;
    tx.add("get_file_locs");

    let style = Arc::new(GeometryStyle::default());

    let cf = Box::new(CollectWorkingTiles::new());
    
    let pp: Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>> = if numchan == 0 {
        let mm = Box::new(ProcessMultiPolygons::new(style.clone(), cf));
        let ww = Box::new(CollectWayNodes::new(mm, style.clone()));
        make_read_primitive_blocks_combine_call_all(ww)
    } else {
        let cfb = Box::new(Callback::new(cf));
        
        let mm = Box::new(Callback::new(Box::new(ProcessMultiPolygons::new(style.clone(), cfb))));
        
        let ww = CallbackSync::new(Box::new(CollectWayNodes::new(mm,style.clone())), numchan);
        
        let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        for w in ww {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            pps.push(Box::new(Callback::new(make_read_primitive_blocks_combine_call_all(w2))))
        }
        Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())))
    };
        
    let mut prog = ProgBarWrap::new(100);
    prog.set_range(100);
    prog.set_message(&format!("process_geometry, numchan={}", numchan));
       
    let (tm,_) = read_all_blocks_parallel_prog(&mut pfilelocs.0, &pfilelocs.1, pp, &prog);
    prog.finish();
    tx.add("process_geometry");
    
    println!("{}\n{}", tm,tx);
    
    for (w,x) in tm.others {
        match x {
            OtherData::Messages(mm) => {
                for m in mm {
                    println!("{}: {}", w, m);
                }
            },
            OtherData::Errors(ee) => {
                println!("{}: {} errors", w, ee.len());
            }
        }
    }
    
    Ok(())

}
