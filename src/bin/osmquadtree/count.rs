extern crate osmquadtree;
//extern crate cpuprofiler;

use std::fs::File;


use osmquadtree::read_file_block::{FileBlock,read_all_blocks_parallel_prog,ProgBarWrap,file_length,read_all_blocks_prog_fpos};

use osmquadtree::count::{CountBlocks,Count,CountChange};

use osmquadtree::elements::{PrimitiveBlock,MinimalBlock,Bbox};

use osmquadtree::update::{read_xml_change,get_file_locs};


use osmquadtree::callback::{Callback,CallbackMerge,CallFinish};
use osmquadtree::utils::{ThreadTimer,MergeTimings};
use osmquadtree::convertblocks::{make_convert_minimal_block,make_convert_primitive_block};

use std::io::{Error,ErrorKind,Result};
use std::io::BufReader;

//use cpuprofiler::PROFILER;


struct CountChangeMinimal {
    cc: Option<CountChange>,
    tm: f64
}

impl CountChangeMinimal {
    pub fn new() -> CountChangeMinimal {
        CountChangeMinimal{cc: Some(CountChange::new()), tm: 0.0}
    }
}



impl CallFinish for CountChangeMinimal {
    type CallType = MinimalBlock;
    type ReturnType = osmquadtree::utils::Timings::<CountChange>;
    
    fn call(&mut self, bl: MinimalBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Self::ReturnType> {
        let mut tm = osmquadtree::utils::Timings::<CountChange>::new();
        tm.add("countchange", self.tm);
        tm.add_other("countchange", self.cc.take().unwrap());
        Ok(tm)
    }
}
  



struct CountPrim {
    cc: Option<Count>,
    tm: f64
}

impl CountPrim {
    pub fn new() -> CountPrim {
        CountPrim{cc: Some(Count::new()), tm: 0.0}
    }
}

type Timings = osmquadtree::utils::Timings<Count>;

impl CallFinish for CountPrim {
    type CallType = PrimitiveBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: PrimitiveBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_primitive(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Timings> {
        let mut tm = Timings::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}
  
  
struct CountMinimal {
    cc: Option<Count>,
    tm: f64
}

impl CountMinimal {
    pub fn new() -> CountMinimal {
        CountMinimal{cc: Some(Count::new()), tm: 0.0}
    }
}


impl CallFinish for CountMinimal {
    type CallType = MinimalBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, bl: MinimalBlock) {
        
        let tx=ThreadTimer::new();
        self.cc.as_mut().unwrap().add_minimal(&bl);
        self.tm += tx.since();
        
    }
    
    fn finish(&mut self) -> std::io::Result<Timings> {
        let mut tm = Timings::new();
        tm.add("count", self.tm);
        tm.add_other("count", self.cc.take().unwrap());
        Ok(tm)
    }
}



fn parse_bbox(fstr: &str) -> Result<Bbox> {
    
    let vv:Vec<&str> = fstr.split(",").collect();
    if vv.len()!=4 {
        return Err(Error::new(ErrorKind::Other,"expected four vals"));
    }
    let mut vvi = Vec::new();
    for v in vv {
        vvi.push(v.parse().unwrap());
    }
    Ok(Bbox::new(vvi[0],vvi[1],vvi[2],vvi[3]))
}
    
    

/*
fn main() {
    
    
    
    let args: Vec<String> = env::args().collect();
    let mut fname = String::from("test.pbf");
    if args.len()>1 {
        fname = args[1].clone();
    }
        
    
    let mut prof = String::from("");
    let mut primitive = false;
    let mut numchan = 4;
    let mut filter: Option<Bbox> = None;
    
    if args.len()>2 {
        for i in 2..args.len() {
            if args[i].starts_with("prof=") {
                prof = args[i].substr(5,args[i].len());
            } else if args[i] == "primitive" {
                primitive=true;
            } else if args[i].starts_with("numchan=") {
                numchan = args[i].substr(8,args[i].len()).parse().unwrap();
            } else if args[i].starts_with("filter=") {
                filter = Some(parse_bbox(&args[i].substr(7,args[i].len())).expect("failed to read filter"));
            }
        }
    }
     */
    

pub fn run_count(fname: &str, use_primitive: bool, numchan: usize, filter_in: Option<&str>) -> Result<()> {
    
    /*if prof.len()>0 {
        PROFILER.lock().unwrap().start(prof.clone()).expect("couldn't start");
    }*/
    let filter = match filter_in {
        None => None,
        Some(s) => Some(parse_bbox(s)?)
    };
    
    let f = File::open(fname).expect("file not present");
    
    
    if fname.ends_with(".osc") {
        let mut cn = CountChange::new();
        let mut fbuf = BufReader::with_capacity(1024*1024, f);
        let data = read_xml_change(&mut fbuf).expect("failed to read osc");
        
        cn.add_changeblock(&data);
        println!("{}", cn);
    } else if fname.ends_with(".osc.gz") {
        let mut cn = CountChange::new();
        let fbuf = BufReader::with_capacity(1024*1024, f);
        let mut gzbuf = BufReader::new(flate2::bufread::GzDecoder::new(fbuf));
        //Box::new(gzbuf) as Box<dyn std::io::BufRead>
        let data = read_xml_change(&mut gzbuf).expect("failed to read osc");
        
        cn.add_changeblock(&data);
        println!("{}", cn);
    } else if fname.ends_with(".pbfc") {
        let pb = ProgBarWrap::new_filebytes(file_length(fname));
        pb.set_message(&format!("count change blocks minimal {}, numchan=1", fname));
        
        let mut fbuf = BufReader::new(f);
        let cc = Box::new(CountChangeMinimal::new());
        let cn = Box::new(Callback::new(make_convert_minimal_block(true,cc)));
        let (mut a,_) = read_all_blocks_prog_fpos(&mut fbuf, cn, &pb);
        pb.finish();
        let cn = std::mem::take(&mut a.others).pop().unwrap().1;
        
        println!("{}", cn);
        //println!("{:?}", cn.relation.get(&Changetype::Create));
        
    } else if std::fs::metadata(fname).expect("failed to open file").is_file() {
        let mut cc = Count::new();
        
        let pb = ProgBarWrap::new_filebytes(file_length(fname));
        
        if numchan == 0 {
            let mut fbuf=BufReader::new(f);
            
            let (a,_) = if use_primitive {
                pb.set_message(&format!("count blocks primitive {}, numchan=0", fname));
                let cm = Box::new(CountPrim::new());
                let cc = make_convert_primitive_block(false,cm);
                read_all_blocks_prog_fpos(&mut fbuf, cc, &pb)
            } else {
                pb.set_message(&format!("count blocks minimal {}, numchan=0", fname));
                let cm = Box::new(CountMinimal::new());
                let cc = make_convert_minimal_block(false,cm);
                read_all_blocks_prog_fpos(&mut fbuf, cc, &pb)
            };
            pb.finish();
            cc.add_other(&a.others[0].1);
            
            //cc = count_all(cc, read_file_block::ReadFileBlocks::new(&mut fbuf), 0, fname, minimal, false);
        
        } else if numchan > 8 {
            return Err(Error::new(ErrorKind::Other,"numchan must be between 0 and 8"));
        } else {
            
            let mut fbuf=f;
            
            let mut ccs: Vec<Box<dyn CallFinish<CallType=(usize,FileBlock),ReturnType=Timings>>> = Vec::new();
            for _ in  0..numchan {
                if use_primitive {
                    pb.set_message(&format!("count blocks primitive {}, numchan={}", fname,numchan));
                    let cm = Box::new(CountPrim::new());
                    ccs.push(Box::new(Callback::new(make_convert_primitive_block(false,cm))));
                } else {
                    pb.set_message(&format!("count blocks minimal {}, numchan={}", fname,numchan));
                    let cm = Box::new(CountMinimal::new());
                    ccs.push(Box::new(Callback::new(make_convert_minimal_block(false,cm))));
                }
            }
            let cm = Box::new(CallbackMerge::new(ccs, Box::new(MergeTimings::new())));
            let (a,_)  = read_all_blocks_prog_fpos(&mut fbuf, cm, &pb);
            pb.finish();
            for (_,x) in a.others {
                cc.add_other(&x);
            }
        
        }
        println!("{}", cc);
    } else {
        
        
        let (fbufs, locsv) = get_file_locs(fname, filter).expect("?");
        
        
        let pb = ProgBarWrap::new(100);
        
        
        
        let mut pps: Vec<Box<dyn CallFinish<CallType=(usize,Vec<FileBlock>),ReturnType=Timings>>> = Vec::new();
        
        if use_primitive { 
            pb.set_message(&format!("count blocks combine primitive {}, numchan={}", fname,numchan));
            for _ in 0..numchan {
                let cca = Box::new(CountPrim::new());
                pps.push(Box::new(Callback::new(osmquadtree::convertblocks::make_read_primitive_blocks_combine_call_all(cca))));
            }
        } else {
            pb.set_message(&format!("count blocks combine minimal {}, numchan={}", fname,numchan));
            for _ in 0..numchan {
                let cca = Box::new(CountMinimal::new());
                pps.push(Box::new(Callback::new(osmquadtree::convertblocks::make_read_minimal_blocks_combine_call_all(cca))));
            }
        }
        let readb = Box::new(CallbackMerge::new(pps, Box::new(MergeTimings::new())));
        let (a,_b) = read_all_blocks_parallel_prog(fbufs, locsv, readb, &pb);
        pb.finish();
        
        let mut cc = Count::new();
        for (_,y) in &a.others {
            cc.add_other(y);
        }
        
        println!("{}", cc);
        
    }
    Ok(())
    /*if prof.len()>0 {
        PROFILER.lock().unwrap().stop().expect("couldn't stop");
    }*/
    
    
    
    
}
