use crate::callback::{CallFinish,Callback,CallbackMerge,CallbackSync};
use crate::utils::{ThreadTimer,Timer,ReplaceNoneWithTimings, MergeTimings,CallAll};
use crate::pbfformat::{writefile::WriteFile,header_block::HeaderType,write_pbf::pack_data,write_pbf::pack_value,read_file_block::pack_file_block};
use crate::geometry::postgresql::{PostgresqlConnection,PostgresqlOptions,TableSpec,PrepTable,AllocFunc,pack_geometry_block,prepare_tables};
use crate::geometry::{Timings,OtherData,GeometryBlock};
use crate::geometry::postgresql::Connection;


use serde::Serialize;
use std::fs::File;
use std::collections::BTreeMap;
use std::io::{Result,Write};
use std::sync::{Arc,Mutex};

struct PackedBlob {
    table: usize,
    data: Vec<u8>
}

impl PackedBlob {
    pub fn new(i: usize) -> PackedBlob {
        PackedBlob{table: i, data: Vec::new() }
    }
}

impl Write for PackedBlob {
    fn write(&mut self, d: &[u8]) -> Result<usize> {
        self.data.write(d)
    }
    fn flush(&mut self) -> Result<()> {
        self.data.flush()
    }
}

const PGCOPY_HEADER: &[u8] = &[80, 71, 67, 79, 80, 89, 10, 255, 13, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0];
const PGCOPY_TAIL: &[u8] = &[255, 255];


struct WriteGzipBlob {
    output: flate2::write::GzEncoder<File>
}

impl WriteGzipBlob {
    fn new(fname: &str) -> WriteGzipBlob {
        let f = File::create(fname).expect("!");
        let mut pp = flate2::write::GzEncoder::new(f, flate2::Compression::default());
        pp.write_all(&PGCOPY_HEADER).expect("!");
        
        WriteGzipBlob{output: pp}
    }
}

impl CallFinish for WriteGzipBlob {
    type CallType=Vec<u8>;
    type ReturnType=();
    
    fn call(&mut self, b: Vec<u8>) {
        self.output.write_all(&b).expect("!");
    }
    fn finish(&mut self) -> Result<()> {
        self.output.write_all(&PGCOPY_TAIL)?;
        self.output.try_finish()?;
        Ok(())
    }
}
       
#[derive(Serialize,Debug)]
struct CopySpec<'a> {
    pub tabs: &'a Vec<TableSpec>,
    pub before: Vec<String>,
    pub copy: Vec<String>,
    pub after: Vec<String>
}

impl<'a> CopySpec<'a> {
    pub fn new(tabs: &'a Vec<TableSpec>, extended: bool) -> CopySpec<'a> {
        
        let (before,copy,after) = prepare_tables(None, &tabs, extended).expect("!");
        CopySpec{tabs,before,copy,after}
    }
}
        

struct WritePackedFiles {
    
    outputs: BTreeMap<usize, (usize,Option<Box<dyn CallFinish<CallType=Vec<u8>,ReturnType=()>>>)>,
    tt: f64
}

impl WritePackedFiles {
    pub fn new(prfx: Option<&str>, tabs: &Vec<TableSpec>, septhreads: bool) -> WritePackedFiles {
        let mut outputs = BTreeMap::new();
        
        match prfx {
            Some(prfx) => { serde_json::to_writer(std::fs::File::create(&format!("{}spec.json", prfx)).expect("!"), &CopySpec::new(tabs, tabs.len()>3)).expect("!"); },
            None => {}
        }
        for (i,t) in tabs.iter().enumerate() {
            match prfx {
                Some(prfx) => {
                    let fname = format!("{}{}.copy.gz", prfx, t.name);
                    let x: Box<dyn CallFinish<CallType=Vec<u8>,ReturnType=()>> =
                        if septhreads {
                            Box::new(Callback::new(Box::new(WriteGzipBlob::new(&fname))))
                        } else {
                            Box::new(WriteGzipBlob::new(&fname))
                        };
                    outputs.insert(i, (0, Some(x)));
                },
                None => {
                    outputs.insert(i, (0,None));
                }
            }
        }
        let tt = 0.0;
        WritePackedFiles{outputs, tt}
    }
}

impl CallFinish for WritePackedFiles {
    type CallType = Vec<PackedBlob>;
    type ReturnType = Timings;
    
    fn call(&mut self, pbbs: Vec<PackedBlob>) {
        let tx=ThreadTimer::new();
        for mut pb in pbbs {
            if !pb.data.is_empty() {
                match self.outputs.get_mut(&pb.table) {
                    Some(pp) => { 
                        pp.0 += pb.data.len();
                        match pp.1.as_mut() {
                            None => {},
                            Some(q) => { q.call(std::mem::take(&mut pb.data)); }
                        }
                    },
                    None => { panic!("unknown table"); }
                }
            }
        }
        self.tt += tx.since();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add("WritePackedFiles", self.tt);
        
        let mut ts = Vec::new();
        for (i,(a,mut b)) in std::mem::take(&mut self.outputs) {
            
            ts.push(format!("table {}: {} bytes", i, a));
            match b.as_mut() {
                None => {},
                Some(q) => { q.finish()?; }
            }
            
        }
        tm.add_other("WritePackedFles", OtherData::Messages(ts));
        Ok(tm)
    }
}


struct WritePackedPbfFile {
    out: WriteFile,
}

impl WritePackedPbfFile {
    pub fn new(outfn: &str) -> WritePackedPbfFile {
        let out = WriteFile::new(outfn, HeaderType::None);
        WritePackedPbfFile{out: out}
    }
    
}
impl CallFinish for WritePackedPbfFile {
    type CallType = Vec<(i64,Vec<u8>)>;
    type ReturnType = Timings;
    
    fn call(&mut self, pbbs: Vec<(i64,Vec<u8>)>) {
        
        self.out.call(pbbs);
        
    }
    fn finish(&mut self) -> Result<Timings> {
        let (t,_) = self.out.finish()?;
        
        let mut tm = Timings::new();
        tm.add("WritePackedPbfFile", t);
        
        Ok(tm)
    }
}

fn pack_pbf_blobs(pbbs: Vec<PackedBlob>) -> Vec<(i64,Vec<u8>)> {
    let mut res = Vec::new();
    for p in pbbs {
        if !p.data.is_empty() {
            let mut packed = Vec::with_capacity(p.data.len()+20);
            pack_value(&mut packed, 1, p.table as u64);
            
            let mut d = Vec::with_capacity(21+p.data.len());
            d.write_all(&PGCOPY_HEADER).expect("!");
            d.write_all(&p.data).expect("!");
            d.write_all(&PGCOPY_TAIL).expect("!");
            
            pack_data(&mut packed, 2, &d);
            
            res.push((p.table as i64,pack_file_block("BlobData", &packed, true).expect("!")));
        }
    }
    res
}

fn make_write_packed_pbffile(p: &str, tabs: &Vec<TableSpec>, numchan: usize) -> Box<dyn CallFinish<CallType=Vec<PackedBlob>, ReturnType=Timings>> {
    let mut wf = Box::new(WritePackedPbfFile::new(&p));
    let header = serde_json::to_vec(&CopySpec::new(tabs, tabs.len()>3)).expect("!");
    wf.call(vec![(-1,pack_file_block("BlobHeaderJson", &header, true).expect("!"))]);
    
    if numchan == 0 {
        
        Box::new(CallAll::new(wf, "pack_pbf_blobs", Box::new(pack_pbf_blobs)))
    } else {
        let wfs = CallbackSync::new(wf, numchan);
        let mut packs: Vec<Box<dyn CallFinish<CallType=Vec<PackedBlob>, ReturnType=Timings>>> = Vec::new();
        for w in wfs {
            let w2 = Box::new(ReplaceNoneWithTimings::new(w));
            packs.push(Box::new(Callback::new(Box::new(CallAll::new(w2, "pack_pbf_blobs", Box::new(pack_pbf_blobs))))));
        }
        Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new())))
    }
}



struct WritePostgresData {
    conn: Arc<Mutex<Connection>>, // see altconnection.rs or postgresconnection.rs
    
    copy: Vec<String>,
    exec_after: bool,
    after: Vec<String>,
    tt: f64,
    nc: usize,
    nb: usize
}

impl WritePostgresData {
    
    pub fn new(conn: Connection, copy: Vec<String>, exec_after: bool, after: Vec<String>) -> WritePostgresData {
        WritePostgresData{
                conn: Arc::new(Mutex::new(conn)),
                copy: copy, exec_after: exec_after, after: after,
                tt: 0.0, nc: 0, nb: 0}
    }
    
    pub fn connect(connstr: &str, tableprfx: &str, opts: &PostgresqlOptions, newthread: bool, exec_after: bool) -> Result<Box<dyn CallFinish<CallType=Vec<PackedBlob>,ReturnType=Timings>>> {
        
        let mut conn = Connection::connect(connstr)?;
        
        conn.execute("begin")?;
        let (before,copy,after) = prepare_tables(Some(tableprfx), &opts.table_spec, opts.extended)?;
        
        for (i,qu) in before.iter().enumerate() {
            let tx=Timer::new();
            print!("{} {:100.100} ", i,qu);
            conn.execute(&qu)?;
            println!(": {:.1}s", tx.since());
        }
        let wpg = Box::new(WritePostgresData::new(conn, copy, exec_after, after));
        if newthread {
            Ok(Box::new(Callback::new(wpg)))
        } else {
            Ok(wpg)
        }
    }
    
    fn call_copy(&mut self, pb: &PackedBlob)  {
        
        
        let mut conn = self.conn.lock().unwrap();
        conn.copy(&self.copy[pb.table], &vec![PGCOPY_HEADER,&pb.data,PGCOPY_TAIL]).expect("copy failed");
        
        
        self.nc+=1;
        self.nb += pb.data.len();
        
    }
        
}

impl CallFinish for WritePostgresData  {
    type CallType = Vec<PackedBlob>;
    type ReturnType = Timings;
    
    fn call(&mut self, pbs: Vec<PackedBlob>) {
        
        let tx = ThreadTimer::new();
        for pb in &pbs {
            if !pb.data.is_empty() {
                self.call_copy(pb);
            }
        }
        
        self.tt+=tx.since();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tm = Timings::new();
        tm.add("WritePostgresData::copy", self.tt);
        
        
        
        let mut conn = self.conn.lock().unwrap();
        
        let tx = Timer::new();
        conn.execute("commit").expect("commit failed");
        tm.add("WritePostgresData::commit", tx.since());
        let txx = Timer::new();
        
        
        if self.exec_after {
           for (i,qu) in self.after.iter().enumerate() {
                let tx=Timer::new();
                print!("{} {:100.100} ", i,qu);
                std::io::stdout().flush().unwrap();
                
                match conn.execute(&qu) {
                    Ok(_) => {},
                    Err(e) => { print!(" FAILED {:?}", e); }
                };
                println!(": {:.1}s", tx.since());
            }
            
        } else {
            println!("indices:");
            for a in &self.after {
                println!("{};", a);
            }
        }
        tm.add("WritePostgresData::finish", txx.since());
        
        
        tm.add_other("WritePostgresData", OtherData::Messages(vec![format!("added {} blobs [{} mb]", self.nc, (self.nb as f64)/1024.0/1024.0)]));
        Ok(tm)
    }
}
        
    


    



fn prep_output(opts: &PostgresqlOptions, numchan: usize) -> Result<Box<dyn CallFinish<CallType=Vec<PackedBlob>, ReturnType=Timings>>> {
    match &opts.connection {
        PostgresqlConnection::Connection((connstr, tableprfx,execindices)) => WritePostgresData::connect(connstr, tableprfx, opts, numchan != 0,*execindices),
        PostgresqlConnection::Null => Ok(Box::new(WritePackedFiles::new(None, &opts.table_spec, numchan != 0))),
        PostgresqlConnection::CopyFilePrfx(p) => Ok(Box::new(WritePackedFiles::new(Some(&p), &opts.table_spec, numchan != 0))),
        PostgresqlConnection::CopyFileBlob(p) => Ok(make_write_packed_pbffile(&p, &opts.table_spec, numchan)),
    }
}
                    
struct PackPostgresData<T: ?Sized> {
    out: Box<T>,
    alloc_func: AllocFunc,
    preptables: Arc<Vec<PrepTable>>,
    tt: f64,
    count: usize,
    errs: usize,
}

impl<T> PackPostgresData<T> 
    where T: CallFinish<CallType=Vec<PackedBlob>, ReturnType=Timings> + ?Sized
{
    pub fn new(out: Box<T>, alloc_func: AllocFunc, preptables: Arc<Vec<PrepTable>>) -> PackPostgresData<T> {
        PackPostgresData{out: out, alloc_func: alloc_func, preptables: preptables, tt: 0.0, count: 0, errs: 0}
    }


    fn call_all(&mut self, geoms: &GeometryBlock) -> Vec<PackedBlob> {
        
        let mut blobs = Vec::new();
        //let mut outs = Vec::new();
        for i in 0..self.preptables.len() {
            
            blobs.push(PackedBlob::new(i));
            //outs.push(&blobs[i] as &mut dyn Write);
        }
        let (c,e) = pack_geometry_block(&self.preptables, &mut blobs, &*self.alloc_func, &geoms).expect("!");
        self.count += c;
        self.errs+=e;
        blobs
    }


}

impl<T> CallFinish for PackPostgresData<T>
    where T: CallFinish<CallType=Vec<PackedBlob>, ReturnType=Timings> + ?Sized
{
    type CallType = GeometryBlock;
    type ReturnType = Timings;
    
    fn call(&mut self, geoms: GeometryBlock) {
        let tx= ThreadTimer::new();
        let blobs = self.call_all(&geoms);
        self.out.call(blobs);
        self.tt += tx.since();
    }
    
    fn finish(&mut self) -> Result<Timings> {
        let mut tm = self.out.finish()?;
        tm.add("PackPostgresData", self.tt);
        tm.add_other("PackPostgresData", OtherData::Messages(vec![
            format!("added {} objects", self.count),
            format!("skipped {} errors", self.errs)
        ]));
        Ok(tm)
    }
}


pub fn make_write_postgresql_geometry(options: &PostgresqlOptions, numchan: usize) -> Result<crate::geometry::CallFinishGeometryBlock> {
    
        
    let out = prep_output(options,numchan)?;
    
    let mut preptables = Vec::new();
    for t in &options.table_spec {
        //println!("{}", make_createtable(t, "")?);
        //println!("{:?}", t);
        preptables.push(PrepTable::from_tablespec(t)?);
    }
    
    
    let preptables = Arc::new(preptables);
    if numchan == 0 {
        Ok(Box::new(PackPostgresData::new(out, options.table_alloc.clone(), preptables.clone())))
    } else {
        let outs = CallbackSync::new(out, numchan);
        let mut packs: Vec<crate::geometry::CallFinishGeometryBlock> = Vec::new();
        for o in outs {
            let o2 = Box::new(ReplaceNoneWithTimings::new(o));
            packs.push(Box::new(Callback::new(Box::new(PackPostgresData::new(o2, options.table_alloc.clone(), preptables.clone())))));
        }
        Ok(Box::new(CallbackMerge::new(packs, Box::new(MergeTimings::new()))))
    }
    
}
