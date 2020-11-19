use std::fmt;
use std::collections::HashMap;
use super::callback::{CallFinish,CollectResult};
use std::marker::PhantomData;
use std::io::Result;

fn as_secs(dur: std::time::Duration) -> f64 {
    (dur.as_secs() as f64)*1.0 + (dur.subsec_nanos() as f64)*0.000000001
}

pub struct Timer(std::time::SystemTime);

impl Timer {
    pub fn new() -> Timer {
        Timer(std::time::SystemTime::now())
    }
    
    pub fn since(&self) -> f64 {
        as_secs(self.0.elapsed().unwrap())
    }
    
    pub fn reset(&mut self) {
        self.0=std::time::SystemTime::now();
    }
}

pub struct ThreadTimer(cpu_time::ThreadTime);

impl ThreadTimer {
    pub fn new() -> ThreadTimer {
        ThreadTimer(cpu_time::ThreadTime::now())
    }
    
    pub fn since(&self) -> f64 {
        as_secs(self.0.elapsed())
    }
    
    pub fn reset(&mut self) {
        self.0=cpu_time::ThreadTime::now();
    }
}

pub struct Checktime {        
    //st: std::time::SystemTime,
    //lt: std::time::SystemTime
    st: Timer,
    lt: Timer,
    thres: f64,
}

impl Checktime {
    pub fn new() -> Checktime {
        
        Checktime{st:Timer::new(), lt: Timer::new(), thres: 2.0}
        
        /*let st=std::time::SystemTime::now();
        let lt=std::time::SystemTime::now();
        Checktime{st,lt}        */
    }
    pub fn with_threshold(thres: f64) -> Checktime {
        Checktime{st:Timer::new(), lt: Timer::new(), thres: thres}
    }
    
    pub fn checktime(&mut self) -> Option<f64> {
        let lm = self.lt.since();
        if lm > self.thres {
            self.lt.reset();
            return Some(self.st.since());
        }
        /*
        let lm=as_secs(self.lt.elapsed().unwrap());
        if lm > 2.0 {
            self.lt=std::time::SystemTime::now();
            return Some(self.gettime());
        }*/
        None
    }
    pub fn gettime(&self) -> f64{
        //as_secs(self.st.elapsed().unwrap())
        self.st.since()
    }
}
        


pub struct Timings<T: Sync+Send+'static> { 
    pub timings: HashMap<String,f64>,
    pub others: Vec<(String,T)>,
}

impl<T> Timings<T>
    where T: Sync+Send+'static
{
    pub fn new() -> Timings<T> {
        Timings{timings: HashMap::new(),others: Vec::new() }
    }
    
    pub fn add(&mut self, k: &str, v: f64) {
        self.timings.insert(String::from(k), v);
    }
    pub fn add_other(&mut self, k: &str, v: T) {
        self.others.push((String::from(k),v));
    }
    
    pub fn combine(&mut self, mut other: Self) {
        for (k,v) in other.timings {
            if self.timings.contains_key(&k) {
                *self.timings.get_mut(&k).unwrap() += v;
            } else {
                self.timings.insert(k, v);
            }
        }
        for (a,b) in std::mem::take(&mut other.others) {
            self.others.push((a,b));
        }
    }
    
    
}
impl<T> fmt::Display for Timings<T>
    where T: Sync+Send+'static
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { 
        let mut fs = String::new();
        for (k,v) in &self.timings {
            fs = format!("{} {}:{:0.1}s", fs, k, v);
        }
        write!(f, "Timings: {}", fs)
    }
}

pub struct ConsumeAll<T: Sync+Send+'static,U: Sync+Send+'static>(PhantomData<T>,PhantomData<U>);
impl<T,U> ConsumeAll<T,U>
    where T: Sync+Send+'static,
          U: Sync+Send+'static
{
    pub fn new() -> ConsumeAll<T,U> {
        ConsumeAll(PhantomData,PhantomData)
    }
}

impl<T,U> CallFinish for ConsumeAll<T,U>
    where T: Sync+Send+'static,
          U: Sync+Send+'static
{
    type CallType=T;
    type ReturnType=Timings<U>;
    
    fn call(&mut self, _: T) {}
    
    fn finish(&mut self) -> Result<Timings<U>> {
        Ok(Timings::new())
    }
    
}

pub struct MergeTimings<U: Sync+Send+'static>(PhantomData<U>);

impl<U: Sync+Send+'static> MergeTimings<U> {
    pub fn new() -> MergeTimings<U> {
        MergeTimings(PhantomData)
    }
}

impl<U> CollectResult for MergeTimings<U>
    where U: Sync+Send+'static
{
    type InType = Timings<U>;
    type OutType = Timings<U>;
    
    fn collect(&self, vv: Vec<Self::InType>) -> Self::OutType {
        let mut vv=vv;
        if vv.is_empty() {
            return Timings::new();
        }
        
        let mut r = vv.pop().unwrap();
        if vv.len()>0 {
            for v in vv {
                r.combine(v);
            }
        }
        r
    }
}


pub struct ReplaceNoneWithTimings<T> {
    out: Box<T>,
}
impl<T> ReplaceNoneWithTimings<T> {
    pub fn new(out: Box<T>) -> ReplaceNoneWithTimings<T> {
        ReplaceNoneWithTimings{out}
    }
}

impl<T,U> CallFinish for ReplaceNoneWithTimings<T> 
    where T: CallFinish<ReturnType=Option<Timings<U>>>,
          U: Sync+Send+'static
{
    type CallType = T::CallType;
    type ReturnType = Timings<U>;
    
    fn call(&mut self, c: Self::CallType) {
        self.out.call(c);
    }
    
    fn finish(&mut self) -> Result<Self::ReturnType> {
        let x = self.out.finish()?;
        match x {
            None => Ok(Timings::new()),
            Some(y) => Ok(y)
        }
    }
}


pub struct CallAll<T: CallFinish,U: Sync+Send+'static, W: Fn(U) -> T::CallType, V> {
    out: Box<T>,
    tm: f64,
    msg: String,
    callfunc: Box<W>,
    x: PhantomData<U>,
    y: PhantomData<V>
}

impl<T,U,W,V> CallAll<T,U,W,V>
    where   T: CallFinish<ReturnType=Timings<V>>,
            U: Sync+Send+'static,
            W: Fn(U) -> T::CallType + Sync+Send+'static,
            V: Sync+Send+'static
{
    pub fn new(out: Box<T>, msg: &str, callfunc: Box<W>) -> CallAll<T,U,W,V> {
        
        CallAll{out:out,msg:String::from(msg),tm:0.0,callfunc:callfunc,x:PhantomData,y: PhantomData}
    }
}

impl<T,U,W,V> CallFinish for CallAll<T,U,W,V> 
where   T: CallFinish<ReturnType=Timings<V>>,
        U: Sync+Send+'static,
        W: Fn(U) -> T::CallType + Sync+Send+'static,
        V: Sync+Send+'static
{
    type CallType=U;
    type ReturnType=Timings<V>;
    
    fn call(&mut self, c: U) {
        let tx = ThreadTimer::new();
        let r = (self.callfunc)(c);
        self.tm += tx.since();
        self.out.call(r);
    }
    
    fn finish(&mut self) -> Result<Timings<V>> {
        let mut t = self.out.finish()?;
        t.add(self.msg.as_str(), self.tm);
        Ok(t)
    }
}
