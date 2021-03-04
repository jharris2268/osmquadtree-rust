use std::io::{Error, ErrorKind, Result};
use std::sync::mpsc;
use std::thread;

pub trait CallFinish: Sync + Send + 'static {
    type CallType;
    type ReturnType;
    fn call(&mut self, f: Self::CallType);
    fn finish(&mut self) -> Result<Self::ReturnType>;
}

fn call_all<T: Send + 'static, U: Send + 'static>(
    recv: mpsc::Receiver<T>,
    mut cf: Box<impl CallFinish<CallType = T, ReturnType = U>>,
) -> Result<U> {
    for m in recv.iter() {
        cf.call(m);
    }

    cf.finish()
}

fn call_all_sync<T: Send + 'static, U: Send + 'static>(
    recvs: Vec<mpsc::Receiver<T>>,
    mut cf: Box<impl CallFinish<CallType = T, ReturnType = U> + ?Sized>,
) -> Result<U> {
    let mut i = 0;
    let l = recvs.len();
    let mut nf = 0;
    loop {
        match recvs[i % l].recv() {
            Ok(m) => cf.call(m),

            Err(_) => {
                nf += 1;
                if nf == l {
                    return cf.finish();
                }
            }
        }
        i += 1;
    }
}

pub struct Callback<T, U> {
    send: Option<mpsc::SyncSender<T>>,
    result: Option<thread::JoinHandle<Result<U>>>,
}
impl<T, U> Callback<T, U>
where
    T: Send + 'static,
    U: Send + 'static,
{
    pub fn new(cf: Box<impl CallFinish<CallType = T, ReturnType = U>>) -> Callback<T, U> {
        let (send, recv) = mpsc::sync_channel(1);

        let result = thread::spawn(move || call_all(recv, cf));

        Callback {
            send: Some(send),
            result: Some(result),
        }
    }
}

impl<T, U> CallFinish for Callback<T, U>
where
    T: Send + 'static,
    U: Send + 'static,
{
    type CallType = T;
    type ReturnType = U;
    fn call(&mut self, t: T) {
        match &self.send {
            Some(s) => {
                s.send(t).expect("failed to send");
            }
            _ => {}
        }
    }

    fn finish(&mut self) -> Result<U> {
        self.send = None;

        let r = std::mem::replace(&mut self.result, None);

        match r {
            Some(r) => match r.join() {
                Ok(p) => p,
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to join {:?}", e),
                )),
            },
            None => Err(Error::new(ErrorKind::Other, "already called finish")),
        }
    }
}

static MAXNUMCHAN: usize = 8;

pub struct CallbackSync<T, U> {
    send: Option<mpsc::SyncSender<T>>,
    result: Option<thread::JoinHandle<Result<U>>>,
    expectresult: bool,
    //th: usize
}

impl<T, U> CallbackSync<T, U>
where
    T: Send + 'static,
    U: Send + 'static,
{
    pub fn new(
        cf: Box<impl CallFinish<CallType = T, ReturnType = U> + ?Sized>,
        numchan: usize,
    ) -> Vec<Box<CallbackSync<T, U>>> {
        if numchan == 0 || numchan > MAXNUMCHAN {
            panic!(
                "wrong numchan {}: must between 1 and {}",
                numchan, MAXNUMCHAN
            );
        }
        let mut sends = Vec::new();
        let mut recvs = Vec::new();

        for _ in 0..numchan {
            let (send, recv) = mpsc::sync_channel(1);
            sends.push(send);
            recvs.push(recv);
        }

        let mut res = Vec::new();

        let result = thread::spawn(move || call_all_sync(recvs, cf));
        res.push(Box::new(CallbackSync {
            send: sends.pop(),
            result: Some(result),
            expectresult: true,
        }));

        for _ in 1..numchan {
            res.push(Box::new(CallbackSync {
                send: sends.pop(),
                result: None,
                expectresult: false,
            }));
        }
        res.reverse();
        res
    }
}

impl<T, U> CallFinish for CallbackSync<T, U>
where
    T: Send + 'static,
    U: Send + 'static,
{
    type CallType = T;
    type ReturnType = Option<U>;

    fn call(&mut self, t: T) {
        match &self.send {
            Some(s) => {
                s.send(t).expect("failed to send");
            }
            _ => {}
        }
    }

    fn finish(&mut self) -> Result<Option<U>> {
        self.send = None;

        if !self.expectresult {
            return Ok(None);
        }

        let r = std::mem::replace(&mut self.result, None);

        match r {
            Some(r) => match r.join() {
                Ok(p) => match p {
                    Ok(q) => Ok(Some(q)),

                    Err(e) => Err(e),
                },
                Err(e) => Err(Error::new(
                    ErrorKind::Other,
                    format!("failed to join {:?}", e),
                )),
            },
            None => Err(Error::new(ErrorKind::Other, "already called finish")),
        }
    }
}

pub trait CollectResult: Sync + Send + 'static {
    type InType;
    type OutType;
    fn collect(&self, a: Vec<Self::InType>) -> Self::OutType;
}

pub struct CallbackMerge<T, U, V> {
    callbacks: Vec<Box<dyn CallFinish<CallType = T, ReturnType = U>>>,
    collect: Box<dyn CollectResult<InType = U, OutType = V>>,
    idx: usize,
}

impl<T, U, V> CallbackMerge<T, U, V>
where
    T: Send + 'static,
    U: Send + 'static,
    V: Send + 'static,
{
    pub fn new(
        callbacks: Vec<Box<dyn CallFinish<CallType = T, ReturnType = U>>>,
        collect: Box<dyn CollectResult<InType = U, OutType = V>>,
    ) -> CallbackMerge<T, U, V> {
        CallbackMerge {
            callbacks: callbacks,
            collect: collect,
            idx: 0,
        }
    }
}

impl<T, U, V> CallFinish for CallbackMerge<T, U, V>
where
    T: Send + 'static,
    U: Send + 'static,
    V: Send + 'static,
{
    type CallType = T;
    type ReturnType = V;

    fn call(&mut self, t: T) {
        let l = self.callbacks.len();
        self.callbacks[self.idx % l].call(t);
        self.idx += 1;
    }

    fn finish(&mut self) -> Result<Self::ReturnType> {
        let mut r = Vec::new();
        let mut err: Option<Error> = None;
        for c in self.callbacks.iter_mut() {
            match c.finish() {
                Ok(s) => {
                    r.push(s);
                }
                Err(e) => {
                    err = Some(e);
                }
            }
        }

        match err {
            Some(e) => Err(e),
            None => Ok(self.collect.collect(r)),
        }
    }
}
