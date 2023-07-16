


use std::sync::atomic::{AtomicUsize,Ordering};
use std::io::{Error,ErrorKind};


static mut MESSENGER: &dyn Messenger = &NopMessenger;

static STATE: AtomicUsize = AtomicUsize::new(0);

pub fn set_messenger(logger: &'static dyn Messenger) -> std::io::Result<()> {
    set_messenger_inner(|| logger)
}

pub fn set_boxed_messenger(logger: Box<dyn Messenger>) -> std::io::Result<()> {
    set_messenger_inner(|| Box::leak(logger))
}


fn set_messenger_error() -> std::io::Result<()> {
    Err(Error::new(
        ErrorKind::Other,
        format!("failed to set messager"),
    ))
}


fn set_messenger_inner<F>(make_logger: F) -> std::io::Result<()>
where
    F: FnOnce() -> &'static dyn Messenger,
{
    let old_state = match STATE.compare_exchange(
        0,
        1,
        Ordering::SeqCst,
        Ordering::SeqCst,
    ) {
        Ok(s) | Err(s) => s,
    };
    match old_state {
        0 => {
            unsafe {
                MESSENGER = make_logger();
            }
            STATE.store(2, Ordering::SeqCst);
            Ok(())
        }
        1 => {
            while STATE.load(Ordering::SeqCst) == 1 {
                //std::sync::atomic::spin_loop_hint();
                std::hint::spin_loop();
            }
            set_messenger_error()
        }
        2 => {
            unsafe {
                MESSENGER = make_logger();
            }
            STATE.store(2, Ordering::SeqCst);
            Ok(())
        }
        _ => set_messenger_error(),
    }
}
pub fn messenger() -> &'static dyn Messenger {
    if STATE.load(Ordering::SeqCst) != 2 {
        static NOP: NopMessenger = NopMessenger;
        &NOP
    } else {
        unsafe { MESSENGER }
    }
}

pub trait ProgressBytes {
    fn change_message(&self, new_message: &str);
    fn progress_bytes(&self, bytes: u64);
    fn finish(&self);
}

pub trait ProgressPercent {
    fn change_message(&self, new_message: &str);
    fn progress_percent(&self, percent: f64);
    fn finish(&self);
}

pub trait TaskSequence {
    fn start_task(&self, message: &str);
    fn finish(&self);
}

pub struct ProgressPercentPartial<'a, T: ProgressPercent + ?Sized> {
    inner: &'a Box<T>,
    start: f64,
    end: f64
    
}

impl<'a, T> ProgressPercent for ProgressPercentPartial<'a, T> 
    where T: ProgressPercent + ?Sized {
        
    fn change_message(&self, new_message: &str) { self.inner.change_message(new_message); }
    fn progress_percent(&self, percent: f64) {
        self.inner.progress_percent(percent * (self.end-self.start) + self.start);
    }
    fn finish(&self) {}
    
}

impl<'a, T> ProgressPercentPartial<'a, T>
    where T: ProgressPercent + ?Sized {

    pub fn new(inner: &'a Box<T>, start: f64, end: f64) -> ProgressPercentPartial<'a, T> {
        ProgressPercentPartial{inner, start, end}
    }
}


pub trait Messenger {
    fn message(&self, message: &str);
    
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes>;
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent>;
    
    fn start_task_sequence(&self, message: &str, num_tasks: usize) -> Box<dyn TaskSequence>;
    
}

struct NopProgressBytes;

impl ProgressBytes for NopProgressBytes {
    fn change_message(&self, _new_message: &str) {}
    fn progress_bytes(&self, _bytes: u64) {}
    fn finish(&self) {}
}

struct NopProgressPercent;
impl ProgressPercent for NopProgressPercent {
    fn change_message(&self, _new_message: &str) {}
    fn progress_percent(&self, _percent: f64) {}
    fn finish(&self) {}
}

struct NopTaskSequence;
impl TaskSequence for NopTaskSequence {
    fn start_task(&self, _message: &str) {}
    fn finish(&self) {}
}


    


struct NopMessenger;
impl Messenger for NopMessenger {
    fn message(&self, _message: &str) {}
    
    fn start_progress_bytes(&self, _message: &str, _total_bytes: u64) -> Box<dyn ProgressBytes> {
        Box::new(NopProgressBytes)
    }
    
    fn start_progress_percent(&self, _message: &str)  -> Box<dyn ProgressPercent> {
        Box::new(NopProgressPercent)
    }
    
    fn start_task_sequence(&self, _message: &str, _num_tasks: usize) -> Box<dyn TaskSequence> {
        Box::new(NopTaskSequence)
    }
    
}


#[macro_export]
macro_rules! message {
    ($($arg:tt)+) => ({
        $crate::logging::messenger().message(
                &format!($($arg)+),
            );
        
    });
    
}

#[macro_export]
macro_rules! progress_bytes {
    ($msg:expr, $total_bytes:expr) => ({
        $crate::logging::messenger().start_progress_bytes($msg, $total_bytes)
        
    });
    
}

#[macro_export]
macro_rules! progress_percent {
    ($msg:expr) => ({
        $crate::logging::messenger().start_progress_percent($msg)
        
    });
    
}

#[macro_export]
macro_rules! task_sequence {
    ($message:expr, $num_tasks:expr) => ({
        $crate::logging::messenger().start_task_sequence($message, $num_tasks)
        
    });
    
}
    
