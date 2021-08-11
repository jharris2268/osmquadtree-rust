


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
                std::sync::atomic::spin_loop_hint();
            }
            set_messenger_error()
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


pub trait Messenger {
    fn message(&self, message: &str);
    
    fn start_progress_bytes(&self, message: &str, total_bytes: u64);
    fn progress_bytes(&self, bytes: u64);
    fn finish_progress_bytes(&self);
    
    
    fn start_progress_percent(&self, message: &str);
    fn progress_percent(&self, percent: f64);
    fn finish_progress_percent(&self);
}


struct NopMessenger;
impl Messenger for NopMessenger {
    fn message(&self, _message: &str) {}
    
    fn start_progress_bytes(&self, _message: &str, _total_bytes: u64) {}
    fn progress_bytes(&self, _bytes: u64) {}
    fn finish_progress_bytes(&self) {}
    
    
    fn start_progress_percent(&self, _message: &str) {}
    fn progress_percent(&self, _percent: f64) {}
    fn finish_progress_percent(&self) {}
}



    
