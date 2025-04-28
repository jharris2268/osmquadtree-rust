use osmquadtree::logging::{Messenger, ProgressPercent,ProgressBytes,TaskSequence, set_boxed_messenger};

use std::cell::RefCell;
use rand::Rng;
use std::time::{SystemTime};

fn elapsed(start: &SystemTime) -> f64 {
     match start.elapsed() {
        Ok(dur) => dur.as_secs_f64(),
        Err(_) => 0.0
    }
}
fn eta_bytes(elapsed: f64, bytes: u64, total_bytes: u64) -> f64 {
    (total_bytes - bytes) as f64 / bytes as f64 * elapsed
}
fn eta_percent(elapsed: f64, percent: f64) -> f64 {
    (100.0 - percent) / percent * elapsed
}

fn time_str(secs_in: f64) -> String {
    let hours = f64::floor(secs_in / 3600.0);
    let mins = f64::floor( (secs_in - hours*60.0) / 60.0);
    let secs = secs_in - hours*3600.0 - mins*60.0;
    
    if hours==0.0 {
        if mins==0.0 {
            return format!("       {:02.1}s", secs);
        } else {
            return format!("    {:02.0}:{:02.1}s", mins, secs);
        }
    }
    return format!("{:-3.0}:{:02.0}:{:02.1}s", hours, mins, secs);
}

fn bytes_str(bytes: u64) -> String {
    if bytes < 1500 {
        return format!("{} bytes", bytes);
    }
    if bytes < 1500*1024 {
        return format!("{:.1} kb", (bytes as f64)/1024.0);
    }
    if bytes < 1500*1024*1024 {
        return format!("{:.1} mb", (bytes as f64)/1024.0/1024.0);
    }
    return format!("{:.1} gb", (bytes as f64)/1024.0/1024.0/1024.0);
}

struct KlaskProgressBytes {
    id: String,
    message: RefCell<String>,
    total_bytes: u64,
    start_time: SystemTime
}
impl KlaskProgressBytes {
    fn new(message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        let mut rng = rand::rng();
        let id = format!("KlaskProgressBytes_{}", rng.random::<u64>());
        let start_time=SystemTime::now();
        Box::new(KlaskProgressBytes{
            id: id,
            message: RefCell::new(String::from(message)),
            total_bytes: total_bytes,
            start_time: start_time
        })
    }
}

impl ProgressBytes for KlaskProgressBytes {
    fn change_message(&self, new_message: &str) {
        self.message.replace(String::from(new_message));
    }
    fn progress_bytes(&self, bytes: u64) {
        let elapsed= elapsed(&self.start_time);
        let eta = eta_bytes(elapsed, bytes, self.total_bytes);
        let msg = format!("[{}: {} remaining] {} {} / {}", 
            time_str(elapsed), time_str(eta),
            self.message.borrow(),
            bytes_str(bytes), bytes_str(self.total_bytes));
        
        klask::output::progress_bar_with_id(
            &self.id,
            &msg,
            bytes as f32 / self.total_bytes as f32);
    }
    fn finish(&self) {}
}

struct KlaskProgressPercent {
    id: String,
    message: RefCell<String>,
    start_time: SystemTime

}
impl KlaskProgressPercent {
    fn new(message: &str) -> Box<dyn ProgressPercent> {
        let mut rng = rand::rng();
        let id = format!("KlaskProgressPercent_{}", rng.random::<u64>());
        let start_time=SystemTime::now();
        Box::new(KlaskProgressPercent{
            id: id,
            message: RefCell::new(String::from(message)),
            start_time: start_time
        })
    }
}
impl ProgressPercent for KlaskProgressPercent {
    fn change_message(&self, new_message: &str) {
        self.message.replace(String::from(new_message));
    }
    fn progress_percent(&self, percent: f64) {
        let elapsed= elapsed(&self.start_time);
        let eta = eta_percent(elapsed, percent);
        let msg = format!("[{}: {} remaining] {} {:5.1}%", 
            time_str(elapsed), time_str(eta),
            self.message.borrow(),
            percent);       
        
        klask::output::progress_bar_with_id(
            &self.id,
            &msg,
            percent as f32 / 100.0);
    }
    fn finish(&self) {}
}

struct KlaskTaskSequence;
impl TaskSequence for KlaskTaskSequence {
    fn start_task(&self, _message: &str) {}
    fn finish(&self) {}
}






pub struct KlaskMessenger;
    
impl KlaskMessenger {
    
    pub fn new() -> KlaskMessenger {
        println!("create KlaskMessenger");
        KlaskMessenger
    }
    
    
}

impl Messenger for KlaskMessenger {
    
    
    fn message(&self, message: &str) {
        println!("{}", message);
        
    }
    
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent> {
        println!("KlaskMessenger::start_progress_percent({})", message);
        KlaskProgressPercent::new(message)
    }
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        println!("KlaskMessenger::start_progress_bytes({},{})",message,total_bytes);
        KlaskProgressBytes::new(message, total_bytes)
    }
    
    fn start_task_sequence(&self, _message: &str, _num_tasks: usize) -> Box<dyn TaskSequence> {
        Box::new(KlaskTaskSequence{})
    }        
        
}

pub fn is_klask_child_app() -> bool {
    std::env::var("KLASK_CHILD_APP").is_ok()
}

pub fn register_messenger_klask() -> std::io::Result<()> {
    let messenger = Box::new(KlaskMessenger::new());
    set_boxed_messenger(messenger)
}

