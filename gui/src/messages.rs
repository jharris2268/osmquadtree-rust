use osmquadtree::logging::{Messenger, ProgressPercent,ProgressBytes,TaskSequence, set_boxed_messenger};
//use gtk::glib;

//use std::sync::{Arc,Mutex, mpsc};

const MESSAGE_INTEVAL: f64 = 0.1;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
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

struct GuiProgressBytes {
    sender: async_channel::Sender<Message>, //Arc<Mutex<mpsc::SyncSender<Message>>>,
    cancel_set: Arc<AtomicBool>,
    key: String,
    start_time: SystemTime,
    last_message: RefCell<Option<f64>>,
    total_bytes: u64
}
impl GuiProgressBytes {
    fn new(sender: async_channel::Sender<Message>, cancel_set: Arc<AtomicBool>, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        let mut rng = rand::rng();
        let key = format!("ProgressBytes_{}", rng.random::<u64>());
        send_message(sender.clone(), cancel_set.clone(), Message::StartProgressBytes(key.clone(), message.into(), total_bytes));
        let start_time = SystemTime::now();
        let last_message = RefCell::new(None);
        
        Box::new(GuiProgressBytes{sender, cancel_set, key, start_time, last_message, total_bytes})
    }
}

impl ProgressBytes for GuiProgressBytes {
    fn change_message(&self, new_message: &str) {
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::ChangeProgressBytes(self.key.clone(), new_message.into()));
    }
    fn progress_bytes(&self, bytes: u64) {
        let t = elapsed(&self.start_time);
        if let Some(lt) = *self.last_message.borrow() {
            if (bytes < self.total_bytes) && (t - lt < MESSAGE_INTEVAL) {
                return;
            }
        }
        let eta = eta_bytes(t, bytes, self.total_bytes);
        let tail = format!("{} {}: {} remaining", time_str(t), bytes_str(bytes), time_str(eta));
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::UpdateProgressBytes(self.key.clone(), bytes, tail));
        self.last_message.replace(Some(t));
    }
    fn finish(&self) {
        let t = elapsed(&self.start_time);
        let tail = format!("{} {}: {} remaining", time_str(t), bytes_str(self.total_bytes), time_str(0.0));
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::FinishProgressBytes(self.key.clone(), tail));
    }
}

struct GuiProgressPercent {
    sender: async_channel::Sender<Message>,
    cancel_set: Arc<AtomicBool>,
    key: String,
    start_time: SystemTime,
    last_message: RefCell<Option<f64>>

}
impl GuiProgressPercent {
    fn new(sender: async_channel::Sender<Message>, cancel_set: Arc<AtomicBool>,message: &str) -> Box<dyn ProgressPercent> {
        let mut rng = rand::rng();
        let key = format!("ProgressBytes_{}", rng.random::<u64>());
        send_message(sender.clone(), cancel_set.clone(), Message::StartProgressPercent(key.clone(), message.into()));
        
        let start_time = SystemTime::now();
        let last_message = RefCell::new(None);
        
        Box::new(GuiProgressPercent{sender, cancel_set, key, start_time, last_message})
    }
}
impl ProgressPercent for GuiProgressPercent {
    fn change_message(&self, new_message: &str) {
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::ChangeProgressPercent(self.key.clone(), new_message.into()));
    }
    fn progress_percent(&self, percent: f64) {
        let t = elapsed(&self.start_time);
        if let Some(lt) = *self.last_message.borrow() {
            if (percent < 100.0) && (t - lt < MESSAGE_INTEVAL) {
                return;
            }
        }
        let eta = eta_percent(t, percent);
        let tail = format!("{} {:4.1}%: {} remaining", time_str(t), percent, time_str(eta));
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::UpdateProgressPercent(self.key.clone(), percent, tail));
        self.last_message.replace(Some(t));
    }
    fn finish(&self) {
        let t = elapsed(&self.start_time);
        let tail = format!("{} {:4.1}%: {} remaining", time_str(t), 100.0, time_str(0.0));
        send_message(self.sender.clone(), self.cancel_set.clone(), Message::FinishProgressPercent(self.key.clone(), tail));
    }
}

struct GuiTaskSequence;
impl TaskSequence for GuiTaskSequence {
    fn start_task(&self, _message: &str) {}
    fn finish(&self) {}
}

#[derive(Debug, Clone)]
pub enum Message {
    Message(String),
    StartProgressBytes(String,String, u64),
    ChangeProgressBytes(String,String),
    UpdateProgressBytes(String,u64,String),
    FinishProgressBytes(String,String),
    
    StartProgressPercent(String,String),
    ChangeProgressPercent(String,String),
    UpdateProgressPercent(String,f64,String),
    FinishProgressPercent(String,String),
}



pub struct GuiMessenger {
    send: async_channel::Sender<Message>,
    cancel_set: Arc<AtomicBool>,
}
    
impl GuiMessenger {
    
    pub fn new(send: async_channel::Sender<Message>, cancel_set: Arc<AtomicBool>) -> GuiMessenger {
        GuiMessenger{send, cancel_set}
    }
    
    
}

fn send_message(sender: async_channel::Sender<Message>, cancel_set: Arc<AtomicBool>, msg: Message) {
    
    if cancel_set.load(Ordering::Relaxed) {
        panic!("cancel set..");
    }
    
    match sender.force_send(msg.clone()) {
        Ok(None) => {},
        Ok(Some(x)) => { println!("dropped message {:?}", x); },
        Err(e) => {
            println!("failed to send message {:?} {:?}", msg, e);
        }
    };
    
}
        

impl Messenger for GuiMessenger {
    
    
    fn message(&self, message: &str) {
        send_message(self.send.clone(), self.cancel_set.clone(), Message::Message(message.into()));
    }
    
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent> {
        GuiProgressPercent::new(self.send.clone(), self.cancel_set.clone(), message)
    }
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        
        GuiProgressBytes::new(self.send.clone(), self.cancel_set.clone(), message, total_bytes)
    }
    
    fn start_task_sequence(&self, _message: &str, _num_tasks: usize) -> Box<dyn TaskSequence> {
        Box::new(GuiTaskSequence{})
    }        
        
}


pub fn register_messenger_gui() -> std::io::Result<(async_channel::Receiver<Message>, Arc<AtomicBool>)> {
    let (send, recv) = async_channel::bounded(25);
    //let (send_cancel, recv_cancel) = async_channel::unbounded();
    let cancel_set = Arc::new(AtomicBool::new(false));
    /*let cancel_set_clone = cancel_set.clone();
    glib::spawn_future_local(
        async move {
            
            //for message in receiver.iter() {
            while let Ok(message) = recv_cancel.recv().await {
                
                cancel_set_clone.store(message, Ordering::Relaxed);
            }
            
            
            
        }
    );*/    
    
    let messenger = Box::new(GuiMessenger::new(send, cancel_set.clone()));
    set_boxed_messenger(messenger)?;
    Ok((recv, cancel_set))
}

