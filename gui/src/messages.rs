use osmquadtree::logging::{Messenger, ProgressPercent,ProgressBytes,TaskSequence, set_boxed_messenger};

//use std::sync::{Arc,Mutex, mpsc};



//use std::cell::RefCell;
use rand::Rng;
/*
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
*/
struct GuiProgressBytes {
    sender: async_channel::Sender<Message>, //Arc<Mutex<mpsc::SyncSender<Message>>>,
    key: String
}
impl GuiProgressBytes {
    fn new(sender: async_channel::Sender<Message>, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        let mut rng = rand::rng();
        let key = format!("ProgressBytes_{}", rng.random::<u64>());
        send_message(sender.clone(), Message::StartProgressBytes(key.clone(), message.into(), total_bytes));
        Box::new(GuiProgressBytes{sender: sender, key: key})
    }
}

impl ProgressBytes for GuiProgressBytes {
    fn change_message(&self, new_message: &str) {
        send_message(self.sender.clone(), Message::ChangeProgressBytes(self.key.clone(), new_message.into()));
    }
    fn progress_bytes(&self, bytes: u64) {
        
        send_message(self.sender.clone(), Message::UpdateProgressBytes(self.key.clone(), bytes));
    }
    fn finish(&self) {
        send_message(self.sender.clone(), Message::FinishProgressBytes(self.key.clone()));
    }
}

struct GuiProgressPercent {
    sender: async_channel::Sender<Message>,
    key: String

}
impl GuiProgressPercent {
    fn new(sender: async_channel::Sender<Message>, message: &str) -> Box<dyn ProgressPercent> {
        let mut rng = rand::rng();
        let key = format!("ProgressBytes_{}", rng.random::<u64>());
        send_message(sender.clone(), Message::StartProgressPercent(key.clone(), message.into()));
        Box::new(GuiProgressPercent{sender: sender, key: key})
    }
}
impl ProgressPercent for GuiProgressPercent {
    fn change_message(&self, new_message: &str) {
        send_message(self.sender.clone(), Message::ChangeProgressPercent(self.key.clone(), new_message.into()));
    }
    fn progress_percent(&self, percent: f64) {
        
        send_message(self.sender.clone(), Message::UpdateProgressPercent(self.key.clone(), percent));
    }
    fn finish(&self) {
        send_message(self.sender.clone(), Message::FinishProgressPercent(self.key.clone()));
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
    UpdateProgressBytes(String,u64),
    FinishProgressBytes(String),
    
    StartProgressPercent(String,String),
    ChangeProgressPercent(String,String),
    UpdateProgressPercent(String,f64),
    FinishProgressPercent(String),
}



pub struct GuiMessenger {
    send: async_channel::Sender<Message>
}
    
impl GuiMessenger {
    
    pub fn new(send: async_channel::Sender<Message>) -> GuiMessenger {
        GuiMessenger{send: send}
        
    }
    
    
}

fn send_message(sender: async_channel::Sender<Message>, msg: Message) {
    
    match sender.send_blocking(msg.clone()) {
        Ok(_) => {},
        Err(e) => {
            println!("failed to send message {:?} {:?}", msg, e);
        }
    };
    
}
        

impl Messenger for GuiMessenger {
    
    
    fn message(&self, message: &str) {
        send_message(self.send.clone(), Message::Message(message.into()));
    }
    
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent> {
        GuiProgressPercent::new(self.send.clone(), message)
    }
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        
        GuiProgressBytes::new(self.send.clone(), message, total_bytes)
    }
    
    fn start_task_sequence(&self, _message: &str, _num_tasks: usize) -> Box<dyn TaskSequence> {
        Box::new(GuiTaskSequence{})
    }        
        
}


pub fn register_messenger_gui() -> std::io::Result<async_channel::Receiver<Message>> {
    let (send, recv) = async_channel::bounded(1);
    let messenger = Box::new(GuiMessenger::new(send));
    set_boxed_messenger(messenger)?;
    Ok(recv)
}

