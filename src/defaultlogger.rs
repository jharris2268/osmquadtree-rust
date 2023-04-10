
use indicatif::{ProgressBar, ProgressStyle};
use crate::logging::{ProgressBytes,ProgressPercent,TaskSequence, Messenger,set_boxed_messenger};
use crate::message;

use std::sync::{Arc, Mutex, MutexGuard};


pub struct ProgressBytesDefault {
    pb: ProgressBar
}

impl ProgressBytesDefault {
    pub fn new(message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        let pb = ProgressBar::new(total_bytes);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:100.cyan/blue}] {bytes} / {total_bytes} ({eta_precise}) {msg}")
            .progress_chars("#>-"));
        
        pb.set_message(message);
        
        Box::new(ProgressBytesDefault{pb: pb})
    }
}

impl ProgressBytes for ProgressBytesDefault {
    fn change_message(&self, new_message: &str) {
        self.pb.set_message(new_message);
    }
    
    
    fn progress_bytes(&self, bytes: u64) {
        
        self.pb.set_position(bytes);
        
    }
    fn finish(&self) {
        
        self.pb.finish();
        
    }
}

pub struct ProgressPercentDefault {
    pb: ProgressBar
}

impl ProgressPercentDefault {
    pub fn new(message: &str) -> Box<dyn ProgressPercent> {
                
        let pb = ProgressBar::new(1000);
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:100.cyan/blue}] {percent:>4}% ({eta_precise}) {msg}")
            .progress_chars("#>-"));
        pb.set_message(message);
        
        Box::new(ProgressPercentDefault{pb: pb})
    }
}

impl ProgressPercent for ProgressPercentDefault {
    fn change_message(&self, new_message: &str) {
        self.pb.set_message(new_message);
    }
    
    fn progress_percent(&self, percent: f64) {
        
        self.pb.set_position((percent*10.0) as u64);
        
    }
    fn finish(&self) {
        
        self.pb.finish();
        
    }
}
pub struct TaskSequenceState {
    sequence_message: String,
    num_tasks: usize,
    current_task: usize
}

impl TaskSequenceState {
    
    fn new(message: &str, num_tasks: usize) -> Arc<Mutex<TaskSequenceState>> {
        Arc::new(Mutex::new(
            TaskSequenceState{
                sequence_message: String::from(message),
                num_tasks: num_tasks,
                current_task: 0
            }
        ))
    }
    
    fn start_task(&mut self, msg: &str) {
        self.current_task += 1;
        message!("[{} {}/{}] {}", self.sequence_message, self.current_task, self.num_tasks, msg);
    }
    
    fn finish(&mut self) {}
}
       


pub struct TaskSequenceDefault {
    
    state: Arc<Mutex<TaskSequenceState>>
        
}



impl TaskSequenceDefault {
    pub fn new(message: &str, num_tasks: usize) -> Box<dyn TaskSequence> {
        Box::new(TaskSequenceDefault{state: TaskSequenceState::new(message, num_tasks)})
    }
    
    #[inline]
    fn state(&self) -> MutexGuard<'_, TaskSequenceState> {
        self.state.lock().unwrap()
    }
    
}

impl TaskSequence for TaskSequenceDefault {
    fn start_task(&self, msg: &str) {
        self.state().start_task(msg);
    }
    
    fn finish(&self) {
        self.state().finish();
    }
}
        
        
        
        
        

pub struct MessengerDefault;
    
impl MessengerDefault {
    
    pub fn new() -> MessengerDefault {
        
        MessengerDefault
    }
    
    
}

impl Messenger for MessengerDefault {
    
    
    fn message(&self, message: &str) {
        let lns = message.split("\n");
        for (i,l) in lns.enumerate() {
            println!("{} {}", (if i==0 { "MSG:" } else { "    "}), l);
        }
    }
    
    fn start_progress_percent(&self, message: &str) -> Box<dyn ProgressPercent> {
        
        ProgressPercentDefault::new(message)
    }
    fn start_progress_bytes(&self, message: &str, total_bytes: u64) -> Box<dyn ProgressBytes> {
        
        ProgressBytesDefault::new(message, total_bytes)
    }
    
    fn start_task_sequence(&self, message: &str, num_tasks: usize) -> Box<dyn TaskSequence> {
        TaskSequenceDefault::new(message, num_tasks)
    }        
        
}
    
pub fn register_messenger_default() -> std::io::Result<()> {
    let msg = Box::new(MessengerDefault::new());
    set_boxed_messenger(msg)?;
    Ok(())
}
