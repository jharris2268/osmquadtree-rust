
use indicatif::{ProgressBar, ProgressStyle};
use crate::logging::{ProgressBytes,ProgressPercent,Messenger,set_boxed_messenger};



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
    
        
        
}
    
pub fn register_messenger_default() -> std::io::Result<()> {
    let msg = Box::new(MessengerDefault::new());
    set_boxed_messenger(msg)?;
    Ok(())
}
