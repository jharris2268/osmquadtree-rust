
mod imp {
    use gtk::{glib, CompositeTemplate,TemplateChild,TextView,Label,ProgressBar, Button, TextBuffer};
    use gtk::subclass::prelude::*;
    use gtk::prelude::*;
    use glib::subclass::InitializingObject;
    
    
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    
    use crate::messages::Message;
    enum Progress {
        Bytes(String, u64),
        Percent(String)
    }
    
    
    // Object holding the state
    #[derive(CompositeTemplate, Default)]
    #[template(resource = "/uk/me/jamesharris/OsmquadtreeGui/message_panel.ui")]
    pub struct MessagePanel {
        #[template_child]
        pub messages_text_view: TemplateChild<TextView>,
        
        #[template_child]
        pub progress_label_left: TemplateChild<Label>,
        
        #[template_child]
        pub progress_bar: TemplateChild<ProgressBar>,
        
        #[template_child]
        pub progress_label_right: TemplateChild<Label>,
        
        #[template_child]
        pub message_clear_button: TemplateChild<Button>,
        #[template_child]
        pub message_copy_button: TemplateChild<Button>,
        
        //pub settings: OnceCell<Settings>,
        
        pub message_text_buffer: TextBuffer,
        progress_track: RefCell<BTreeMap<String, Progress>>,
        
        
    }

    // The central trait for subclassing a GObject
    #[glib::object_subclass]
    impl ObjectSubclass for MessagePanel {
        // `NAME` needs to match `class` attribute of template
        const NAME: &'static str = "MessagePanel";
        type Type = super::MessagePanel;
        type ParentType = gtk::Box;

        fn class_init(klass: &mut Self::Class) {
            klass.bind_template();
            
            
            
        }

        fn instance_init(obj: &InitializingObject<Self>) {
            obj.init_template();
                                    
        }
    }
    
    impl ObjectImpl for MessagePanel {
        fn constructed(&self) {
        // Call "constructed" on parent
            self.parent_constructed();

            self.messages_text_view.set_buffer(Some(&self.message_text_buffer));

            // Setup
            let obj = self.obj();
            //obj.setup_settings();
            //obj.setup_tasks();
            obj.setup_callbacks();
            //obj.setup_tasks();
            //obj.restore_data();
            //obj.setup_factory();
            //obj.setup_actions();
            
            
        }
        
    }
    impl WidgetImpl for MessagePanel {}
    impl BoxImpl for MessagePanel {}
    
    
    impl MessagePanel {
        
        fn append_message(&self, text: &str) {
            self.message_text_buffer.insert(
                &mut self.message_text_buffer.end_iter(),
                &format!("{}\n",text)
            );
        }
        pub fn handle_message(&self, message: Message) {
            match message {
                Message::Message(msg) => {
                    self.append_message(&msg);
                                    
                },
                
                Message::StartProgressBytes(key, message, nbytes) => {
                    self.progress_label_right.set_label(&message);
                    
                    
                    self.progress_track.borrow_mut().insert(key, Progress::Bytes(message,nbytes));
                },
                
                Message::UpdateProgressBytes(key, bytes, tail) => {
                    
                    if let Some(Progress::Bytes(_, nbytes)) = self.progress_track.borrow().get(&key) {
                        let pos = (bytes as f64)  / (*nbytes as f64);
                        self.progress_bar.set_fraction(pos);
                        self.progress_label_left.set_label(&tail);
                    }
                },                       
                
                Message::FinishProgressBytes(key, tail) => {
                    let mut pt = self.progress_track.borrow_mut();
                    if let Some(Progress::Bytes(msg, _)) = pt.get(&key) {
                        self.append_message(&format!("{} [{}] {}", msg, "=".repeat(25), tail));
                        pt.remove(&key);
                    }
                    self.progress_label_left.set_label(&tail);
                    self.progress_bar.set_fraction(1.0);
                    
                },
                Message::StartProgressPercent(key, message) => {
                    self.progress_label_right.set_label(&message);
                    self.progress_track.borrow_mut().insert(key, Progress::Percent(message));
                },
                Message::UpdateProgressPercent(key, percent, tail) => {
                    
                    if let Some(Progress::Percent(_)) = self.progress_track.borrow().get(&key) {
                        //let pos = (bytes as f64)  / (*nbytes as f64);
                        let pos = percent / 100.0;
                        self.progress_bar.set_fraction(pos);
                        self.progress_label_left.set_label(&tail);
                    }
                },                       
                
                Message::FinishProgressPercent(key, tail) => {
                    let mut pt = self.progress_track.borrow_mut();
                    if let Some(Progress::Percent(msg)) = pt.get(&key) {
                        self.append_message(&format!("{} [{}] {}", msg, "=".repeat(25), tail));
                        pt.remove(&key);
                    }
                    self.progress_label_left.set_label(&tail);
                    self.progress_bar.set_fraction(1.0);
                    
                },
                Message::ChangeProgressBytes(key, message) => {
                    let pt = self.progress_track.borrow_mut();
                    if let Some(Progress::Bytes(_, _)) = pt.get(&key) {
                        self.progress_label_right.set_label(&message);
                    }
                },
                Message::ChangeProgressPercent(key, message) => {
                    let pt = self.progress_track.borrow_mut();
                    if let Some(Progress::Percent(_)) = pt.get(&key) {
                        self.progress_label_right.set_label(&message);
                    }
                },
                /*
                o => {
                    println!("recieved {:?}", o);
                }*/
            }   
        }
    }

}

use gtk::{glib,gio,gdk};
use glib::{Object, clone};
use gtk::prelude::*;
use gtk::subclass::prelude::*;
//use gio::Settings;


//use crate::APP_ID;
use crate::messages::Message;



glib::wrapper! {
    pub struct MessagePanel(ObjectSubclass<imp::MessagePanel>)
        @extends gtk::Box, gtk::Widget,
        @implements gio::ActionGroup, gio::ActionMap, gtk::Accessible, gtk::Buildable,
                    gtk::ConstraintTarget, gtk::Native, gtk::Root, gtk::ShortcutManager;
}

impl MessagePanel {
    pub fn new() -> Self {
        // Create new window
        Object::builder().build()
    }
    
    
    fn setup_callbacks(&self) {
        
        
        self.imp().message_clear_button.connect_clicked(clone!(
            #[weak(rename_to = panel)]
            self,
            move |_| {
                panel.imp().message_text_buffer.set_text("");
                println!("clear messages");
            }
        ));

        self.imp().message_copy_button.connect_clicked(clone!(
            #[weak(rename_to = panel)]
            self,
            move |_| {
                let display = gdk::Display::default().unwrap();
                let clipboard = display.clipboard();
                
                
                /*
                if let Err(e) = clipboard.set_content(Some(&gdk::ContentProvider::for_value(&glib::Value::from(panel.imp().message_text_buffer.text())))) {
                    println!("?? {:?}", e);
                }
                */
                
                panel.imp().message_text_buffer.select_range(
                    &panel.imp().message_text_buffer.start_iter(),
                    &panel.imp().message_text_buffer.end_iter()
                );
                panel.imp().message_text_buffer.copy_clipboard(&clipboard);
                
                
                println!("copy messages");
            }
        ));
    }
        
    
    /*
    fn setup_settings(&self) {
        let settings = Settings::new(APP_ID);
        self.imp()
            .settings
            .set(settings)
            .expect("`settings` should not be set before calling `setup_settings`.");
    }

    fn settings(&self) -> &Settings {
        self.imp()
            .settings
            .get()
            .expect("`settings` should be set in `setup_settings`.")
    }*/
    
    pub fn handle_message(&self, message: Message) {
        self.imp().handle_message(message);
        
        


    }
}
