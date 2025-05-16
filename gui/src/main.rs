mod messages;
mod message_panel;

//use osmquadtree_utils::clap::{make_app, run, Defaults};

use osmquadtree_utils::commands::{make_app, Defaults, run_app};

use osmquadtree_utils::error::{Error, Result};
use osmquadtree::message;

use gtk::prelude::*;
use gtk::{glib, gio, Application, ApplicationWindow, Box as GtkBox, Orientation, Label};
use gtk::{Widget, ScrolledWindow, TextView, TextBuffer, Grid, Align, Stack, StackSidebar};
use gtk::{Button, CheckButton, Entry, FileDialog, ToggleButton};
use glib::clone;

const APP_ID: &str = "uk.me.jamesharris.OsmquadtreeGui";

use std::sync::{Arc,Mutex};


use clap::{Command,Arg,ValueHint,ArgAction};

//use std::collections::{BTreeMap,VecDeque};
//use messages::Message;
use crate::message_panel::MessagePanel;

fn run_cmd<T: AsRef<std::ffi::OsStr> + std::fmt::Display >(cmd: &[T]) -> Result<()> {
    
    let defaults = Defaults::new();
    run_app(&defaults, cmd)
    
    /*
    println!("called run_cmd");
    let app = make_app();
    println!("made app");
    
    let xx = app.try_get_matches_from(cmd)?;
    println!("got matches");
    
    if let Some((a,b)) = xx.subcommand() {
        let defaults = Defaults::new();
        println!("call {} with {:?}", a, b);
        run(&defaults, a, b)
    } else {
        Err(Error::InvalidInputError(format!("{} invalid", cmd.iter().map(|x| format!("{}", x)).collect::<Vec<String>>().join(" "))))
    }*/
}
/*
fn dump_cmd(lines: &mut Vec<String>, indent: usize, cmd: &clap::Command) {
    lines.push(format!("{}app: {}, {} subcommands, {} args", " ".repeat(indent*2), cmd.get_name(), cmd.get_subcommands().count(), cmd.get_arguments().count()));
    for a in cmd.get_arguments() {
        lines.push(format!("{}{} {} {} {} {   }", 
            " ".repeat(indent*2+4),
            a.get_id(),
            match a.get_short() {Some(s) => format!("{}",s), None => String::from("-")},
            match a.get_long() {Some(s) => s.into(), None => "-"},
            match a.get_num_args() {Some(s) => format!("{}",s), None => String::from("-")},
            match a.get_help() {Some(s) => format!("{}",s), None => String::from("-")},
        ));
    }
    
    for c in cmd.get_subcommands() {
        dump_cmd(lines, indent+1, c);
    }
        
}*/

fn main() -> glib::ExitCode {
    // Create a new application
    
    gio::resources_register_include!("osmquadtree_gui.gresource")
        .expect("Failed to register resources.");
    
    let app = Application::builder().application_id(APP_ID).build();
    

    app.connect_activate(build_ui);
    
    // Run the application
    app.run()
}
fn build_help_page_widgets(clap_app: &mut clap::Command) -> ScrolledWindow {
    
    
    
    let gtk_box = GtkBox::builder()
        .orientation(Orientation::Vertical)
        .build();
    
    let mut help = Vec::new();
    clap_app.write_help(&mut help).expect("?");
    
    let help_str = String::from_utf8(help).unwrap();
    
    let buffer = TextBuffer::builder()
        .text(&help_str)
        .build();
    
    let text_view = TextView::builder()
        .editable(false)
        .monospace(true)
        .buffer(&buffer)
        .hexpand(true)
        .vexpand(true)
        .cursor_visible(false)
        .build();
    
    gtk_box.append(&text_view);
    
    
    ScrolledWindow::builder()
        //.hscrollbar_policy(PolicyType::Never) // Disable horizontal scrolling
        //.min_content_width(360)
        .child(&gtk_box)
        .build()
    
}
    
/*
fn add_notebook_page<T: IsA<Widget>>(notebook: &Notebook, label: &str, widget: &T) {
    
    let label_widget = Label::builder()
        .label(label)
        .build();
        
    let l2 = format!("{}",label);
    widget.connect_show(move |_| { println!("page with label {} shown", l2);});
    notebook.append_page(widget, Some(&label_widget));
    
    
}*/

#[derive(Debug)]
enum ParamWidget {
    Entry(Entry),
    CheckButton(CheckButton),
}

fn prep_args(command_name: &str, entries: &[(String, Option<String>, ParamWidget)]) -> Vec<String> {
    
    let mut result = vec!();
    result.push("osmquadtree-utils".into());
    result.push(command_name.into());
    
    
    for (a,b,c) in entries {
        match c {
            ParamWidget::Entry(entry) => {
                if !entry.text().is_empty() {
                    
                    if let Some(x) = b {
                        result.push(x.into());
                    }
                    result.push(entry.text().into());
                }
            },
            ParamWidget::CheckButton(cb) => {
                if cb.is_active() {
                    if let Some(x) = b {
                        result.push(x.into());
                    } else {
                        println!("!!?? {} {:?} {}", a,b,cb.is_active());
                    }
                }
            }
        }
    }
    result
}
                    
                    
    

/*
fn is_entry(a: &Arg) -> bool {
    if a.get_num_args() == Some(1.into()) {
        return true;
    }
    if a.is_required_set() {
        return true;
    }
    false
}*/

#[derive(PartialEq,Debug)]
enum TaskStatus {
    Running,
    NotRunning
}


#[derive(PartialEq,Debug)]
enum ArgType {
    Checkbox,
    Entry,
    EntryFile,
    EntryPath,
    EntryFilePath
}

fn get_arg_type(a: &Arg) -> Result<ArgType> {
    
    match (a.get_action(), a.get_value_hint()) {
        
        (ArgAction::SetTrue, _) => Ok(ArgType::Checkbox),
        (ArgAction::Set, ValueHint::Unknown) => Ok(ArgType::Entry),
        (ArgAction::Set, ValueHint::FilePath) => Ok(ArgType::EntryFile),
        (ArgAction::Set, ValueHint::DirPath) => Ok(ArgType::EntryPath),
        (ArgAction::Set, ValueHint::AnyPath) => Ok(ArgType::EntryFilePath),
        (p,q) => Err(Error::InvalidInputError(format!("unexpected argument (action, value_hint) {:?} {:?}", p, q)))
    }
}



fn build_subcommand_page(cmd: &Command, task_status: Arc<Mutex<TaskStatus>>, win: &ApplicationWindow) -> impl IsA<Widget> {
    
    let mut entries = Vec::new();
    
    let grid = Grid::builder()
        .row_spacing(12)
        .column_spacing(12)
        .build();
    
    let name = format!("{}", cmd.get_name());
    
    let name_label = Label::builder()
        .use_markup(true)
        .build();
    name_label.set_markup(&format!("<span size=\"x-large\">{}</span>", cmd.get_name()));
    
    grid.attach(&name_label, 0, 0, 4, 1);
    
    let mut row_idx = 1;
    
    for a in cmd.get_arguments() {
        
        let mut tooltip = "".into();
        if let Some(help) = a.get_help() {
            tooltip = format!("{}", help);
        }
        
        let id_label = Label::builder()
            .label(&format!("{}", a.get_id()))
            .halign(Align::Start)
            .tooltip_text(&tooltip)
            .build();
        
        
        grid.attach(&id_label, 0, row_idx, 1, 1);
        
        if let Some(s) = a.get_short() {
            let short_label = Label::builder()
                .label(&format!("{}", s))
                .halign(Align::Start)
                .tooltip_text(&tooltip)
                .build();
            grid.attach(&short_label, 1, row_idx, 1, 1);
        }
        
        if let Some(l) = a.get_long() {
            let long_label = Label::builder()
                .label(&format!("{}", l))
                .halign(Align::Start)
                .tooltip_text(&tooltip)
                .build();
            grid.attach(&long_label, 2, row_idx, 1, 1);
        }
        
        
        
        match get_arg_type(a) {
            
            Err(e) => { println!("{} {} ?? {:?}", cmd.get_name(), a.get_id(), e); },
            Ok(ArgType::Checkbox) => {
                let checkbutton = CheckButton::builder()
                    .build();
            
                grid.attach(&checkbutton, 3, row_idx, 1, 1);
                let ss = match a.get_short() {
                    Some(s) => Some(format!("-{}", s)),
                    None => None
                };
                entries.push((format!("{}",a.get_id()), ss, ParamWidget::CheckButton(checkbutton)));
            },
            Ok(arg_type) => {
            
                let (entry_width, select_file, select_path) = match arg_type {
                    
                    ArgType::Entry => (3,false,false),
                    ArgType::EntryFile => (2, true, false),
                    ArgType::EntryPath => (2, false, true),
                    ArgType::EntryFilePath => (1, true, true),
                    _ => unreachable!()
                };
                           
                let entry = Entry::builder()
                    .hexpand(true)
                    .build();
                
                grid.attach(&entry, 3, row_idx, entry_width, 1);
                let ss = match a.get_short() {
                    Some(s) => Some(format!("-{}", s)),
                    None => None
                };
                
                
                
                if select_path {
                    
                    let arg_id = format!("{}",a.get_id());
                    let button = Button::builder()
                        .label("path")
                        .build();
                    
                    //let entry_clone = entry.clone();
                    //let win_clone = win.clone();
                    button.connect_clicked(clone!(
                        #[weak] entry,
                        #[weak] win,
                        move |_| {
                            println!("clicking button");
                            let dialog = FileDialog::builder()
                                .title(&format!("choose for {}", arg_id))
                                //.application(&parent)
                                .build();
                            
                            dialog.select_folder(
                                Some(&win),
                                gio::Cancellable::NONE,
                                clone!(
                                    #[weak] entry,
                                    move |result| {
                                        
                                        match result {
                                            Ok(fileobj) => {
                                                match fileobj.path() {
                                                    Some(path) => {
                                                        
                                                        println!("choosen {:?}", path);
                                                        match path.to_str() {
                                                            Some(p) => {entry.set_text(p); },
                                                            None => { println!("invalid?"); }
                                                        }
                                                    },
                                                    None => {
                                                        println!("choose NONE?");
                                                    }
                                                }
                                                
                                            },
                                            Err(e) => {
                                                println!("failed {:?}", e);
                                            }
                                        }
                                    }
                                )
                                
                            );
                        })
                    );
                    
                    grid.attach(&button, if select_file { 4 } else { 5 }, row_idx, 1, 1);
                }
                if select_file {
                    let arg_id = format!("{}",a.get_id());
                    let button = Button::builder()
                        .label("file")
                        .build();
                    
                    //let entry_clone = entry.clone();
                    //let win_clone = win.clone();
                    button.connect_clicked(clone!(
                        #[weak] entry,
                        #[weak] win,
                        move |_| {
                            println!("clicking button");
                            let dialog = FileDialog::builder()
                                .title(&format!("choose for {}", arg_id))
                                //.application(&parent)
                                .build();
                            
                            dialog.open(
                                Some(&win),
                                gio::Cancellable::NONE,
                                clone!(
                                    #[weak] entry,
                                    move |result| {
                                        
                                        match result {
                                            Ok(fileobj) => {
                                                match fileobj.path() {
                                                    Some(path) => {
                                                        
                                                        println!("choosen {:?}", path);
                                                        match path.to_str() {
                                                            Some(p) => {entry.set_text(p); },
                                                            None => { println!("invalid?"); }
                                                        }
                                                    },
                                                    None => {
                                                        println!("choose NONE?");
                                                    }
                                                }
                                                
                                            },
                                            Err(e) => {
                                                println!("failed {:?}", e);
                                            }
                                        }
                                    }
                                )
                                
                            );
                        })
                    );
                    
                    grid.attach(&button, 5, row_idx, 1, 1);
                }
                        
                entries.push((format!("{}",a.get_id()), ss, ParamWidget::Entry(entry)));                
            }
             
        } 
            
        row_idx+=1;
    }   
    
    let run_button = Button::builder()
            .label("RUN")
            .build();
        
    grid.attach(&run_button, 3, row_idx, 1, 1);
    
    
    
    
    run_button.connect_clicked(move |_| {
        
          
        match task_status.lock() {
            Ok(mut t) => {
                println!("TaskStatus: {:?}", t);
                if *t == TaskStatus::Running {
                    message!("task already running...");
                    return;
                } else {
                    *t = TaskStatus::Running
                }
                println!("TaskStatus: {:?}", t);
            },
            Err(e) => {
                message!("?? {:?}", e);
                return;
            }
        }
        println!("TaskStatus: {:?}", task_status.lock());
        let task_status_clone = task_status.clone();
        println!("task_status_clone: {:?}",task_status_clone.lock());
        let args = prep_args(&name, &entries);
        gio::spawn_blocking(clone!(
            move || {
                if let Err(e) = run_cmd(&args) { 
                    message!("run_cmd failed?? {:?}, {:?}", args, e);
                }               
                
                *task_status_clone.lock().unwrap() = TaskStatus::NotRunning;
                println!("task_status_clone: {:?}",task_status_clone.lock());
            }
        ));
        
    });   
    
    
    
    grid
    
    
}


fn add_subcommand_pages(stack: &Stack, cmd: &Command, task_status: Arc<Mutex<TaskStatus>>, parent: &ApplicationWindow) {

    
    
    if !(cmd.has_subcommands() && cmd.is_subcommand_required_set()) {
        
        let page = build_subcommand_page(cmd, task_status.clone(), parent);
        
        let name = cmd.get_name();
        
        //add_notebook_page(notebook, name, &page);
        stack.add_titled(&page, Some(&name), &name);
        
    } else {
        println!("skip {}", cmd.get_name());
    }
    
    if cmd.has_subcommands() {
        for subcommand in cmd.get_subcommands() {
            //add_subcommand_pages(notebook, subcommand);
            if subcommand.get_name() == "help" {
                //pass
            } else {
                add_subcommand_pages(stack, subcommand, task_status.clone(), parent);
            }
            
        }
    }
    
}


fn build_ui(app: &Application) {
    
    let (receiver, cancel_set) = messages::register_messenger_gui().unwrap();
    /*
    let mut visible_messages = VecDeque::new();
    for _ in 0..10 {
        visible_messages.push_back("".into());
    }*/
    
    let mut clap_app = make_app();
    
    /*let notebook = Notebook::builder()
        .scrollable(true)
        //.tab_pos(PositionType::Left)
        .build();
    */
    
    let grid = Grid::builder()
        .margin_start(12)
        .margin_top(12)
        .margin_end(12)
        .margin_bottom(12)
        .column_homogeneous(false)
        .row_homogeneous(false)
        .build();
    
    
    let window = ApplicationWindow::builder()
        .application(app)
        .default_width(1200)
        .default_height(950)
        .child(&grid)
        .build();
    
    
    let stack = Stack::builder()
        .hexpand(true)
        .vexpand(true)
        .build();
    
    
    
    
    
    let help_page = build_help_page_widgets(&mut clap_app);
    //add_notebook_page(&notebook, "Help", &help_page);
    stack.add_titled(&help_page, Some("Help"), "Help");
    //add_subcommand_pages(&notebook, &clap_app);
    
    let task_status = Arc::new(Mutex::new(TaskStatus::NotRunning));
    add_subcommand_pages(&stack, &clap_app, task_status, &window);
    
    
    let stack_sidebar = StackSidebar::builder()
        .hexpand(false)
        .vexpand(false)
        .build();
    stack_sidebar.set_stack(&stack);
        
    let selection_model = stack.pages();
    selection_model.connect_selection_changed(clone!(
        #[weak] selection_model,
        move |_,_,_| {
            
            let curr_selection = selection_model.selection();
            message!("page changed: {} {}", curr_selection.size(), curr_selection.minimum());
        
        //message!("switch to page {} [{}]", page, label);
    }));
    
    let scrolled_window = ScrolledWindow::builder()
        .margin_start(12)
        .margin_top(12)
        .margin_end(12)
        .margin_bottom(12)
        //.orientation(Orientation::Vertical)
        //.child(&notebook)
        .vexpand(true)
        .hexpand(false)
        .height_request(600)
        .width_request(120)
        .child(&stack_sidebar)
        .build();
    
    grid.attach(&scrolled_window, 0, 0, 1, 1);
    grid.attach(&stack, 1, 0, 2, 1);
    
        
    
    

    let message_panel = MessagePanel::new();
    
    grid.attach(&message_panel, 0, 1, 3, 1);
    
    
    glib::spawn_future_local(clone!(
        #[weak] message_panel,
        async move {
            
            //for message in receiver.iter() {
            while let Ok(message) = receiver.recv().await {
                
                message_panel.handle_message(message);
            }
            
            
            
        }
    ));
    
    let button = ToggleButton::builder()
        .label("cancel")
        .margin_start(12)
        .margin_end(12)
        .margin_top(12)
        .margin_bottom(12)
        .build();
    
    //gtk_box.append(&button);
    grid.attach(&button, 2, 2, 1, 1);
    
    button.connect_toggled(move |b| {
        cancel_set.store(b.is_active(), std::sync::atomic::Ordering::Relaxed);
    });
    
    /*
    button.connect_clicked(clone!(
        
        move |_| {
            
            gio::spawn_blocking(move || {
                
                if let Err(e) = run_cmd(&vec!["osmquadtree-utils", "count", "/home/james/data/planet-feb2025/", "--filter", "-1.0,49.5,2.5,53.0"]) {
                    message!("run_cmd failed?? {:?}", e);
                }               
                
            });
            
        }
    ));*/
    
    
    
    window.present();
}
    
    
  
