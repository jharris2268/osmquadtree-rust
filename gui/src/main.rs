mod messages;
mod message_panel;

use osmquadtree_utils::clap::{make_app, run, Defaults};
use osmquadtree_utils::error::{Error, Result};
use osmquadtree::message;

use gtk::prelude::*;
use gtk::{glib, gio, Application, ApplicationWindow, Box as GtkBox, Orientation, Label};
use gtk::{Widget, ScrolledWindow, TextView, TextBuffer, Grid, Align, Stack, StackSidebar};
use gtk::{Button, CheckButton, Entry};
use glib::clone;

const APP_ID: &str = "uk.me.jamesharris.OsmquadtreeGui";

use clap::{Command,Arg};

//use std::collections::{BTreeMap,VecDeque};
//use messages::Message;
use crate::message_panel::MessagePanel;

fn run_cmd<T: AsRef<std::ffi::OsStr> + std::fmt::Display >(cmd: &[T]) -> Result<()> {
    
    let app = make_app();
    let xx = app.get_matches_from(cmd);
    if let Some((a,b)) = xx.subcommand() {
        let defaults = Defaults::new();
        println!("call {} with {:?}", a, b);
        run(&defaults, a, b)
    } else {
        Err(Error::InvalidInputError(format!("{} invalid", cmd.iter().map(|x| format!("{}", x)).collect::<Vec<String>>().join(" "))))
    }
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
                    
                    
    


fn is_entry(a: &Arg) -> bool {
    if a.get_num_args() == Some(1.into()) {
        return true;
    }
    if a.is_required_set() {
        return true;
    }
    false
}

fn build_subcommand_page(cmd: &Command) -> impl IsA<Widget> {
    
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
        
        let id_label = Label::builder()
            .label(&format!("{}", a.get_id()))
            .halign(Align::Start)
            .build();
        grid.attach(&id_label, 0, row_idx, 1, 1);
        
        if let Some(s) = a.get_short() {
            let short_label = Label::builder()
                .label(&format!("{}", s))
                .halign(Align::Start)
                .build();
            grid.attach(&short_label, 1, row_idx, 1, 1);
        }
        
        if let Some(l) = a.get_long() {
            let long_label = Label::builder()
                .label(&format!("{}", l))
                .halign(Align::Start)
                .build();
            grid.attach(&long_label, 2, row_idx, 1, 1);
        }
        
        if is_entry(a) {
            
            let entry = Entry::builder()
                .hexpand(true)
                .build();
            
            grid.attach(&entry, 3, row_idx, 1, 1);
            let ss = match a.get_short() {
                Some(s) => Some(format!("-{}", s)),
                None => None
            };
            entries.push((format!("{}",a.get_id()), ss, ParamWidget::Entry(entry)));
        } else {
            let checkbutton = CheckButton::builder()
                .build();
            
            grid.attach(&checkbutton, 3, row_idx, 1, 1);
            let ss = match a.get_short() {
                Some(s) => Some(format!("-{}", s)),
                None => None
            };
            entries.push((format!("{}",a.get_id()), ss, ParamWidget::CheckButton(checkbutton)));
        }
        row_idx+=1;
    }   
    
    let run_button = Button::builder()
            .label("RUN")
            .build();
        
    grid.attach(&run_button, 3, row_idx, 1, 1);
    run_button.connect_clicked(move |_| {
        
        let args = prep_args(&name, &entries);
        
        //println!("run {} {:?}", name, args);
        gio::spawn_blocking(move || {
            if let Err(e) = run_cmd(&args) { 
                message!("run_cmd failed?? {:?}, {:?}", args, e);
            }               
            
        });
        
    });   
    
    grid
    
    
}


fn add_subcommand_pages(stack: &Stack, cmd: &Command) {
//fn add_subcommand_pages(notebook: &Notebook, cmd: &Command) {
    /*
    for i in 0..8 {
        let name = format!("page {:02}", i);
        let grid = Grid::builder()
            .build();
               
        let name_label = Label::builder()
            .use_markup(true)
            .build();
        name_label.set_markup(&format!("<span size=\"x-large\">{}</span>", &name));
        grid.attach(&name_label, 0, 0, 4, 1);
        add_notebook_page(notebook, &name, &grid);
    }*/
    
    
    if !(cmd.has_subcommands() && cmd.is_subcommand_required_set()) {
        
        let page = build_subcommand_page(cmd);
        let name = cmd.get_name();
        
        //add_notebook_page(notebook, name, &page);
        stack.add_titled(&page, Some(&name), &name);
        
    } else {
        println!("skip {}", cmd.get_name());
    }
    
    if cmd.has_subcommands() {
        for subcommand in cmd.get_subcommands() {
            //add_subcommand_pages(notebook, subcommand);
            add_subcommand_pages(stack, subcommand);
            
        }
    }
    
}


fn build_ui(app: &Application) {
    
    let receiver = messages::register_messenger_gui().unwrap();
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
    
    let stack = Stack::builder()
        .hexpand(true)
        .vexpand(true)
        .build();
    
    
    
    
    
    let help_page = build_help_page_widgets(&mut clap_app);
    //add_notebook_page(&notebook, "Help", &help_page);
    stack.add_titled(&help_page, Some("Help"), "Help");
    //add_subcommand_pages(&notebook, &clap_app);
    add_subcommand_pages(&stack, &clap_app);
    
    
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
    
    let button = Button::builder()
        .label("run")
        .margin_start(12)
        .margin_end(12)
        .margin_top(12)
        .margin_bottom(12)
        .build();
    
    //gtk_box.append(&button);
    grid.attach(&button, 2, 2, 1, 1);
    
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
    
    let window = ApplicationWindow::builder()
        .application(app)
        .default_width(1200)
        .default_height(950)
        .child(&grid)
        .build();
    
    window.present();
}
    
    
  
