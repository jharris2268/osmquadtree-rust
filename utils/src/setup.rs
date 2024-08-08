
//use std::io::{Result,Error,ErrorKind};
use std::sync::Arc;
use std::path::{Path,PathBuf};


//#[allow(unused_imports)]
use {
    
    osmquadtree::elements::{WithQuadtree},
    osmquadtree::calcqts::{run_calcqts, run_calcqts_addto_objs},
    osmquadtree::pbfformat::{file_length, CompressionType},
    osmquadtree::sortblocks::{find_groups, find_tree_groups, sort_blocks, sort_blocks_inmem, QuadtreeTree, SortBlocks, write_blocks},
    osmquadtree::update::{run_update, run_update_initial,get_state},
    osmquadtree::utils::{date_string, timestamp_string}
};

use console::Style;
use dialoguer::{theme::ColorfulTheme, Confirm, Input, Select};

use crate::error::{Error,Result};



fn round_timestamp(ts: i64) -> i64 {
    (f64::round(ts as f64/(24.0*60.0*60.0)) as i64)*24*60*60
}
/*
fn estimate_timestamp(src_filename: &str, numchan: usize) -> Result<i64> {
    
    let count = call_count(src_filename, false, numchan, None, None)?;
    if let CountAny::Count(cc) = count {
        let max_ts = i64::max(i64::max(cc.node.max_ts, cc.way.max_ts),cc.relation.max_ts);
        if max_ts > 0 {
            return Ok(round_timestamp(max_ts)));
        }
    }
    
    return Err(Error::new(ErrorKind::Other, format!("{} not a pbf file??", src_filename)))
}*/

fn prepare_output_dir(dest_root: &str) -> Result<PathBuf> {
    let dest_path = Path::new(dest_root).to_owned();
    if dest_path.try_exists()? {
        if dest_path.is_file() {
            return Err(Error::OutputFileExists("specifed output exists and is a file?".to_string()));
        } else {
            //nothing to do
        }
    } else {
        std::fs::create_dir(&dest_path)?;
    }
    
    Ok(dest_path)
}


pub(crate) fn run(ram_gb_default: usize, numchan_default:usize) -> Result<()> {
    
    let theme = ColorfulTheme {
        values_style: Style::new().yellow().dim(),
        ..ColorfulTheme::default()
    };
    println!("Welcome to the osmquadtree setup wizard");
    println!("This will sort a planet (or extract) pbf file into spatial related blocks");

    if !Confirm::with_theme(&theme)
        .with_prompt("Have you downloaded a planet.osm.pbf file?")
        .interact()?
    {
        println!("{}\n{}",
            "Please read https://wiki.openstreetmap.org/wiki/Downloading_data first,",
            "and download a planet (or extract) pbf file");
            
        return Ok(());
    }
    let extra_options = Confirm::with_theme(&theme)
        .with_prompt("do you want extra options?")
        .default(false)
        .interact()?;
    
    let numchan:usize = if extra_options {
        Input::with_theme(&theme)
            .with_prompt("how many parallel threads (defaults to number of logical cores). Enter zero for single threading.")
            .with_initial_text(format!("{}", numchan_default))
            .interact()?
    } else {
        numchan_default
    };
    
    let ram_gb: usize = if extra_options {
        Input::with_theme(&theme)
            .with_prompt("maximum memory available (in GB)")
            .with_initial_text(format!("{}", ram_gb_default))
            .interact()?
    } else {
        ram_gb_default
    };
    
    
            
    
    let src_filename: String = Input::with_theme(&theme)
        .with_prompt("please enter file name")
        .interact_text()?;   
    
    let dest_root: String = Input::with_theme(&theme)
        .with_prompt("please specify output directory")
        .interact_text()?;
    
    let dest_path = prepare_output_dir(&dest_root)?;
    
    let compression_type = if extra_options {
        let sel = Select::with_theme(&theme)
            .with_prompt("compression type")
            .default(1)
            .item("None")
            .item("Zlib")
            .item("Brotli")
            .interact()?;
        match sel {
            0 => CompressionType::Uncompressed,
            1 => CompressionType::Zlib,
            2 => CompressionType::Brotli,
            _ => unreachable!()
        }

    } else {
        CompressionType::Zlib
    };
    
    
    println!("osmquadtree can use also sort replaction diffs in the same way.");
    println!("These can be combined with the original data to produce up-to-date extracts.");
    
    let update_confirm = Confirm::with_theme(&theme)
        .with_prompt("set up replication diff updating")
        .interact()?;
        
    let mut update_spec = None;
    if update_confirm {
    
        let items = vec!["planet.openstreetmap.org", "geofabrik.de", "other"];
        let replication_select = Select::with_theme(&theme)
            .with_prompt("select source for update replication diffs")
            .items(&items)
            .default(0)
            .interact()?;
            
        let replication_src = match replication_select {
            0 => String::from("https://planet.openstreetmap.org/replication/day/"),
            1 => {
                Input::with_theme(&theme)
                    .with_prompt("enter replication root:")
                    .with_initial_text("https://download.geofabrik.de/<SRC>")
                    .interact_text()?
            },
            2 => {
                Input::with_theme(&theme)
                    .with_prompt("enter replication root:")
                    .interact_text()?
            },
            x => Err(Error::InvalidInputError(format!("invalid option selected {}", x)))?
        };
        
        //let mut current_state: Option<(i64,i64)> = None;
        match get_state(&replication_src, None) {
            Ok((a,b)) => { println!("replication source ok, current state {} [timestamp {}]",a,timestamp_string(b)); },
            Err(e) => {
                return Err(Error::GetStateError(format!("replication src incorrect? {} state.txt not available [{}]", replication_src, e)));
                //return Err(Error::new(ErrorKind::Other, format!("replication src incorrect? {} state.txt not available [{}]", replication_src, e)));
            }
        };
        
        
        let repl_diff_location: String = Input::with_theme(&theme)
            .with_prompt("replication diff local file location")
            .interact_text()?
        ;
        
        let p = prepare_output_dir(&repl_diff_location)?; 
        let state_csv = p.join("state.csv");
        if !state_csv.is_file() {
            match std::fs::OpenOptions::new().create(true).write(true).open(state_csv) {
                Ok(_) => Ok(()),
                Err(e) => Err(e),
            }?;
        }
        
        update_spec = Some((replication_src, repl_diff_location));
    }
    
           
        
    
    let file_len = file_length(&src_filename);
    let (out_filename_file,timestamp) = if file_len < (ram_gb as u64)*1024*1024*1024 / 64  {
        
        let (blocks, lt, max_timestamp) = run_calcqts_addto_objs(&src_filename, 17, 0.05, numchan)?;
        
        
        let timestamp = round_timestamp(max_timestamp);
        println!("max_timestamp = {} [{}], rounded: {} [{}]", max_timestamp, timestamp_string(max_timestamp), timestamp, timestamp_string(timestamp));
        
        
        let out_filename_file = format!("{}.pbf",date_string(timestamp));
        let out_filename = String::from(dest_path.join(&out_filename_file).to_str().unwrap());
        println!("will write to {}", out_filename);
        
        let mut tree = Box::new(QuadtreeTree::new());
        for bl in &blocks {
            for n in &bl.nodes {
                tree.add(n.get_quadtree(),1);
            }
            for w in &bl.ways {
                tree.add(w.get_quadtree(),1);
            }
            for r in &bl.relations {
                tree.add(r.get_quadtree(),1);
            }
        }
        
        let groups: Arc<QuadtreeTree> = Arc::from(find_tree_groups(tree, 40000, -1)?);
        println!("groups: {} {}", groups.len(), groups.total_weight());
        
        let mut sb = SortBlocks::new(groups);
        for bl in blocks {
            sb.add_all(bl.into_iter())?;
        }
        
        let sorted = sb.finish();
        
        write_blocks(&out_filename, sorted, numchan, timestamp, compression_type)?;
        println!("{}", lt);
        (out_filename_file, timestamp)
        
    } else {
               
        let use_inmem = file_len < (ram_gb as u64)*1024*1024*1024/32;
        
        
        let qtsfn_in = String::from(dest_path.join("qts.pbf").to_str().unwrap());
        
        let (qts_filename, mut lt, max_timestamp) = run_calcqts(&src_filename, Some(&qtsfn_in), 17, 0.05, None, false, numchan, ram_gb)?;
        
        
        
        
        let timestamp = round_timestamp(max_timestamp);
        println!("max_timestamp = {} [{}], rounded: {} [{}]", max_timestamp, timestamp_string(max_timestamp), timestamp, timestamp_string(timestamp));
        
        let out_filename_file = format!("{}.pbf",date_string(timestamp));
        let out_filename = String::from(dest_path.join(&out_filename_file).to_str().unwrap());
            
            
        let max_depth=17;
        let target = 40000;
        let mintarget = -1;
        let mut splitat = 0i64;
        let tempinmem = file_len < (ram_gb as u64)*1024*1024*1024/16;
        let mut limit = 0usize;

        if splitat == 0 {
            splitat = 1500000i64 / target;
        }
                
                
        let groups: Arc<QuadtreeTree> = Arc::from(find_groups(
            &qts_filename, numchan, max_depth, target, mintarget, &mut lt,
        )?);

        println!("groups: {} {}", groups.len(), groups.total_weight());
        if limit == 0 {
            limit = 4000000usize * ram_gb / (groups.len() / (splitat as usize));
            if tempinmem {
                limit = usize::max(1000, limit / 10);
            }
        }
        if use_inmem {
            sort_blocks_inmem(
                &src_filename, &qts_filename, &out_filename,
                    groups, numchan, timestamp, compression_type, &mut lt)?;
        } else {
            sort_blocks(
                &src_filename, &qts_filename, &out_filename,
                    groups, numchan, splitat, tempinmem, limit, timestamp, false, compression_type, &mut lt)?;
        }
        
        std::fs::remove_file(&qts_filename)?;
        
        
        println!("{}", lt);
        
        (out_filename_file, timestamp)
        
    };
    
    if let Some((replication_src, repl_diff_location)) = update_spec {
        
        run_update_initial(&dest_root, &out_filename_file, &timestamp_string(timestamp), None, Some(&replication_src), &repl_diff_location,
            17, 0.05, numchan)?;
            
            
        run_update(&dest_root, 0, false, numchan)?;
        
    }            
            
    Ok(())    
}
