
use crate::commands::{Defaults};
use crate::commands::sortblocks::{SortblocksType, SortblocksCommon, CompressionType as ClapCompresstionType};

use crate::error::Result;
use osmquadtree::message;
use osmquadtree::sortblocks::{sort_blocks, sort_blocks_inmem, find_groups, QuadtreeTree};
use osmquadtree::pbfformat::{CompressionType,file_length};
use osmquadtree::utils::{LogTimes,parse_timestamp};
use std::sync::Arc;


pub(crate) fn get_compression_type(compression_type: &ClapCompresstionType, level: &Option<u32>) -> CompressionType {

    

    if compression_type.brotli {
        
        if let Some(l) = level {
            if *l>11 {
                panic!("max compression level for brotli is 11")
            }
            return CompressionType::BrotliLevel(*l);
        } else {
            return CompressionType::Brotli;
        }
    } else if compression_type.lzma {
        if let Some(l) = level {
            if *l>9 {
                panic!("max compression level for lzma is 9")
            }
            return CompressionType::LzmaLevel(*l);
        } else {
            return CompressionType::Lzma;
        }
    } else if compression_type.uncompressed {
        
        return CompressionType::Uncompressed;
    }

    
    if let Some(l) = level {
        if *l>9 {
            panic!("max compression level for zlib is 9")
        }
        return CompressionType::ZlibLevel(*l);
    }
    return CompressionType::Zlib;
    
    
}

pub(crate) fn run_sortblocks(sortblocks: &SortblocksCommon, sortblocks_type: SortblocksType, defaults: &Defaults)  -> Result<()> {
    let mut lt = LogTimes::new();

    
    
    
    let splitat = 1500000i64 / sortblocks.target;

    let qtsfn = match &sortblocks.qtsfn {
        Some(q) => String::from(q),
        None => format!("{}-qts.pbf", &sortblocks.input[0..sortblocks.input.len() - 4]),
    };
    

    let outfn = match &sortblocks.outfn {
        Some(q) => String::from(q),
        None => format!("{}-blocks.pbf", &sortblocks.input[0..sortblocks.input.len() - 4]),
    };
    

    let timestamp = match sortblocks.timestamp.as_deref() {
        Some(t) => parse_timestamp(&t)?,
        None => 0,
    };
    
    let min_target = match sortblocks.min_target {
        None => sortblocks.target / 2,
        Some(m) => m
    };
    
    
    let numchan = match sortblocks.numchan {
        Some(n) => n.into(),
        None => defaults.numchan_default
    };
    
    let groups: Arc<QuadtreeTree> = Arc::from(find_groups(
        &qtsfn, numchan, sortblocks.max_qt_graph_level.into(), sortblocks.target, min_target, &mut lt,
    )?);

    message!("groups: {} {}", groups.len(), groups.total_weight());
    
           
    
    
    
    let compression_type = get_compression_type(&sortblocks.compression_type, &sortblocks.compression_level);
    
    
    match sortblocks_type {
        SortblocksType::Inmem => {
            sort_blocks_inmem(&sortblocks.input, &qtsfn, &outfn, groups, numchan, timestamp, compression_type, &mut lt)?;
        },
        SortblocksType::Normal((keeptemps, ram_gb_in)) => {
            
            let ram_gb = match ram_gb_in {
                Some(r) => r.into(),
                None => defaults.ram_gb_default
            }; 
            
            let tempinmem = file_length(&sortblocks.input) < 32 * 1024 * 1024 * (ram_gb as u64);
    
            let limit = {
                
                let mut l = 4000000usize * ram_gb / (groups.len() / (splitat as usize));
                if tempinmem {
                    l = usize::max(1000, l / 10);
                }
                l
                
            };
            
            sort_blocks(
                &sortblocks.input, &qtsfn, &outfn, groups, numchan, splitat, tempinmem, limit, /*write_at*/
                timestamp, keeptemps, compression_type,&mut lt,
            )?;
        }
    }
    message!("{}", lt);
    Ok(())
}
