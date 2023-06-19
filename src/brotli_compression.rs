
use brotli::{CompressorWriter, Decompressor};
use std::io::{Result,Read,Write};

pub fn compress_brotli(src: &[u8], level: u32) -> Result<Vec<u8>> {

    let mut writer = CompressorWriter::new(
        Vec::new(),
        src.len(),
        level,
        22);
    writer.write_all(src)?;
    let r = writer.into_inner();

    //println!("compress brotli {} => {} [{}%]", src.len(), r.len(), 100.0*(r.len() as f64) / (src.len() as f64));
	Ok(r)
}


pub fn decompress_brotli(src: &[u8]) -> Result<Vec<u8>> {

	let mut comp = Vec::new();
    let mut xx = Decompressor::new(
            src,
            4096);
    xx.read_to_end(&mut comp)?;
    //println!("decompress_brotli {} => {}", src.len(), comp.len());
   	Ok(comp)
}
