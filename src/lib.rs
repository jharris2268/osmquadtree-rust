pub mod read_pbf;
pub mod read_file_block;
pub mod write_pbf;
pub mod header_block;
pub mod writefile;
pub mod count;

pub mod callback;
pub mod stringutils;
pub mod utils;
pub mod convertblocks;
pub mod sortblocks;
pub mod update;
pub mod elements;

#[cfg(test)]
mod tests {
    use read_pbf;
        
    #[test]
    fn test_read_all_tags() {
        
        let data:Vec<u8> = vec![8, 27, 16, 181, 254, 132, 214, 241, 2, 26, 4, 102, 114, 111, 103];
        let decoded = read_pbf::read_all_tags(&data, 0);
        
        let should_equal = vec![
            read_pbf::PbfTag::Value(1, 27),
            read_pbf::PbfTag::Value(2, 99233120053),
            read_pbf::PbfTag::Data(3, String::from("frog").into_bytes())
        ];
        
        assert_eq!(decoded, should_equal);
        
        
    }
    
    #[test]
    fn test_read_uint32() {
        let data:Vec<u8> = vec![11, 60, 198, 127];
        let (r, p) = read_pbf::read_uint32(&data,0).unwrap();
        assert_eq!(r, 188532351);
        assert_eq!(p, 4);
    }
}

