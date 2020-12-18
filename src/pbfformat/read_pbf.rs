//use std::error::Error;
use std::io;
use std::io::ErrorKind;
use std::iter::FromIterator;
#[derive(PartialEq, Debug)]
pub enum PbfTag<'a> {
    Value(u64, u64),
    Data(u64, &'a [u8]),
    Null,
}

pub fn read_uint32(data: &[u8], pos: usize) -> io::Result<(u64, usize)> {
    if (pos + 4) > data.len() {
        return Err(io::Error::new(ErrorKind::Other, "too short"));
    }
    let mut res: u64 = 0;
    //assert!(pos+3 < data.len());

    res |= data[pos + 3] as u64;
    res |= (data[pos + 2] as u64) << 8;
    res |= (data[pos + 1] as u64) << 16;
    res |= (data[pos + 0] as u64) << 24;

    Ok((res, pos + 4))
}

pub fn un_zig_zag(uv: u64) -> i64 {
    let x = (uv >> 1) as i64;
    if (uv & 1) != 0 {
        return x ^ -1;
    }
    x
}

pub fn read_uint(data: &[u8], pos: usize) -> (u64, usize) {
    let mut res: u64 = 0;
    let mut i = 0;
    loop {
        if i >= 10 {
            break;
        }
        //for i in 0..9 {
        let x = data[pos + i];
        let y = (x & 127) as u64;
        res |= y << (7 * i);

        if (x & 128) == 0 {
            return (res, pos + i + 1);
        }
        i += 1;
    }
    (res, pos + 10)
}

pub fn read_data<'a>(data: &'a [u8], pos: usize) -> (&'a [u8], usize) {
    let (ln, pos) = read_uint(data, pos);

    let l = ln as usize;
    (&data[pos..pos + l], pos + l)
}

pub fn read_tag<'a>(data: &'a [u8], pos: usize) -> (PbfTag<'a>, usize) {
    let (t, pos) = read_uint(data, pos);

    if t == 0 {
        return (PbfTag::Null, pos);
    }

    if (t & 7) == 0 {
        let (v, pos) = read_uint(data, pos);
        return (PbfTag::Value(t >> 3, v), pos);
    }
    if (t & 7) == 2 {
        let (s, pos) = read_data(data, pos);
        return (PbfTag::Data(t >> 3, s), pos);
    }
    (PbfTag::Null, pos)
}

pub struct IterTags<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> IterTags<'a> {
    pub fn new(data: &'a [u8], pos: usize) -> IterTags<'a> {
        IterTags { data, pos }
    }
}

impl<'a> Iterator for IterTags<'a> {
    type Item = PbfTag<'a>;

    fn next(&mut self) -> Option<PbfTag<'a>> {
        if self.pos < self.data.len() {
            let (t, npos) = read_tag(self.data, self.pos);
            self.pos = npos;
            return Some(t);
        }
        None
    }
}

pub fn read_all_tags<'a>(data: &'a [u8], pos: usize) -> Vec<PbfTag<'a>> {
    Vec::from_iter(IterTags::new(data, pos))
    
}

fn count_packed_len(data: &[u8]) -> usize {
    let mut pos = 0;
    let mut count = 0;

    while pos < data.len() {
        pos = read_uint(data, pos).1;
        count += 1;
    }
    count
}

pub struct DeltaPackedInt<'a> {
    data: &'a [u8],
    curr: i64,
    pos: usize,
}

impl DeltaPackedInt<'_> {
    pub fn new(data: &'_ [u8]) -> DeltaPackedInt<'_> {
        DeltaPackedInt {
            data,
            curr: 0,
            pos: 0,
        }
    }
}

impl Iterator for DeltaPackedInt<'_> {
    type Item = i64;

    fn next(&mut self) -> Option<i64> {
        if self.pos < self.data.len() {
            let (t, npos) = read_uint(&self.data, self.pos);
            let p = un_zig_zag(t);
            self.curr += p;

            self.pos = npos;

            Some(self.curr)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let s = count_packed_len(&self.data);
        (s, Some(s))
    }
}
impl ExactSizeIterator for DeltaPackedInt<'_> {
    fn len(&self) -> usize {
        count_packed_len(&self.data)
    }
}

pub struct PackedInt<'a> {
    data: &'a [u8],
    pos: usize,
}
impl PackedInt<'_> {
    pub fn new(data: &[u8]) -> PackedInt<'_> {
        PackedInt { data, pos: 0 }
    }
}

impl Iterator for PackedInt<'_> {
    type Item = u64;

    fn next(&mut self) -> Option<u64> {
        if self.pos < self.data.len() {
            let (t, npos) = read_uint(&self.data, self.pos);
            self.pos = npos;

            Some(t)
        } else {
            None
        }
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        let s = count_packed_len(&self.data);
        (s, Some(s))
    }
}

impl ExactSizeIterator for PackedInt<'_> {
    fn len(&self) -> usize {
        count_packed_len(&self.data)
    }
}

pub fn read_delta_packed_int(data: &[u8]) -> Vec<i64> {    
    DeltaPackedInt::new(data).collect()

}

pub fn read_packed_int(data: &[u8]) -> Vec<u64> {
    PackedInt::new(data).collect()
}

#[cfg(test)]
mod tests {
    use crate::pbfformat::read_pbf;
    #[test]
    fn test_read_all_tags() {
        let data: Vec<u8> = vec![
            8, 27, 16, 181, 254, 132, 214, 241, 2, 26, 4, 102, 114, 111, 103,
        ];
        let decoded = read_pbf::read_all_tags(&data, 0);

        let should_equal = vec![
            read_pbf::PbfTag::Value(1, 27),
            read_pbf::PbfTag::Value(2, 99233120053),
            read_pbf::PbfTag::Data(3, b"frog"),
        ];

        assert_eq!(decoded, should_equal);
    }

    #[test]
    fn test_read_uint32() {
        let data: Vec<u8> = vec![11, 60, 198, 127];
        let (r, p) = read_pbf::read_uint32(&data, 0).unwrap();
        assert_eq!(r, 188532351);
        assert_eq!(p, 4);
    }
    
    
    #[test]
    fn test_read_packed_int() {
        let data: Vec<u8> = vec![25, 155,33, 232,154,3, 0];
        let unpacked = read_pbf::read_packed_int(&data);
        
        assert_eq!(unpacked, vec![25, 33*128+27, 3*128*128 + 26*128+104, 0]);
    }
    
    
    
}
