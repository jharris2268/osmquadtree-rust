use std::iter::Iterator;

pub fn write_uint32(res: &mut Vec<u8>, val: u32) {
    
    res.push(((val >> 24) & 0xff) as u8);
    res.push(((val >> 16) & 0xff) as u8);
    res.push(((val >> 8 ) & 0xff) as u8);
    res.push(((val)       & 0xff) as u8);
}

    

pub fn zig_zag(v: i64) -> u64 {
    ((v<<1) ^ (v>>63)) as u64
}
    
fn write_varint(res: &mut Vec<u8>, val: u64) {

    let mut v = val;
    for _ in 0..10 {
        if v>0x7f {
        
            res.push(((v & 0x7F) | 0x80) as u8);
            v >>= 7;
            if v==0 {
                return;
            }
        } else {
            res.push(v as u8);
            return;
        }
            
    }
    
}
fn varint_length(mut val: u64) -> usize {
    let mut l=1;
    for _ in 0..10 {
        if val>0x7f {
            l+=1;
            val>>=7;
        } else {
            return l
        }
        
    }
    return 10;
}

pub fn pack_value(res: &mut Vec<u8>, key: u64, val: u64) {
        
    write_varint(res, key << 3);
    write_varint(res, val);
        
    
}

pub fn pack_data(res: &mut Vec<u8>, key: u64, data: &[u8]) {
    
    write_varint(res, (key << 3) | 2);
    write_varint(res, data.len() as u64);
    res.extend(data);

}

pub fn value_length(key: u64, val: u64) -> usize {
    varint_length(key<<3) + varint_length(val)
}

pub fn data_length(key: u64, l: usize) -> usize {
    varint_length(key<<3 | 2 ) + varint_length(l as u64) + l
}

pub fn pack_int(vals: impl Iterator<Item= u64>) -> Vec<u8> {
    let mut res = Vec::new();
    for v in vals {
        write_varint(&mut res, v);
    }
    res
}

pub fn pack_int_ref<'a>(vals: impl Iterator<Item=&'a u64>) -> Vec<u8> {
    let mut res = Vec::new();
    for v in vals {
        write_varint(&mut res, *v);
    }
    res
}

pub fn packed_int_length(vals: impl Iterator<Item= u64>) -> usize {
    let mut r=0;
    for v in vals {
        r += varint_length(v);
    }
    r
}
pub fn packed_int_ref_length<'a>(vals: impl Iterator<Item=&'a u64>) -> usize {
    let mut r=0;
    for v in vals {
        r += varint_length(*v);
    }
    r
}

pub fn pack_delta_int_ref<'a>(vals: impl Iterator<Item = &'a i64>) -> Vec<u8>{
    
    let mut res = Vec::new();
    
    let mut curr = 0;
    for v in vals {
        write_varint(&mut res, zig_zag(*v - curr));
        curr = *v;
    }
    res
}
        
pub fn pack_delta_int(vals: impl Iterator<Item = i64>) -> Vec<u8>{
    
    let mut res = Vec::new();
    
    let mut curr = 0;
    for v in vals {
        write_varint(&mut res, zig_zag(v - curr));
        curr = v;
    }
    res
}

pub fn packed_delta_int_length(vals: impl Iterator<Item= i64>) -> usize {
    let mut r=0;
    let mut curr = 0;
    for v in vals {
        r += varint_length(zig_zag(v-curr));
        curr = v;
    }
    r
}
pub fn packed_delta_int_ref_length<'a>(vals: impl Iterator<Item=&'a i64>) -> usize {
    let mut r=0;
    let mut curr = 0;
    for v in vals {
        r += varint_length(zig_zag(*v-curr));
        curr = *v;
    }
    r
}
