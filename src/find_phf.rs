use std::{
    arch::x86_64::_pext_u64,
    hash::{Hash, Hasher},
};

use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rustc_hash::FxHasher;

use crate::station_names::STATION_NAMES;

const OFFSET: usize = 1;

fn get_name_sample(name: &[u8]) -> usize {
    let ptr = unsafe { name.as_ptr().add(OFFSET) } as *const u64;
    let sample = unsafe { ptr.read_unaligned() };
    let len_mask = if name.len() > 8 + OFFSET {
        !0
    } else {
        (1 << ((name.len() - OFFSET) * 8 - OFFSET)) - 1
    };
    (sample & len_mask) as usize
}

pub fn print_phf() {
    for name in STATION_NAMES {
        let sample = get_name_sample(name);
        println!("{}:{}", std::str::from_utf8(name).unwrap(), sample % 13779)
    }
}

pub fn find_seed_fxhash() {
    for log_size in 9..22 {
        println!("testing size {log_size}");
        (0..22).into_par_iter().for_each(|tid| {
            let mut vec = vec![false; 1 << (log_size)];
            for seed in (0..1_000_000).step_by(22) {
                for divisor in tid + 413..vec.len() {
                    let mut found = true;
                    for name in STATION_NAMES.iter() {
                        let sample = get_name_sample(name);
                        // let masked_sample = seed as u64 + unsafe { _pext_u64( masked_sample, 0b00011111_00011111_00011111_00011111_00011111_00011111_00011111_00011111) };
                        let mut hasher = FxHasher::with_seed(seed);
                        sample.hash(&mut hasher);
                        let hash = hasher.finish() as usize;
                        // let hash = masked_sample as usize;
                        let vec_index = hash % divisor;
                        // let bit_index = hash & 7;
                        if !vec[vec_index] {
                            vec[vec_index] = true;
                        } else {
                            // println!("failed at {} collided with \n{hash}\n{vec_index}", unsafe {
                            //     std::str::from_utf8_unchecked(name)
                            // },);
                            // unsafe { libc::exit(0) };
                            vec.fill(false);
                            found = false;
                            break;
                        }
                    }
                    if found {
                        println!("Seed Found: {seed}");
                        unsafe { libc::exit(0) };
                    }
                }
            }
        });
    }
    println!("Failed");
}
pub fn find_seed() {
    const OFFSET: usize = 1;
    (0..22).into_par_iter().for_each(|tid| {
        let mut vec = vec![false; 1 << 20];
        for divisor in tid + 413..vec.len() {
            let mut found = true;
            for name in STATION_NAMES.iter() {
                let ptr = unsafe { name.as_ptr().add(OFFSET) } as *const u64;
                let sample = unsafe { ptr.read_unaligned() };
                let len_mask = if name.len() > 8 + OFFSET {
                    !0
                } else {
                    (1 << ((name.len() - OFFSET) * 8 - OFFSET)) - 1
                };
                let masked_sample = sample & len_mask;
                // let masked_sample = unsafe {
                //     _pext_u64(
                //         masked_sample,
                //         0b00011111_00011111_00011111_00011111_00011111_00011111_00011111_00011111,
                //     )
                // };
                let vec_index = masked_sample as usize % divisor;
                // let bit_index = hash & 7;
                if !vec[vec_index] {
                    vec[vec_index] = true;
                } else {
                    // println!("failed at {} collided with \n{hash}\n{vec_index}", unsafe {
                    //     std::str::from_utf8_unchecked(name)
                    // },);
                    // unsafe { libc::exit(0) };
                    vec.fill(false);
                    found = false;
                    break;
                }
            }
            if found {
                println!("Divisor Found: {divisor}");
                unsafe { libc::exit(0) };
            }
        }
    });
    println!("Failed");
}
