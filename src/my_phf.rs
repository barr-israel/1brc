use std::{
    hash::{Hash, Hasher},
    io::Write,
    mem::{MaybeUninit, transmute},
};

use rustc_hash::FxHasher;

use crate::station_names::STATION_NAMES;

const SIZE: usize = 7088;
const SEED: usize = 1339;

pub struct StationEntry {
    pub sum: i32,
    pub min: i32,
    pub max: i32,
    pub count: i32,
}

impl StationEntry {
    fn get_result(&self) -> (f32, f32, f32) {
        (
            self.min as f32 / 10f32,
            self.sum as f32 / (self.count as f32 * 10f32),
            self.max as f32 / 10f32,
        )
    }
}

fn get_name_index(name: &[u8]) -> usize {
    const OFFSET: usize = 1;
    let ptr = unsafe { name.as_ptr().add(OFFSET) } as *const u64;
    let mut sample = unsafe { ptr.read_unaligned() };
    let len = (name.len() - 1).min(8);
    let to_mask = len * 8;
    let mask = u64::MAX >> (64 - to_mask);
    sample &= mask;
    let mut hasher = FxHasher::with_seed(SEED);
    sample.hash(&mut hasher);
    let hash = hasher.finish() as usize;
    hash % SIZE
}

pub struct MyPHFMap {
    entries: Box<[StationEntry; SIZE]>,
}

impl MyPHFMap {
    pub fn new() -> MyPHFMap {
        let mut entries;
        unsafe {
            entries = Box::<[MaybeUninit<StationEntry>; SIZE]>::new_uninit().assume_init();
        }
        for entry in entries.iter_mut() {
            entry.write(StationEntry {
                min: 1000,
                max: -1000,
                sum: 0,
                count: 0,
            });
        }
        MyPHFMap {
            entries: unsafe {
                transmute::<Box<[MaybeUninit<StationEntry>; SIZE]>, Box<[StationEntry; SIZE]>>(
                    entries,
                )
            },
        }
    }

    pub fn insert_measurement(&mut self, name: &[u8], measurement: i32) {
        let index = get_name_index(name);
        let entry = unsafe { self.entries.get_unchecked_mut(index) };
        entry.sum += measurement;
        entry.count += 1;
        if measurement > entry.max {
            entry.max = measurement;
        }
        if measurement < entry.min {
            entry.min = measurement;
        }
    }

    pub fn merge_maps(&mut self, other_map: Self) {
        for (entry, other_entry) in self.entries.iter_mut().zip(other_map.entries.iter()) {
            if (entry.count != 0) | (other_entry.count != 0) {
                entry.sum += other_entry.sum;
                entry.count += other_entry.count;
                entry.max = entry.max.max(other_entry.max);
                entry.min = entry.min.min(other_entry.min);
            }
        }
    }

    pub fn print_results(self) {
        let mut out = std::io::stdout().lock();
        let _ = out.write_all(b"{");
        for station_name in STATION_NAMES[..STATION_NAMES.len() - 1].iter() {
            let name = unsafe { std::str::from_utf8_unchecked(station_name) };
            let index = get_name_index(station_name);
            let entry = unsafe { self.entries.get_unchecked(index) };
            if entry.count != 0 {
                let (min, avg, max) = entry.get_result();
                let _ = out.write_fmt(format_args!("{name}={min:.1}/{avg:.1}/{max:.1}, "));
            }
        }
        let station_name = STATION_NAMES[STATION_NAMES.len() - 1];
        let name = unsafe { std::str::from_utf8_unchecked(station_name) };
        let index = get_name_index(station_name);
        let entry = unsafe { self.entries.get_unchecked(index) };
        if entry.count != 0 {
            let (min, avg, max) = entry.get_result();
            let _ = out.write_fmt(format_args!("{name}={min:.1}/{avg:.1}/{max:.1}}}"));
        }
        _ = out.flush();
    }
}
