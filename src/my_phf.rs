use std::{
    arch::x86_64::{_MM_HINT_ET0, _mm_prefetch},
    io::Write,
    mem::{MaybeUninit, transmute},
};

use crate::{gperf, station_names::STATION_NAMES};

const SIZE: usize = 13779;

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

// pub fn get_name_index(name: &[u8]) -> usize {
//     const OFFSET: usize = 1;
//     let ptr = unsafe { name.as_ptr().add(OFFSET) } as *const u64;
//     let mut sample = unsafe { ptr.read_unaligned() };
//     let len = (name.len() - 1).min(8);
//     let to_mask = len * 8;
//     let mask = u64::MAX >> (64 - to_mask);
//     sample &= mask;
//     sample as usize % SIZE
// }

pub fn get_name_index(name: &[u8]) -> usize {
    gperf::hash(name, name.len())
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

    pub fn prefetch(&self, name_index: usize) {
        unsafe { _mm_prefetch::<_MM_HINT_ET0>(self.entries.as_ptr().add(name_index) as *const i8) };
    }

    pub fn insert_measurement(&mut self, name: &[u8], measurement: i32) {
        self.insert_measurement_by_index(get_name_index(name), measurement);
    }
    pub fn insert_measurement_by_index(&mut self, name_index: usize, measurement: i32) {
        let entry = unsafe { self.entries.get_unchecked_mut(name_index) };
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
