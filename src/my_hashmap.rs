use std::{
    arch::x86_64::{__m256i, _mm256_loadu_si256},
    hash::{Hash, Hasher},
    mem::{MaybeUninit, transmute},
    ptr::null,
    slice::from_raw_parts,
    str::FromStr,
};

use rustc_hash::FxHasher;

const LOG_SIZE: usize = 14; // 16K entries, must support at least 10,000
const SIZE: usize = 1 << LOG_SIZE;
const MASK: usize = SIZE - 1;

pub struct StationEntry {
    pub sum: i32,
    pub min: i32,
    pub max: i32,
    pub count: i32,
}
#[derive(Eq, Copy, Clone)]
pub struct StationName {
    pub ptr: *const u8,
    pub len: u8,
}

unsafe impl Send for StationName {}
unsafe impl Sync for StationName {}

impl StationName {
    #[cfg(all(target_feature = "avx512bw", target_feature = "avx512vl"))]
    #[target_feature(enable = "avx512bw,avx512vl")]
    fn eq_inner(&self, other: &Self) -> bool {
        use std::arch::x86_64::_mm256_mask_cmpneq_epu8_mask;

        if self.len != other.len {
            return false;
        }
        let s = unsafe { _mm256_loadu_si256(self.ptr as *const __m256i) };
        let o = unsafe { _mm256_loadu_si256(other.ptr as *const __m256i) };
        let mask = (1 << self.len.max(other.len)) - 1;
        let diff = _mm256_mask_cmpneq_epu8_mask(mask, s, o);
        diff == 0
    }
    #[cfg(all(target_feature = "avx2", not(target_feature = "avx512bw")))]
    #[target_feature(enable = "avx2")]
    fn eq_inner(&self, other: &Self) -> bool {
        use std::arch::x86_64::{_mm256_cmpeq_epi8, _mm256_movemask_epi8};

        if self.len != other.len {
            return false;
        }
        let s = unsafe { _mm256_loadu_si256(self.ptr as *const __m256i) };
        let o = unsafe { _mm256_loadu_si256(other.ptr as *const __m256i) };
        let mask = (1 << self.len) - 1;
        let diff = _mm256_movemask_epi8(_mm256_cmpeq_epi8(s, o)) as u32;
        diff & mask == mask
    }
    #[cfg(not(target_feature = "avx2"))]
    fn eq_inner(&self, other: &Self) -> bool {
        let self_slice = unsafe { from_raw_parts(self.ptr, self.len as usize) };
        let other_slice = unsafe { from_raw_parts(other.ptr, other.len as usize) };
        self_slice == other_slice
    }
}
impl PartialEq for StationName {
    fn eq(&self, other: &Self) -> bool {
        unsafe { self.eq_inner(other) }
    }
}

impl Hash for StationName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let ptr = self.ptr as *const u32;
        unsafe { ptr.read_unaligned() }.hash(state);
    }
}
impl From<StationName> for String {
    fn from(StationName { ptr, len }: StationName) -> Self {
        let slice = unsafe { from_raw_parts(ptr, len as usize) };
        String::from_str(std::str::from_utf8(slice).unwrap()).unwrap()
    }
}

pub struct MyHashMap {
    names: Box<[StationName; SIZE]>,
    entries: Box<[StationEntry; SIZE]>,
}

impl MyHashMap {
    pub fn new() -> MyHashMap {
        let mut names;
        let mut entries;
        unsafe {
            names = Box::<[MaybeUninit<StationName>; SIZE]>::new_uninit().assume_init();
            entries = Box::<[MaybeUninit<StationEntry>; SIZE]>::new_uninit().assume_init();
        }
        for name in names.iter_mut() {
            name.write(StationName {
                ptr: null(),
                len: 0,
            });
        }
        for entry in entries.iter_mut() {
            entry.write(StationEntry {
                min: 1000,
                max: -1000,
                sum: 0,
                count: 0,
            });
        }
        MyHashMap {
            names: unsafe {
                transmute::<Box<[MaybeUninit<StationName>; SIZE]>, Box<[StationName; SIZE]>>(names)
            },
            entries: unsafe {
                transmute::<Box<[MaybeUninit<StationEntry>; SIZE]>, Box<[StationEntry; SIZE]>>(
                    entries,
                )
            },
        }
    }

    pub fn insert_measurement(&mut self, name: StationName, measurement: i32) {
        let mut hasher = FxHasher::default();
        name.hash(&mut hasher);
        let mut hash = hasher.finish() as usize;
        let entry = unsafe {
            loop {
                let index = hash & MASK;
                let potential_name = self.names.get_unchecked_mut(index);
                if potential_name.ptr.is_null() {
                    *potential_name = name;
                    break self.entries.get_unchecked_mut(index);
                }
                if *potential_name == name {
                    break self.entries.get_unchecked_mut(index);
                }
                hash = hash.wrapping_add(1);
            }
        };
        entry.sum += measurement;
        entry.count += 1;
        if measurement > entry.max {
            entry.max = measurement;
        }
        if measurement < entry.min {
            entry.min = measurement;
        }
    }

    pub fn merge_entry(&mut self, name: &StationName, other_entry: &StationEntry) {
        let mut hasher = FxHasher::default();
        name.hash(&mut hasher);
        let mut hash = hasher.finish() as usize;
        let entry = unsafe {
            loop {
                let index = hash & MASK;
                let potential_name = self.names.get_unchecked_mut(index & MASK);
                if potential_name.ptr.is_null() {
                    *potential_name = *name;
                    break self.entries.get_unchecked_mut(index);
                }
                if *potential_name == *name {
                    break self.entries.get_unchecked_mut(index);
                }
                hash = hash.wrapping_add(1);
            }
        };
        entry.sum += other_entry.sum;
        entry.count += other_entry.count;
        entry.max = entry.max.max(other_entry.max);
        entry.min = entry.min.min(other_entry.min);
    }

    pub fn iter(&self) -> impl Iterator<Item = (&StationName, &StationEntry)> {
        self.names
            .iter()
            .zip(self.entries.iter())
            .filter(|(name, _)| !name.ptr.is_null())
    }
}
