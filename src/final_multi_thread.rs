use std::io::{PipeWriter, Write};
use std::{fs::File, hash::Hash, io::Error, os::fd::AsRawFd, slice::from_raw_parts, str::FromStr};

use rayon::iter::{ParallelBridge, ParallelIterator};

#[allow(unused_imports)]
use std::arch::x86_64::{
    __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_mask_cmpneq_epu8_mask,
    _mm256_movemask_epi8, _mm256_set1_epi8, _pext_u32,
};

use memchr::memrchr;
use rustc_hash::FxHashMap;

#[allow(unused_imports)]
use memchr::memchr;

const MARGIN: usize = 32;

struct StationEntry {
    sum: i32,
    min: i32,
    max: i32,
    count: i32,
}
#[derive(Eq)]
struct StationName {
    ptr: *const u8,
    len: u8,
}

unsafe impl Send for StationName {}
unsafe impl Sync for StationName {}

impl StationName {
    #[cfg(all(target_feature = "avx512bw", target_feature = "avx512vl"))]
    #[target_feature(enable = "avx512bw,avx512vl")]
    fn eq_inner(&self, other: &Self) -> bool {
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

fn parse_measurement(text: &[u8]) -> i32 {
    static LUT: [i16; 1 << 16] = {
        let mut lut = [0; 1 << 16];
        let mut i = 0usize;
        while i < (1 << 16) {
            let digit0 = i as i16 & 0xf;
            let digit1 = (i >> 4) as i16 & 0xf;
            let digit2 = (i >> 8) as i16 & 0xf;
            let digit3 = (i >> 12) as i16 & 0xf;
            lut[i] = if digit1 == b'.' as i16 & 0xf {
                digit0 * 10 + digit2
            } else {
                digit0 * 100 + digit1 * 10 + digit3
            };
            i += 1;
        }
        lut
    };
    let negative = unsafe { *text.get_unchecked(0) } == b'-';
    let raw_key = unsafe { (text.as_ptr().add(negative as usize) as *const u32).read_unaligned() };
    let packed_key = unsafe { _pext_u32(raw_key, 0b00001111000011110000111100001111) };
    let abs_val = unsafe { *LUT.get_unchecked(packed_key as usize) } as i32;
    if negative { -abs_val } else { abs_val }
}

fn map_file(file: &File) -> Result<&[u8], Error> {
    let mapped_length = file.metadata().unwrap().len() as usize + MARGIN;
    match unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            mapped_length,
            libc::PROT_READ,
            libc::MAP_SHARED,
            file.as_raw_fd(),
            0,
        )
    } {
        libc::MAP_FAILED => Err(Error::last_os_error()),
        ptr => {
            unsafe { libc::madvise(ptr, mapped_length, libc::MADV_SEQUENTIAL) };
            Ok(unsafe { from_raw_parts(ptr as *const u8, mapped_length) })
        }
    }
}

#[target_feature(enable = "avx2")]
fn read_line(text: &[u8]) -> (&[u8], StationName, i32) {
    let separator: __m256i = _mm256_set1_epi8(b';' as i8);
    let line_break: __m256i = _mm256_set1_epi8(b'\n' as i8);
    let line: __m256i = unsafe { _mm256_loadu_si256(text.as_ptr() as *const __m256i) };
    let separator_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, separator));
    let line_break_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, line_break));
    let separator_pos = separator_mask.trailing_zeros() as usize;
    let line_break_pos = line_break_mask.trailing_zeros() as usize;
    (
        &text[line_break_pos + 1..],
        StationName {
            ptr: text.as_ptr(),
            len: separator_pos as u8,
        },
        parse_measurement(&text[separator_pos + 1..line_break_pos]),
    )
}

fn process_chunk(chunk: &[u8]) -> FxHashMap<StationName, StationEntry> {
    let mut summary =
        FxHashMap::<StationName, StationEntry>::with_capacity_and_hasher(1024, Default::default());
    let mut remainder = chunk;
    while (remainder.len() - MARGIN) != 0 {
        let station_name: StationName;
        let measurement: i32;
        (remainder, station_name, measurement) = unsafe { read_line(remainder) };
        summary
            .entry(station_name)
            .and_modify(|e| {
                if measurement < e.min {
                    e.min = measurement;
                }
                if measurement > e.max {
                    e.max = measurement;
                }
                e.sum += measurement;
                e.count += 1;
            })
            .or_insert(StationEntry {
                sum: measurement,
                min: measurement,
                max: measurement,
                count: 1,
            });
    }
    summary
}

fn merge_summaries(
    mut summary1: FxHashMap<StationName, StationEntry>,
    summary2: FxHashMap<StationName, StationEntry>,
) -> FxHashMap<StationName, StationEntry> {
    summary2
        .into_iter()
        .for_each(|(station_name, partial_entry)| {
            summary1
                .entry(station_name)
                .and_modify(|entry| {
                    if partial_entry.min < entry.min {
                        entry.min = partial_entry.min;
                    }
                    if partial_entry.max > entry.max {
                        entry.max = partial_entry.max;
                    }
                    entry.sum += partial_entry.sum;
                    entry.count += partial_entry.count;
                })
                .or_insert(partial_entry);
        });
    summary1
}

pub fn run(mut writer: PipeWriter) {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let mapped_file = map_file(&file).unwrap();
    let thread_count: usize = std::env::args()
        .nth(1)
        .expect("missing thread count")
        .parse()
        .expect("invalid thread count");
    rayon::ThreadPoolBuilder::new()
        .num_threads(thread_count)
        .build_global()
        .unwrap();
    let chunks_mult = 16;
    let chunks = thread_count * chunks_mult;
    let ideal_chunk_size = mapped_file.len() / chunks;
    let mut remainder = mapped_file;
    let summary: FxHashMap<StationName, StationEntry> = (0..chunks)
        .map(|_| {
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            chunk
        })
        .par_bridge()
        .map(process_chunk)
        .reduce(
            FxHashMap::<StationName, StationEntry>::default,
            merge_summaries,
        );
    let mut summary: Vec<(String, f32, f32, f32)> = summary
        .into_iter()
        .map(|(station_name, e)| {
            (
                station_name.into(),
                e.min as f32 / 10f32,
                e.sum as f32 / (e.count as f32 * 10f32),
                e.max as f32 / 10f32,
            )
        })
        .collect();
    summary.sort_unstable_by(|m1, m2| m1.0.cmp(&m2.0));
    let mut out = std::io::stdout().lock();
    let _ = out.write_all(b"{");
    for (station_name, min, avg, max) in summary[..summary.len() - 1].iter() {
        let _ = out.write_fmt(format_args!("{station_name}={min:.1}/{avg:.1}/{max:.1}, "));
    }
    let (station_name, min, avg, max) = summary.last().unwrap();
    let _ = out.write_fmt(format_args!("{station_name}={min:.1}/{avg:.1}/{max:.1}}}"));
    _ = out.flush();
    writer.write_all(&[0]).unwrap();
}
