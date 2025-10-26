use std::io::{PipeWriter, Write};
use std::{fs::File, io::Error, os::fd::AsRawFd, slice::from_raw_parts};

use rayon::iter::{ParallelBridge, ParallelIterator};

#[allow(unused_imports)]
use std::arch::x86_64::{
    __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_mask_cmpneq_epu8_mask,
    _mm256_movemask_epi8, _mm256_set1_epi8, _pext_u32,
};

use memchr::memrchr;

#[allow(unused_imports)]
use memchr::memchr;

use crate::my_phf::{MyPHFMap, get_name_index};

const MARGIN: usize = 32;

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

#[cfg(target_feature = "avx2")]
#[target_feature(enable = "avx2")]
fn read_line(text: &[u8]) -> (&[u8], &[u8], i32) {
    let separator: __m256i = _mm256_set1_epi8(b';' as i8);
    let line_break: __m256i = _mm256_set1_epi8(b'\n' as i8);
    let line: __m256i = unsafe { _mm256_loadu_si256(text.as_ptr() as *const __m256i) };
    let separator_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, separator));
    let line_break_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, line_break));
    let separator_pos = separator_mask.trailing_zeros() as usize;
    let line_break_pos = line_break_mask.trailing_zeros() as usize;
    unsafe {
        (
            text.get_unchecked(line_break_pos + 1..),
            text.get_unchecked(..separator_pos),
            parse_measurement(&text[separator_pos + 1..line_break_pos]),
        )
    }
}

fn process_chunk(chunk: &[u8]) -> MyPHFMap {
    let mut summary = MyPHFMap::new();
    let (mut remainder, station_name, mut measurement) = unsafe { read_line(chunk) };
    let mut index = get_name_index(station_name);
    summary.prefetch(index);
    while remainder.len() != MARGIN {
        let station_name: &[u8];
        let new_measurement: i32;
        (remainder, station_name, new_measurement) = unsafe { read_line(remainder) };
        let new_index = get_name_index(station_name);
        summary.prefetch(new_index);
        summary.insert_measurement_by_index(index, measurement);
        index = new_index;
        measurement = new_measurement;
    }
    summary
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
    let final_summary = (0..chunks)
        .map(|_| {
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            chunk
        })
        .par_bridge()
        .map(process_chunk)
        .reduce(MyPHFMap::new, |mut a, b| {
            a.merge_maps(b);
            a
        });
    final_summary.print_results();
    writer.write_all(&[0]).unwrap();
}
