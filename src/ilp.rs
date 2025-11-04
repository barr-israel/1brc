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

#[cfg(not(target_feature = "avx2"))]
fn read_line(mut text: &[u8]) -> (&[u8], StationName, i32) {
    let station_name_slice: &[u8];
    let measurement_slice: &[u8];
    (station_name_slice, text) = text.split_at(memchr(b';', &text[3..]).unwrap() + 3);
    text = &text[1..]; //skip ';';
    (measurement_slice, text) = text.split_at(memchr(b'\n', &text[3..]).unwrap() + 3);
    text = &text[1..]; //skip \n;
    (
        text,
        StationName {
            ptr: station_name_slice.as_ptr(),
            len: station_name_slice.len() as u8,
        },
        parse_measurement(measurement_slice),
    )
}

fn process_chunk((chunk, chunk2, chunk3, chunk4): (&[u8], &[u8], &[u8], &[u8])) -> MyPHFMap {
    let mut summary = MyPHFMap::new();
    let mut remainder = chunk;
    let mut remainder2 = chunk2;
    let mut remainder3 = chunk3;
    let mut remainder4 = chunk4;
    let separator: __m256i = unsafe { _mm256_set1_epi8(b';' as i8) };
    let line_break: __m256i = unsafe { _mm256_set1_epi8(b'\n' as i8) };
    while (remainder.len() != MARGIN)
        & (remainder2.len() != MARGIN)
        & (remainder3.len() != MARGIN)
        & (remainder4.len() != MARGIN)
    {
        let line: __m256i = unsafe { _mm256_loadu_si256(remainder.as_ptr() as *const __m256i) };
        let line2: __m256i = unsafe { _mm256_loadu_si256(remainder2.as_ptr() as *const __m256i) };
        let line3: __m256i = unsafe { _mm256_loadu_si256(remainder3.as_ptr() as *const __m256i) };
        let line4: __m256i = unsafe { _mm256_loadu_si256(remainder4.as_ptr() as *const __m256i) };

        let separator_mask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, separator)) };
        let separator_mask2 = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line2, separator)) };
        let separator_mask3 = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line3, separator)) };
        let separator_mask4 = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line4, separator)) };

        let line_break_mask = unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, line_break)) };
        let line_break_mask2 =
            unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line2, line_break)) };
        let line_break_mask3 =
            unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line3, line_break)) };
        let line_break_mask4 =
            unsafe { _mm256_movemask_epi8(_mm256_cmpeq_epi8(line4, line_break)) };

        let separator_pos = separator_mask.trailing_zeros() as usize;
        let separator_pos2 = separator_mask2.trailing_zeros() as usize;
        let separator_pos3 = separator_mask3.trailing_zeros() as usize;
        let separator_pos4 = separator_mask4.trailing_zeros() as usize;

        let line_break_pos = line_break_mask.trailing_zeros() as usize;
        let line_break_pos2 = line_break_mask2.trailing_zeros() as usize;
        let line_break_pos3 = line_break_mask3.trailing_zeros() as usize;
        let line_break_pos4 = line_break_mask4.trailing_zeros() as usize;

        let station_name = unsafe { remainder.get_unchecked(..separator_pos) };
        let station_name2 = unsafe { remainder2.get_unchecked(..separator_pos2) };
        let station_name3 = unsafe { remainder3.get_unchecked(..separator_pos3) };
        let station_name4 = unsafe { remainder4.get_unchecked(..separator_pos4) };

        let measurement = parse_measurement(&remainder[separator_pos + 1..line_break_pos]);
        let measurement2 = parse_measurement(&remainder2[separator_pos2 + 1..line_break_pos2]);
        let measurement3 = parse_measurement(&remainder3[separator_pos3 + 1..line_break_pos3]);
        let measurement4 = parse_measurement(&remainder4[separator_pos4 + 1..line_break_pos4]);

        remainder = unsafe { remainder.get_unchecked(line_break_pos + 1..) };
        remainder2 = unsafe { remainder2.get_unchecked(line_break_pos2 + 1..) };
        remainder3 = unsafe { remainder3.get_unchecked(line_break_pos3 + 1..) };
        remainder4 = unsafe { remainder4.get_unchecked(line_break_pos4 + 1..) };

        let index = get_name_index(station_name);
        let index2 = get_name_index(station_name2);
        let index3 = get_name_index(station_name3);
        let index4 = get_name_index(station_name4);

        summary.insert_measurement_by_index(index, measurement);
        summary.insert_measurement_by_index(index2, measurement2);
        summary.insert_measurement_by_index(index3, measurement3);
        summary.insert_measurement_by_index(index4, measurement4);
    }
    while remainder.len() != MARGIN {
        let station_name: &[u8];
        let measurement: i32;
        (remainder, station_name, measurement) = unsafe { read_line(remainder) };
        summary.insert_measurement(station_name, measurement);
    }
    while remainder2.len() != MARGIN {
        let station_name: &[u8];
        let measurement: i32;
        (remainder2, station_name, measurement) = unsafe { read_line(remainder2) };
        summary.insert_measurement(station_name, measurement);
    }
    while remainder3.len() != MARGIN {
        let station_name: &[u8];
        let measurement: i32;
        (remainder3, station_name, measurement) = unsafe { read_line(remainder3) };
        summary.insert_measurement(station_name, measurement);
    }
    while remainder4.len() != MARGIN {
        let station_name: &[u8];
        let measurement: i32;
        (remainder4, station_name, measurement) = unsafe { read_line(remainder4) };
        summary.insert_measurement(station_name, measurement);
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
    let ideal_chunk_size = mapped_file.len() / (chunks * 4);
    let mut remainder = mapped_file;
    let final_summary = (0..chunks)
        .map(|_| {
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk2: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk3: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
            let chunk4: &[u8] = &remainder[..chunk_end + MARGIN + 1];
            remainder = &remainder[chunk_end + 1..];
            (chunk, chunk2, chunk3, chunk4)
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
