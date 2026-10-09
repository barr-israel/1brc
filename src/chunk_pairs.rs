use std::io::{PipeWriter, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::{fs::File, io::Error, os::fd::AsRawFd, slice::from_raw_parts};

#[allow(unused_imports)]
use std::arch::x86_64::{
    __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_mask_cmpneq_epu8_mask,
    _mm256_movemask_epi8, _mm256_set1_epi8, _pext_u32,
};

use llvm_mca::{llvm_mca_begin, llvm_mca_end};
use memchr::memrchr;

#[allow(unused_imports)]
use memchr::memchr;

use crate::my_phf2::MyPHFMap;

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
    let line_break: __m256i = _mm256_set1_epi8(b'\n' as i8);
    let separator: __m256i = _mm256_set1_epi8(b';' as i8);
    let line: __m256i = unsafe { _mm256_loadu_si256(text.as_ptr() as *const __m256i) };
    let line_break_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, line_break));
    let separator_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, separator));
    let line_break_pos = line_break_mask.trailing_zeros() as usize;
    let separator_pos = separator_mask.trailing_zeros() as usize;
    unsafe {
        (
            text.get_unchecked(line_break_pos + 1..),
            text.get_unchecked(..separator_pos),
            parse_measurement(&text[separator_pos + 1..line_break_pos]),
        )
    }
}

fn process_chunk_pair(chunk1: &[u8], chunk2: &[u8], summary: &mut MyPHFMap) {
    let mut remainder1 = chunk1;
    let mut remainder2 = chunk2;
    // process a line from each chunk in parallel to increase ILP
    while remainder1.len() != MARGIN && remainder2.len() != MARGIN {
        llvm_mca_begin!("processing");
        let station_name1: &[u8];
        let measurement1: i32;
        let station_name2: &[u8];
        let measurement2: i32;
        (remainder1, station_name1, measurement1) = unsafe { read_line(remainder1) };
        (remainder2, station_name2, measurement2) = unsafe { read_line(remainder2) };
        summary.insert_measurement(station_name1, measurement1);
        summary.insert_measurement(station_name2, measurement2);
        llvm_mca_end!("processing");
    }
    // handle the tail of the chunk that has more lines
    let mut remainder = match (remainder1.len() != MARGIN, remainder2.len() != MARGIN) {
        (true, false) => remainder1,
        (false, true) => remainder2,
        (false, false) => return,
        (true, true) => unreachable!(),
    };
    while remainder.len() != MARGIN {
        llvm_mca_begin!("tail");
        let station_name: &[u8];
        let measurement: i32;
        (remainder, station_name, measurement) = unsafe { read_line(remainder) };
        summary.insert_measurement(station_name, measurement);
    }
}

pub fn run(mut writer: PipeWriter) {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let mapped_file = map_file(&file).unwrap();
    let mut remainder = mapped_file;
    let thread_count: usize = std::env::args()
        .nth(1)
        .expect("missing thread count")
        .parse()
        .expect("invalid thread count");

    // split the file into chunks
    const CHUNKS_PER_THREAD: usize = 128;
    // chunk pairing later requires an even amount of chunks
    const _: () = assert!(CHUNKS_PER_THREAD.is_multiple_of(2));
    let chunk_count = thread_count * CHUNKS_PER_THREAD;
    let ideal_chunk_size = mapped_file.len() / chunk_count;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..(chunk_count - 1) {
        let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
        let chunk: &[u8] = &remainder[..chunk_end + MARGIN + 1];
        remainder = &remainder[chunk_end + 1..];
        chunks.push(chunk);
    }
    // the remainder in the last chunk
    chunks.push(remainder);

    // spawn threads and let each claim chunks until they are all processed
    let claimed_chunks = AtomicUsize::new(0);
    let final_summary = std::thread::scope(|scope| {
        let thread_handles: Vec<_> = (0..thread_count)
            .map(|_| {
                scope.spawn(|| {
                    let mut thread_summary = MyPHFMap::new();
                    loop {
                        // 2 chunks at a time for better ILP
                        let chunk_pair_first = claimed_chunks.fetch_add(2, Relaxed);
                        if chunk_pair_first >= chunks.len() {
                            return thread_summary;
                        }
                        process_chunk_pair(
                            chunks[chunk_pair_first],
                            chunks[chunk_pair_first + 1],
                            &mut thread_summary,
                        );
                    }
                })
            })
            .collect();
        let mut summary = MyPHFMap::new();
        for handle in thread_handles {
            summary.merge_maps(handle.join().unwrap());
        }
        summary
    });
    final_summary.print_results();
    writer.write_all(&[0]).unwrap();
}
