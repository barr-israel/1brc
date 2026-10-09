use std::hint::{assert_unchecked, select_unpredictable};
use std::io::{PipeWriter, Write};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;
use std::{fs::File, io::Error, os::fd::AsRawFd, slice::from_raw_parts};

#[allow(unused_imports)]
use std::arch::x86_64::{
    __m256i, _MM_HINT_T0, _bzhi_u32, _mm_prefetch, _mm256_cmpeq_epi8, _mm256_loadu_si256,
    _mm256_mask_cmpneq_epu8_mask, _mm256_movemask_epi8, _mm256_set1_epi8, _pext_u32,
};
use std::hint::cold_path;

use llvm_mca::{llvm_mca_begin, llvm_mca_end};
use memchr::memrchr;

#[allow(unused_imports)]
use memchr::memchr;

use crate::my_phf2::MyPHFMap;

const LINES_PER_BATCH: usize = 256;
#[cfg(all(target_feature = "avx2", not(target_feature = "avx512bw")))]
const SIMD_SIZE: usize = 32;
#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512bw",
    target_feature = "avx512vbmi2"
))]
const SIMD_SIZE: usize = 64;
// oversized so we can finish the current iterations without worrying about overflow
const BATCH_BUFFER_SIZE: usize = LINES_PER_BATCH + SIMD_SIZE;

fn parse_measurement_from_end(text: &[u8]) -> (usize, i32) {
    static LUT: [i16; 1 << 12] = {
        let mut lut = [0; 1 << 12];
        let mut i = 0usize;
        while i < (1 << 12) {
            let tens = i as i16 & 0xf;
            let ones = (i >> 4) as i16 & 0xf;
            let tenths = (i >> 8) as i16 & 0xf;
            const SEPARATOR: i16 = (b';' & 0xf) as i16; // 0xB
            const MINUS: i16 = (b'-' & 0xf) as i16; // 0xD
            lut[i] = match tens {
                SEPARATOR => ones * 10 + tenths,
                MINUS => -(ones * 10 + tenths),
                _ => tens * 100 + ones * 10 + tenths,
            };
            i += 1;
        }
        lut
    };
    // SAFETY: minimal line is longer than ";d.d\n"
    unsafe { assert_unchecked(text.len() >= 5) };
    // contains at least the entire measurement
    let raw_key = unsafe { (text.as_ptr().add(text.len() - 4) as *const u32).read_unaligned() };
    // 2nd to last byte is always the decimal separator, ignore it
    let key = unsafe { _pext_u32(raw_key, 0x0F000F0F) };
    // SAFETY: all possible values are within the LUT size
    let value = unsafe { *LUT.get_unchecked(key as usize) };
    let before = text[text.len() - 5];
    let first = raw_key as u8;
    // if the measurement is -dd.d, the LUT does not capture the minus
    let negate = before == b'-';
    let measurement = select_unpredictable(negate, -value, value);
    // this formula covers all 4 formats
    let seperator_position = text.len() - (4 + (first != b';') as usize + negate as usize);
    (seperator_position, measurement as i32)
}

fn map_file(file: &File) -> Result<&[u8], Error> {
    let mapped_length = file.metadata().unwrap().len() as usize + SIMD_SIZE;
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

#[cfg(all(
    target_feature = "avx512f",
    target_feature = "avx512bw",
    target_feature = "avx512vbmi2"
))]
#[target_feature(enable = "avx512f,avx512bw,avx512vbmi2")]
fn fill_batch(text: &[u8], batch_buffer: &mut [usize; BATCH_BUFFER_SIZE]) -> (usize, usize) {
    use std::{
        arch::x86_64::{__m512i, _mm512_loadu_si512},
        array::from_fn,
    };
    const IOTA: [u8; 64] = {
        let mut arr = [0u8; 64];
        let mut i = 0;
        while i < 64 {
            arr[i] = i as u8;
            i += 1
        }
        arr
    };
    let iota_vec = unsafe { _mm512_loadu_si512(IOTA.as_ptr() as *const __m512i) };
    let mut offset = 0usize;
    let mut remaining = text.len();
    let mut batch_size = 0;
    while batch_size <= LINES_PER_BATCH {
        use std::arch::x86_64::{
            _bzhi_u64, _mm512_add_epi64, _mm512_castsi512_si128, _mm512_cmpeq_epi8_mask,
            _mm512_cvtepu8_epi64, _mm512_mask_compress_epi8, _mm512_maskz_compress_epi8,
            _mm512_set1_epi8, _mm512_set1_epi64, _mm512_storeu_si512,
        };

        _mm_prefetch::<_MM_HINT_T0>(unsafe { text.as_ptr().add(offset + 4096) } as *const i8);
        if remaining <= SIMD_SIZE {
            let consumed = batch_buffer[batch_size - 1] + 1;
            return (batch_size, consumed);
        }

        // read the next byte vector
        let byte_vec: __m512i =
            unsafe { _mm512_loadu_si512(text.as_ptr().add(offset) as *const __m512i) };
        let line_breaks: __m512i = _mm512_set1_epi8(b'\n' as i8);
        let mut line_breaks_mask = _mm512_cmpeq_epi8_mask(byte_vec, line_breaks);
        if remaining <= SIMD_SIZE * 2 {
            cold_path(); // only happens at the end of the batch
            // mask bytes that belong to the next chunk
            line_breaks_mask =
                unsafe { _bzhi_u64(line_breaks_mask, (remaining - SIMD_SIZE) as u32) };
        }
        let mut line_breaks_positions = _mm512_maskz_compress_epi8(line_breaks_mask, iota_vec);
        let line_breaks_offsets = _mm512_add_epi64(
            _mm512_cvtepu8_epi64(_mm512_castsi512_si128(line_breaks_positions)), // 8 × u8 → 8 × u64
            _mm512_set1_epi64(offset as i64),
        );
        unsafe {
            _mm512_storeu_si512(
                batch_buffer.as_mut_ptr().add(batch_size) as *mut _,
                line_breaks_offsets,
            )
        };
        let found = line_breaks_mask.count_ones() as usize;
        debug_assert!(found <= 8);
        batch_size += found;
        offset += SIMD_SIZE;
        remaining -= SIMD_SIZE;
    }
    let consumed = batch_buffer[batch_size - 1] + 1;
    (batch_size, consumed)
}

#[cfg(all(target_feature = "avx2", not(target_feature = "avx512bw")))]
#[target_feature(enable = "avx2")]
fn fill_batch(text: &[u8], batch_buffer: &mut [usize; BATCH_BUFFER_SIZE]) -> (usize, usize) {
    let mut offset = 0usize;
    let mut remaining = text.len();
    let mut batch_size = 0;
    while batch_size <= LINES_PER_BATCH {
        _mm_prefetch::<_MM_HINT_T0>(unsafe { text.as_ptr().add(offset + 4096) } as *const i8);
        if remaining <= SIMD_SIZE {
            let consumed = batch_buffer[batch_size - 1] + 1;
            return (batch_size, consumed);
        }

        // read the next byte vector
        let byte_vec: __m256i =
            unsafe { _mm256_loadu_si256(text.as_ptr().add(offset) as *const __m256i) };
        let line_breaks: __m256i = _mm256_set1_epi8(b'\n' as i8);
        let mut line_breaks_mask =
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(byte_vec, line_breaks)) as u32;
        if remaining <= SIMD_SIZE * 2 {
            cold_path(); // only happens at the end of the batch
            // mask bytes that belong to the next chunk
            line_breaks_mask =
                unsafe { _bzhi_u32(line_breaks_mask, (remaining - SIMD_SIZE) as u32) };
        }
        let found = line_breaks_mask.count_ones() as usize;
        debug_assert!(found <= 4, "more than 4 line ends in one 32-byte lane");
        // process up to 4 lines in each iteration, its impossible to have more than 4
        for i in 0..4 {
            batch_buffer[batch_size + i] = offset + line_breaks_mask.trailing_zeros() as usize;
            line_breaks_mask &= line_breaks_mask.wrapping_sub(1); // should compile to a single blsrl instruction
        }
        batch_size += found;
        offset += SIMD_SIZE;
        remaining -= SIMD_SIZE;
    }
    let consumed = batch_buffer[batch_size - 1] + 1;
    (batch_size, consumed)
}
fn process_batch(
    text: &[u8],
    batch: &[usize; BATCH_BUFFER_SIZE],
    batch_size: usize,
    summary: &mut MyPHFMap,
) {
    let mut line_start = 0usize;
    for line_end in &batch[..batch_size] {
        // SAFETY: we already parsed this line, we know where it ends
        let line = unsafe { text.get_unchecked(..*line_end) };
        let (name_end, measurement) = parse_measurement_from_end(line);
        // SAFETY: the range is always valid because it was derived from text
        let name = unsafe { text.get_unchecked(line_start..name_end) };
        summary.insert_measurement(name, measurement);
        line_start = *line_end + 1;
    }
}

fn process_chunk(chunk: &[u8], summary: &mut MyPHFMap) {
    let mut batch = [0usize; BATCH_BUFFER_SIZE];
    let mut remainder = chunk;
    while remainder.len() != SIMD_SIZE {
        llvm_mca_begin!("fill");
        let (batch_size, consumed) = unsafe { fill_batch(remainder, &mut batch) };
        llvm_mca_end!("fill");
        llvm_mca_begin!("process");
        process_batch(remainder, &batch, batch_size, summary);
        llvm_mca_end!("process");
        remainder = &remainder[consumed..];
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
    const CHUNKS_PER_THREAD: usize = 64;
    let chunk_count = thread_count * CHUNKS_PER_THREAD;
    let ideal_chunk_size = mapped_file.len() / chunk_count;
    let mut chunks = Vec::with_capacity(chunk_count);
    for _ in 0..(chunk_count - 1) {
        let chunk_end = memrchr(b'\n', &remainder[..ideal_chunk_size]).unwrap();
        let chunk: &[u8] = &remainder[..chunk_end + SIMD_SIZE + 1];
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
                    while let Some(my_chunk) = chunks.get(claimed_chunks.fetch_add(1, Relaxed)) {
                        process_chunk(my_chunk, &mut thread_summary);
                    }
                    thread_summary
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
