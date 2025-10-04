use std::{fs::File, hash::Hash, io::Error, os::fd::AsRawFd, slice::from_raw_parts, str::FromStr};

use rustc_hash::FxHashMap;

#[allow(unused_imports)]
use memchr::memchr;

#[derive(Eq, PartialEq)]
struct StationName<'a>(&'a [u8]);

impl<'a> Hash for StationName<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let ptr = self.0.as_ptr() as *const u32;
        unsafe { ptr.read_unaligned() }.hash(state);
    }
}
impl<'a> From<StationName<'a>> for String {
    fn from(val: StationName) -> Self {
        String::from_str(std::str::from_utf8(&val.0[..val.0.len() - 1]).unwrap()).unwrap()
    }
}

fn parse_measurement(text: &[u8]) -> i32 {
    if text[0] == b'-' {
        -parse_measurement_pos(&text[1..])
    } else {
        parse_measurement_pos(text)
    }
}
fn parse_measurement_pos(text: &[u8]) -> i32 {
    if text[1] == b'.' {
        // 1 digit number
        (text[0] - b'0') as i32 * 10 + (text[2] - b'0') as i32
    } else {
        // 2 digit number
        (text[0] - b'0') as i32 * 100 + (text[1] - b'0') as i32 * 10 + (text[3] - b'0') as i32
    }
}

fn map_file(file: &File) -> Result<&[u8], Error> {
    let mapped_length = file.metadata().unwrap().len() as usize + 32;
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
fn read_line(text: &[u8]) -> (&[u8], &[u8], &[u8]) {
    use std::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_movemask_epi8, _mm256_set1_epi8};

    let separator: __m256i = _mm256_set1_epi8(b';' as i8);
    let line_break: __m256i = _mm256_set1_epi8(b'\n' as i8);
    let line: __m256i = unsafe {
        use std::arch::x86_64::_mm256_loadu_si256;
        _mm256_loadu_si256(text.as_ptr() as *const __m256i)
    };
    let separator_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, separator));
    let line_break_mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(line, line_break));
    let separator_pos = separator_mask.trailing_zeros() as usize;
    let line_break_pos = line_break_mask.trailing_zeros() as usize;
    (
        &text[line_break_pos + 1..],
        &text[..separator_pos + 1],
        &text[separator_pos + 1..line_break_pos],
    )
}

#[cfg(not(target_feature = "avx2"))]
fn read_line(mut text: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let station_name_slice: &[u8];
    let measurement_slice: &[u8];
    (station_name_slice, text) = text.split_at(memchr(b';', &text[3..]).unwrap() + 3);
    text = &text[1..]; //skip ';';
    (measurement_slice, text) = text.split_at(memchr(b'\n', &text[3..]).unwrap() + 3);
    text = &text[1..]; //skip \n;
    (text, station_name_slice, measurement_slice)
}

pub fn run() {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let mut summary = FxHashMap::<StationName, (i32, i32, i32, i32)>::default();
    let mapped_file = map_file(&file).unwrap();
    let mut remainder = mapped_file;
    while (remainder.len() - 32) != 0 {
        let station_name_slice: &[u8];
        let measurement_slice: &[u8];
        (remainder, station_name_slice, measurement_slice) = unsafe { read_line(remainder) };
        let measurement_value = parse_measurement(measurement_slice);
        summary
            .entry(StationName(station_name_slice))
            .and_modify(|(min, sum, max, count)| {
                *min = (*min).min(measurement_value);
                *sum += measurement_value;
                *max = (*max).max(measurement_value);
                *count += 1;
            })
            .or_insert((measurement_value, measurement_value, measurement_value, 1));
    }
    let mut summary: Vec<(String, f32, f32, f32)> = summary
        .into_iter()
        .map(|(station_name, (min, sum, max, count))| {
            (
                station_name.into(),
                min as f32 / 10f32,
                sum as f32 / (count as f32 * 10f32),
                max as f32 / 10f32,
            )
        })
        .collect();
    summary.sort_unstable_by(|m1, m2| m1.0.cmp(&m2.0));
    print!("{{");
    for (station_name, min, avg, max) in summary[..summary.len() - 1].iter() {
        print!("{station_name}={min:.1}/{avg:.1}/{max:.1}, ");
    }
    let (station_name, min, avg, max) = summary.last().unwrap();
    print!("{station_name}={min:.1}/{avg:.1}/{max:.1}}}");
}
