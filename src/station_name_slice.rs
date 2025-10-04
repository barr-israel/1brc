use std::{fs::File, hash::Hash, io::Error, os::fd::AsRawFd, slice::from_raw_parts, str::FromStr};

use memchr::memchr;
use rustc_hash::FxHashMap;

#[derive(Eq, PartialEq)]
struct StationName<'a>(&'a [u8]);

impl<'a> Hash for StationName<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // if self.0.len() < 4 {
        //     self.0.hash(state);
        // } else {
        let ptr = self.0.as_ptr() as *const u32;
        unsafe { ptr.read_unaligned() }.hash(state);
        // }
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
    let mapped_length = file.metadata().unwrap().len() as usize;
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

pub fn run() {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let mut summary = FxHashMap::<StationName, (i32, i32, i32, i32)>::default();
    let mapped_file = map_file(&file).unwrap();
    let mut remainder = mapped_file;
    while !remainder.is_empty() {
        let station_name_slice: &[u8];
        let measurement_slice: &[u8];
        (station_name_slice, remainder) = remainder.split_at(memchr(b';', remainder).unwrap() + 1);
        (measurement_slice, remainder) = remainder.split_at(memchr(b'\n', remainder).unwrap());
        remainder = &remainder[1..]; //skip \n;
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
