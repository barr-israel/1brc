use std::{
    fs::File,
    hash::Hash,
    io::{BufRead, BufReader},
    str::FromStr,
};

#[derive(Eq, PartialEq)]
struct StationName([u8; 32]);

impl Hash for StationName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let ptr = self.0.as_ptr() as *const u64;
        unsafe { ptr.read_unaligned() }.hash(state);
    }
}
impl From<&[u8]> for StationName {
    fn from(source: &[u8]) -> Self {
        let mut s = Self([0; 32]);
        s.0[..source.len()].copy_from_slice(source);
        s
    }
}
impl From<StationName> for String {
    fn from(val: StationName) -> Self {
        String::from_str(std::str::from_utf8(&val.0).unwrap()).unwrap()
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

pub fn run() {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let mut reader = BufReader::new(file);
    let mut summary = rustc_hash::FxHashMap::<StationName, (i32, i32, i32, i32)>::default();
    let mut buffer = Vec::new();
    while reader.read_until(b'\n', &mut buffer).unwrap() != 0 {
        let first_possible_split = buffer.len() - 7;
        let split_pos = buffer[first_possible_split..]
            .iter()
            .position(|c| *c == b';')
            .unwrap()
            + first_possible_split;
        let (station_name, measurement_slice) = buffer.split_at(split_pos);
        let measurement_value = parse_measurement(&measurement_slice[1..]);
        summary
            .entry(station_name.into())
            .and_modify(|(min, sum, max, count)| {
                *min = (*min).min(measurement_value);
                *sum += measurement_value;
                *max = (*max).max(measurement_value);
                *count += 1;
            })
            .or_insert((measurement_value, measurement_value, measurement_value, 1));
        buffer.clear();
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
