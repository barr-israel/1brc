use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

pub fn run() {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let reader = BufReader::new(file);
    let mut summary = HashMap::<String, (f32, f32, f32, i32)>::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let (station_name, measurement) = line.split_once(';').expect("invalid line");
        let measurement_value: f32 = measurement.parse().expect("not a number");
        summary
            .entry(station_name.into())
            .and_modify(|(min, sum, max, count)| {
                *min = min.min(measurement_value);
                *sum += measurement_value;
                *max = max.max(measurement_value);
                *count += 1;
            })
            .or_insert((measurement_value, measurement_value, measurement_value, 1));
    }
    let mut summary: Vec<(String, f32, f32, f32)> = summary
        .into_iter()
        .map(|(station_name, (min, sum, max, count))| (station_name, min, sum / count as f32, max))
        .collect();
    summary.sort_unstable_by(|m1, m2| m1.0.cmp(&m2.0));
    print!("{{");
    for (station_name, min, avg, max) in summary[..summary.len() - 1].iter() {
        print!("{station_name}={min:.1}/{avg:.1}/{max:.1}, ");
    }
    let (station_name, min, avg, max) = summary.last().unwrap();
    print!("{station_name}={min:.1}/{avg:.1}/{max:.1}}}");
}
