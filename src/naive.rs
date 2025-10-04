use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
};

pub fn run() {
    let file = File::open("measurements.txt").expect("measurements.txt file not found");
    let reader = BufReader::new(file);
    let mut map = HashMap::<String, Vec<f32>>::new();
    for line in reader.lines() {
        let line = line.unwrap();
        let (station_name, measurement) = line.split_once(';').expect("invalid line");
        let measurement_value: f32 = measurement.parse().expect("not a number");
        map.entry(station_name.into())
            .or_default()
            .push(measurement_value);
    }
    let mut summary: Vec<(&String, f32, f32, f32)> = map
        .iter()
        .map(|(station_name, measurements)| {
            let min = *measurements.iter().min_by(|a, b| a.total_cmp(b)).unwrap();
            let avg = measurements.iter().sum::<f32>() / measurements.len() as f32;
            let max = *measurements.iter().max_by(|a, b| a.total_cmp(b)).unwrap();
            (station_name, min, avg, max)
        })
        .collect();
    summary.sort_unstable_by_key(|m| m.0);
    print!("{{");
    for (station_name, min, avg, max) in summary[..summary.len() - 1].iter() {
        print!("{station_name}={min:.1}/{avg:.1}/{max:.1}, ");
    }
    let (station_name, min, avg, max) = summary.last().unwrap();
    print!("{station_name}={min:.1}/{avg:.1}/{max:.1}}}");
}
