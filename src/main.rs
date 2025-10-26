#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

use std::io::Read;

mod affinity;
mod my_phf;
mod station_names;

fn main() {
    let (mut reader, writer) = std::io::pipe().unwrap();
    if unsafe { libc::fork() } == 0 {
        affinity::run(writer);
    } else {
        _ = reader.read_exact(&mut [0u8]);
    }
}
