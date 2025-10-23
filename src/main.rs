use std::io::Read;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod find_phf;
mod my_hashmap;
mod my_phf;
mod station_names;
mod use_phf;

fn main() {
    // find_phf::print_phf();
    let (mut reader, writer) = std::io::pipe().unwrap();
    if unsafe { libc::fork() } == 0 {
        use_phf::run(writer);
    } else {
        _ = reader.read_exact(&mut [0u8]);
    }
}
