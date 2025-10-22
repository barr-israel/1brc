use std::io::Read;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod my_hashmap;
mod use_custom_hashmap;

fn main() {
    let (mut reader, writer) = std::io::pipe().unwrap();
    if unsafe { libc::fork() } == 0 {
        use_custom_hashmap::run(writer);
    } else {
        _ = reader.read_exact(&mut [0u8]);
    }
}
