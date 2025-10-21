use std::io::Read;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod use_avx512;

fn main() {
    let (mut reader, writer) = std::io::pipe().unwrap();
    if unsafe { libc::fork() } == 0 {
        use_avx512::run(writer);
    } else {
        _ = reader.read_exact(&mut [0u8]);
    }
}
