use std::io::Read;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod final_multi_thread;

fn main() {
    let (mut reader, writer) = std::io::pipe().unwrap();
    if unsafe { libc::fork() } == 0 {
        final_multi_thread::run(writer);
    } else {
        _ = reader.read_exact(&mut [0u8]);
    }
}
