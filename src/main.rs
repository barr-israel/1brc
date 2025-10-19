#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod final_single_thread;

fn main() {
    final_single_thread::run();
}
