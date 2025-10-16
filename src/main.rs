#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod private_hashmaps;

fn main() {
    private_hashmaps::run();
}
