#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod use_avx512;

fn main() {
    use_avx512::run();
}
