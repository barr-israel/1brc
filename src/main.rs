#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

mod use_rayon;

fn main() {
    use_rayon::run();
}
