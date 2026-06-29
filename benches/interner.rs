//! Бенчмарк интернера символов.
//!
//! Запуск: `cargo bench --features bench --bench interner`

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use std::thread;
use tick_screener::interner::SymbolInterner;

fn bench_intern_single(c: &mut Criterion) {
    let interner = SymbolInterner::new();
    c.bench_function("intern_single_new_symbol", |b| {
        let mut counter: u64 = 0;
        b.iter(|| {
            counter = counter.wrapping_add(1);
            black_box(interner.intern(&format!("SYM{}", black_box(counter))))
        })
    });
}

fn bench_intern_existing(c: &mut Criterion) {
    let interner = SymbolInterner::new();
    interner.intern("BTC/USDT");
    c.bench_function("intern_existing_symbol", |b| {
        b.iter(|| black_box(interner.intern("BTC/USDT")))
    });
}

fn bench_resolve(c: &mut Criterion) {
    let interner = SymbolInterner::new();
    let id = interner.intern("BTC/USDT.P");
    c.bench_function("resolve", |b| {
        b.iter(|| black_box(interner.resolve(black_box(id))))
    });
}

fn bench_concurrent_intern(c: &mut Criterion) {
    c.bench_function("concurrent_intern_distinct", |b| {
        b.iter(|| {
            let interner = Arc::new(SymbolInterner::new());
            let mut handles = Vec::new();
            for i in 0..8 {
                let interner = Arc::clone(&interner);
                handles.push(thread::spawn(move || {
                    for j in 0..1000 {
                        interner.intern(&format!("SYM{}_{}", i, j));
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        })
    });
}

criterion_group!(
    benches,
    bench_intern_single,
    bench_intern_existing,
    bench_resolve,
    bench_concurrent_intern
);
criterion_main!(benches);
