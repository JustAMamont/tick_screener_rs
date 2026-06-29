//! Бенчмарк ядра сканера.
//!
//! Запуск: `cargo bench --features bench --bench scanner_core`

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::collections::HashSet;
use std::sync::Arc;
use tick_screener::interner::SymbolInterner;
use tick_screener::scanner::{ScannerConfig, ScannerCore};

fn make_config() -> ScannerConfig {
    ScannerConfig {
        return_limit: 1.0,
        volume_limit: 1000.0,
        timeframe_s: 60,
        currency_type: "spot".to_string(),
        delimiter: "".to_string(),
        blacklist: Arc::new(HashSet::new()),
    }
}

fn bench_process_small_batch(c: &mut Criterion) {
    let interner = Arc::new(SymbolInterner::new());
    let core = ScannerCore::new(interner);
    let cfg = make_config();

    c.bench_function("process_50_symbols_1_trade_each", |b| {
        b.iter(|| {
            let trades: Vec<_> = (0..50)
                .map(|i| (format!("SYM{}/USDT", i), 1_700_000_000_000, 100.0, 10.0))
                .collect();
            black_box(core.process_trades(trades, &cfg));
        })
    });
}

fn bench_process_large_batch(c: &mut Criterion) {
    let interner = Arc::new(SymbolInterner::new());
    let core = ScannerCore::new(interner);
    let cfg = make_config();

    c.bench_function("process_1000_symbols_10_trades_each", |b| {
        b.iter(|| {
            let mut trades = Vec::with_capacity(10_000);
            for i in 0..1000 {
                for j in 0..10 {
                    trades.push((
                        format!("SYM{}/USDT", i),
                        1_700_000_000_000 + j * 1000,
                        100.0 + i as f64,
                        10.0,
                    ));
                }
            }
            black_box(core.process_trades(trades, &cfg));
        })
    });
}

criterion_group!(
    benches,
    bench_process_small_batch,
    bench_process_large_batch
);
criterion_main!(benches);
