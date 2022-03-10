use criterion::{black_box, criterion_group, criterion_main, Criterion};

use hashseq::HashSeq;

fn append(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        seq.insert(seq.len(), 'a');
    }
}

fn append_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("append {n}"), |b| {
            let guard = pprof::ProfilerGuard::new(100).unwrap();

            b.iter(|| append(black_box(n)));

            if let Ok(report) = guard.report().build() {
                let file = std::fs::File::create(&format!("append-{n}-fg.svg")).unwrap();
                report.flamegraph(file).unwrap();
            };
        });
    }
}

criterion_group!(benches, append_growth);
criterion_main!(benches);
