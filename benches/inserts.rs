use criterion::{black_box, criterion_group, criterion_main, Criterion};

use hashseq::HashSeq;

fn prepend(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        seq.insert(0, 'a');
    }
}

fn append(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        seq.insert(seq.len(), 'a');
    }
}

fn insert_middle(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        seq.insert(seq.len() / 2, 'a');
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

fn prepend_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("prepend {n}"), |b| {
            let guard = pprof::ProfilerGuard::new(100).unwrap();

            b.iter(|| prepend(black_box(n)));

            if let Ok(report) = guard.report().build() {
                let file = std::fs::File::create(&format!("prepend-{n}-fg.svg")).unwrap();
                report.flamegraph(file).unwrap();
            };
        });
    }
}

fn insert_middle_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("insert-middle {n}"), |b| {
            let guard = pprof::ProfilerGuard::new(100).unwrap();

            b.iter(|| insert_middle(black_box(n)));

            if let Ok(report) = guard.report().build() {
                let file = std::fs::File::create(&format!("insert-middle-{n}-fg.svg")).unwrap();
                report.flamegraph(file).unwrap();
            };
        });
    }
}

criterion_group!(benches, append_growth, prepend_growth, insert_middle_growth);
criterion_main!(benches);
