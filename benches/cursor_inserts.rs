use criterion::{black_box, criterion_group, criterion_main, Criterion};

use hashseq::HashSeq;

fn prepend(n: usize) {
    let mut cursor = HashSeq::default().cursor();
    for _ in 0..n {
        cursor.insert_ahead('a');
    }
}

fn append(n: usize) {
    let mut cursor = HashSeq::default().cursor();
    for _ in 0..n {
        cursor.insert('a');
    }
}

fn insert_middle(n: usize) {
    let mut cursor = HashSeq::default().cursor();
    for _ in 0..n {
	cursor.seek(cursor.seq().len() / 2);
        cursor.insert('a');
    }
}

fn append_growth(c: &mut Criterion) {
    for n in [1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000] {
        // let guard = pprof::ProfilerGuard::new(100).unwrap();

        c.bench_function(&format!("cursor-append {n}"), |b| {
            b.iter(|| append(black_box(n)));
        });

        // if let Ok(report) = guard.report().build() {
        //     let file = std::fs::File::create(&format!("append-{n}-fg.svg")).unwrap();
        //     report.flamegraph(file).unwrap();
        // };
    }
}

fn prepend_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        // let guard = pprof::ProfilerGuard::new(100).unwrap();

        c.bench_function(&format!("cursor-prepend {n}"), |b| {
            b.iter(|| prepend(black_box(n)));
        });

        // if let Ok(report) = guard.report().build() {
        //     let file = std::fs::File::create(&format!("prepend-{n}-fg.svg")).unwrap();
        //     report.flamegraph(file).unwrap();
        // };
    }
}

fn insert_middle_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        // let guard = pprof::ProfilerGuard::new(100).unwrap();

        c.bench_function(&format!("cursor-insert-middle {n}"), |b| {
            b.iter(|| insert_middle(black_box(n)));
        });

        // if let Ok(report) = guard.report().build() {
        //     let file = std::fs::File::create(&format!("insert-middle-{n}-fg.svg")).unwrap();
        //     report.flamegraph(file).unwrap();
        // };
    }
}

criterion_group!(benches, append_growth, prepend_growth, insert_middle_growth);
criterion_main!(benches);
