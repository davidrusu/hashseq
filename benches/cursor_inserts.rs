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
    for n in [1, 10, 100, 1000, 10000, 100000] {
        c.bench_function(&format!("cursor-append {n}"), |b| {
            b.iter(|| append(black_box(n)));
        });
    }
}

fn prepend_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000, 10000, 100000] {
        c.bench_function(&format!("cursor-prepend {n}"), |b| {
            b.iter(|| prepend(black_box(n)));
        });
    }
}

fn insert_middle_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000, 10000, 100000] {
        c.bench_function(&format!("cursor-insert-middle {n}"), |b| {
            b.iter(|| insert_middle(black_box(n)));
        });
    }
}

criterion_group!(benches, append_growth, prepend_growth, insert_middle_growth);
criterion_main!(benches);
