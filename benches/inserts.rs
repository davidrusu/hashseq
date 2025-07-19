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
    seq.insert_batch(0, std::iter::repeat_n('a', n));
}

fn insert_middle(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        seq.insert(seq.len() / 2, 'a');
    }
}

fn insert_random(n: usize) {
    let mut seq = HashSeq::default();
    for _ in 0..n {
        let p = if seq.is_empty() {
            0
        } else {
            rand::random::<usize>() % seq.len()
        };
        seq.insert(p, 'a');
    }
}

fn append_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("index-append-{n}"), |b| {
            b.iter(|| append(black_box(n)));
        });
    }
}

fn prepend_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("prepend {n}"), |b| {
            b.iter(|| prepend(black_box(n)));
        });
    }
}

fn insert_middle_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000] {
        c.bench_function(&format!("insert-middle {n}"), |b| {
            b.iter(|| insert_middle(black_box(n)));
        });
    }
}

fn insert_random_growth(c: &mut Criterion) {
    for n in [1, 10, 100, 1000, 10000] {
        c.bench_function(&format!("insert-random {n}"), |b| {
            b.iter(|| insert_random(black_box(n)));
        });
    }
}

criterion_group!(
    benches,
    append_growth,
    prepend_growth,
    insert_middle_growth,
    insert_random_growth
);
criterion_main!(benches);
