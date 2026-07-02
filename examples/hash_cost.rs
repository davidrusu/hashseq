//! Decompose the per-op id() cost: input width, hasher clone, char lookup.
use hashseq::value::char_value_id;
use hashseq::{HashNode, Id, Op};
use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

fn bench(name: &str, n: u32, mut f: impl FnMut()) {
    // warmup
    for _ in 0..n / 10 {
        f();
    }
    let t = Instant::now();
    for _ in 0..n {
        f();
    }
    println!("{name:>34}: {:6.1} ns/op", t.elapsed().as_nanos() as f64 / n as f64);
}

fn main() {
    let n = 2_000_000u32;
    let template = blake3::Hasher::new_derive_key("hashweb v1 node id");

    // Full current id() on the typing-chain fast path.
    let mut prev = Id([7; 32]);
    bench("id() fast path (68B preimage)", n, || {
        let node = HashNode {
            pins: BTreeSet::new(),
            op: Op::insert_after(prev, 'a'),
        };
        prev = node.id();
    });

    // Isolate input width: same clone+update+finalize, 68 vs 36 bytes.
    let buf68 = [0x5Au8; 68];
    let buf36 = [0x5Au8; 36];
    bench("clone+hash 68B (new width)", n, || {
        let mut h = template.clone();
        h.update(black_box(&buf68));
        black_box(h.finalize());
    });
    bench("clone+hash 36B (old width)", n, || {
        let mut h = template.clone();
        h.update(black_box(&buf36));
        black_box(h.finalize());
    });

    // Hasher clone alone (the template memcpy).
    bench("hasher clone only", n, || {
        black_box(template.clone());
    });

    // Char value-id lookup (should be ~a table index).
    bench("char_value_id('a') lookup", n, || {
        black_box(char_value_id(black_box('a')));
    });

    // One-shot derive_key for reference (recomputes context each call).
    bench("one-shot derive_key 68B", n, || {
        black_box(blake3::derive_key("hashweb v1 node id", black_box(&buf68)));
    });

    // hazmat: fresh hasher from a pre-hashed context key (same output as
    // new_derive_key — no identity change), skipping the template clone.
    use blake3::hazmat::HasherExt;
    let ctx_key = blake3::hazmat::hash_derive_key_context("hashweb v1 node id");
    bench("ctx-key fresh hasher 68B", n, || {
        let mut h = blake3::Hasher::new_from_context_key(black_box(&ctx_key));
        h.update(black_box(&buf68));
        black_box(h.finalize());
    });
    bench("ctx-key fresh hasher 36B", n, || {
        let mut h = blake3::Hasher::new_from_context_key(black_box(&ctx_key));
        h.update(black_box(&buf36));
        black_box(h.finalize());
    });

    // sanity: identical output to the derive_key template path
    let mut a = blake3::Hasher::new_derive_key("hashweb v1 node id");
    a.update(&buf68);
    let mut b = blake3::Hasher::new_from_context_key(&ctx_key);
    b.update(&buf68);
    assert_eq!(a.finalize(), b.finalize(), "hazmat path must match derive_key");
    println!("   hazmat output == derive_key: verified");
}
