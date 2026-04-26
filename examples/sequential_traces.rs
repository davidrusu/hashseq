use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;
use std::time::Instant;

use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use hashseq::encoding::{decode_string, decode_utf8_char, decode_varint};
use hashseq::{HashSeq, encode_hashseq};
use serde::Deserialize;
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

/// (position, delete_count, insert_content)
#[derive(Debug, Clone, Deserialize)]
struct TestPatch(usize, usize, String);

#[derive(Debug, Clone, Deserialize)]
struct TestTxn {
    patches: Vec<TestPatch>,
}

#[derive(Debug, Clone, Deserialize)]
struct TestData {
    #[serde(rename = "endContent")]
    end_content: String,
    txns: Vec<TestTxn>,
}

impl TestData {
    fn patch_count(&self) -> usize {
        self.txns.iter().map(|txn| txn.patches.len()).sum()
    }

    fn op_count(&self) -> usize {
        self.txns
            .iter()
            .flat_map(|txn| txn.patches.iter())
            .map(|TestPatch(_, del, ins)| *del + ins.chars().count())
            .sum()
    }

    fn patches(&self) -> impl Iterator<Item = &TestPatch> {
        self.txns.iter().flat_map(|txn| txn.patches.iter())
    }
}

fn load_testing_data(filename: &str) -> TestData {
    let file = File::open(filename).expect("Failed to open file");
    let mut reader = BufReader::new(file);
    let mut raw_json = Vec::new();

    if filename.ends_with(".gz") {
        let mut decoder = GzDecoder::new(reader);
        decoder
            .read_to_end(&mut raw_json)
            .expect("Failed to decompress");
    } else {
        reader.read_to_end(&mut raw_json).expect("Failed to read");
    }

    serde_json::from_slice(&raw_json).expect("Failed to parse JSON")
}

struct RunStats {
    times_ms: Vec<f64>,
    correct: bool,
    run_count: usize,
    ops: usize,
    patches: usize,
    final_text_bytes: usize,
    memory_bytes: usize,
    encoded_bytes: usize,
    text_gzip_bytes: usize,
    encoded_gzip_bytes: usize,
    breakdown: ByteBreakdown,
}

#[derive(Default)]
struct ByteBreakdown {
    dict_header: usize,
    roots: usize,
    runs: usize,
    runs_text: usize,
    befores: usize,
    forward_remove_runs: usize,
    backward_remove_runs: usize,
    single_run_removes: usize,
    before_removes: usize,
    root_removes: usize,
    orphans: usize,
}

impl RunStats {
    fn avg_ms(&self) -> f64 {
        self.times_ms.iter().sum::<f64>() / self.times_ms.len() as f64
    }

    fn std_dev_percent(&self) -> f64 {
        let avg = self.avg_ms();
        let variance = self.times_ms.iter().map(|t| (t - avg).powi(2)).sum::<f64>()
            / self.times_ms.len() as f64;
        (variance.sqrt() / avg) * 100.0
    }

    fn min_ms(&self) -> f64 {
        self.times_ms.iter().cloned().fold(f64::INFINITY, f64::min)
    }

    fn max_ms(&self) -> f64 {
        self.times_ms
            .iter()
            .cloned()
            .fold(f64::NEG_INFINITY, f64::max)
    }

    fn ops_per_sec(&self) -> f64 {
        self.ops as f64 / (self.avg_ms() / 1000.0)
    }

    fn patches_per_sec(&self) -> f64 {
        self.patches as f64 / (self.avg_ms() / 1000.0)
    }
}

fn build_seq(data: &TestData) -> (HashSeq, std::time::Duration) {
    let mut seq = HashSeq::default();
    let start = Instant::now();
    for TestPatch(pos, del, ins) in data.patches() {
        seq.remove_batch(*pos, *del);
        seq.insert_batch(*pos, ins.chars());
    }
    let elapsed = start.elapsed();
    (seq, elapsed)
}

/// Re-walk an encoded HashSeq using the public primitive decoders to attribute
/// every byte to a section. Mirrors `encode_hashseq`'s layout exactly.
///
/// Also asserts every entry in the dictionary header is referenced at least
/// once by the body — an unused entry would mean wasted bytes in the encoder.
fn byte_breakdown(bytes: &[u8]) -> ByteBreakdown {
    // Tag bytes for orphan ops (must match the constants in src/encoding.rs).
    const TAG_INSERT_ROOT: u8 = 0x01;
    const TAG_INSERT_BEFORE: u8 = 0x02;
    const TAG_REMOVE: u8 = 0x03;
    const TAG_INSERT_AFTER: u8 = 0x04;

    fn read_varint(bytes: &[u8], pos: &mut usize) -> usize {
        let (v, sz) = decode_varint(&bytes[*pos..]).expect("varint");
        *pos += sz;
        v
    }
    /// Skip a varint that's a positional index (run_idx, elem_idx, before_idx, etc.)
    /// or just a count — *not* a reference into the dictionary.
    fn skip_varint(bytes: &[u8], pos: &mut usize) {
        let (_, sz) = decode_varint(&bytes[*pos..]).expect("varint");
        *pos += sz;
    }
    /// Skip a varint that *is* a reference into the ID dictionary, and mark it.
    fn skip_idx(bytes: &[u8], pos: &mut usize, referenced: &mut [bool]) {
        let (idx, sz) = decode_varint(&bytes[*pos..]).expect("idx");
        assert!(
            idx < referenced.len(),
            "dict index {idx} out of bounds (dict has {} entries)",
            referenced.len()
        );
        referenced[idx] = true;
        *pos += sz;
    }
    fn skip_idx_set(bytes: &[u8], pos: &mut usize, referenced: &mut [bool]) {
        let n = read_varint(bytes, pos);
        for _ in 0..n {
            skip_idx(bytes, pos, referenced);
        }
    }
    fn skip_utf8_char(bytes: &[u8], pos: &mut usize) {
        let (_, sz) = decode_utf8_char(&bytes[*pos..]).expect("char");
        *pos += sz;
    }

    let mut b = ByteBreakdown::default();
    let mut pos = 0;

    // Dict header: varint(num_ids) + num_ids * 32.
    let dict_start = pos;
    let num_ids = read_varint(bytes, &mut pos);
    pos += num_ids * 32;
    b.dict_header = pos - dict_start;
    let mut referenced: Vec<bool> = vec![false; num_ids];

    // Roots: varint(num) + num * { idx_set extra_deps, utf8 ch }
    let s = pos;
    let num_roots = read_varint(bytes, &mut pos);
    for _ in 0..num_roots {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_utf8_char(bytes, &mut pos);
    }
    b.roots = pos - s;

    // Runs: varint(num) + num * { idx insert_after, idx_set first_extra_deps, string run_text }
    let s = pos;
    let num_runs = read_varint(bytes, &mut pos);
    for _ in 0..num_runs {
        skip_idx(bytes, &mut pos, &mut referenced);
        skip_idx_set(bytes, &mut pos, &mut referenced);
        let (run_text, sz) = decode_string(&bytes[pos..]).expect("string");
        pos += sz;
        b.runs_text += run_text.len();
    }
    b.runs = pos - s;

    // Befores: varint(num) + num * { idx_set extra_deps, idx anchor, utf8 ch }
    let s = pos;
    let num_befores = read_varint(bytes, &mut pos);
    for _ in 0..num_befores {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_idx(bytes, &mut pos, &mut referenced);
        skip_utf8_char(bytes, &mut pos);
    }
    b.befores = pos - s;

    // Forward remove runs: varint(num) + num * { idx_set first_extra_deps, varint run_idx, varint start, varint end }
    let s = pos;
    let num_forward = read_varint(bytes, &mut pos);
    for _ in 0..num_forward {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx (positional)
        skip_varint(bytes, &mut pos); // start_idx (positional)
        skip_varint(bytes, &mut pos); // end_idx (positional)
    }
    b.forward_remove_runs = pos - s;

    // Backward remove runs: same shape
    let s = pos;
    let num_backward = read_varint(bytes, &mut pos);
    for _ in 0..num_backward {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx
        skip_varint(bytes, &mut pos); // start_idx
        skip_varint(bytes, &mut pos); // end_idx
    }
    b.backward_remove_runs = pos - s;

    // Single-run standalone removes: varint(num) + num * { idx_set extra_deps, varint run_idx, varint elem_idx }
    let s = pos;
    let num_single = read_varint(bytes, &mut pos);
    for _ in 0..num_single {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx
        skip_varint(bytes, &mut pos); // elem_idx
    }
    b.single_run_removes = pos - s;

    // Before-target standalone removes: varint(num) + num * { idx_set extra_deps, varint before_idx }
    let s = pos;
    let num_before_rm = read_varint(bytes, &mut pos);
    for _ in 0..num_before_rm {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // before_idx (positional)
    }
    b.before_removes = pos - s;

    // Root-target standalone removes: varint(num) + num * { idx_set extra_deps, varint root_idx }
    let s = pos;
    let num_root_rm = read_varint(bytes, &mut pos);
    for _ in 0..num_root_rm {
        skip_idx_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // root_idx (positional)
    }
    b.root_removes = pos - s;

    // Orphans: varint(num) + num * tagged HashNode
    let s = pos;
    let num_orphans = read_varint(bytes, &mut pos);
    for _ in 0..num_orphans {
        let tag = bytes[pos];
        pos += 1;
        match tag {
            TAG_INSERT_ROOT => {
                skip_idx_set(bytes, &mut pos, &mut referenced);
                skip_utf8_char(bytes, &mut pos);
            }
            TAG_INSERT_AFTER | TAG_INSERT_BEFORE => {
                skip_idx_set(bytes, &mut pos, &mut referenced);
                skip_idx(bytes, &mut pos, &mut referenced);
                skip_utf8_char(bytes, &mut pos);
            }
            TAG_REMOVE => {
                skip_idx_set(bytes, &mut pos, &mut referenced);
                let n = read_varint(bytes, &mut pos);
                for _ in 0..n {
                    skip_idx(bytes, &mut pos, &mut referenced);
                }
            }
            other => panic!("unknown orphan tag: {other:#x}"),
        }
    }
    b.orphans = pos - s;

    assert_eq!(
        pos,
        bytes.len(),
        "byte_breakdown didn't consume the full encoding ({} of {} bytes)",
        pos,
        bytes.len()
    );

    // Sanity check: every dictionary entry must be referenced by the body.
    // An unused entry would mean the encoder wrote a 32-byte ID nobody asked for.
    let unused: Vec<usize> = referenced
        .iter()
        .enumerate()
        .filter_map(|(i, used)| (!*used).then_some(i))
        .collect();
    assert!(
        unused.is_empty(),
        "{} of {} dictionary entries are never referenced (e.g. indices {:?}) — \
         {} bytes of dict header are wasted",
        unused.len(),
        referenced.len(),
        &unused[..unused.len().min(8)],
        unused.len() * 32,
    );

    b
}

fn gzip_size(bytes: &[u8]) -> usize {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes).expect("gzip");
    encoder.finish().expect("gzip finish").len()
}

fn measure_memory(seq: &HashSeq) -> usize {
    let before = GLOBAL.stats().bytes_allocated;
    let clone = seq.clone();
    let after = GLOBAL.stats().bytes_allocated;
    // Use clone to prevent the allocation from being optimized away.
    std::hint::black_box(&clone);
    after.saturating_sub(before)
}

fn run_trace(data: &TestData, iterations: usize) -> RunStats {
    let ops = data.op_count();
    let patches = data.patch_count();

    let mut times_ms = Vec::with_capacity(iterations);
    let mut correct = true;
    let mut run_count = 0;

    for _ in 0..iterations {
        let (seq, elapsed) = build_seq(data);
        times_ms.push(elapsed.as_secs_f64() * 1000.0);
        let result: String = seq.iter().collect();
        correct = correct && result == data.end_content;
        run_count = seq.runs.len();
    }

    // Storage measurements: build once more outside the timing loop.
    let (seq, _) = build_seq(data);
    let final_text_bytes = seq.iter().map(|c| c.len_utf8()).sum();
    let memory_bytes = measure_memory(&seq);
    let encoded = encode_hashseq(&seq);
    let encoded_bytes = encoded.len();
    let breakdown = byte_breakdown(&encoded);
    let text: String = seq.iter().collect();
    let text_gzip_bytes = gzip_size(text.as_bytes());
    let encoded_gzip_bytes = gzip_size(&encoded);

    RunStats {
        times_ms,
        correct,
        run_count,
        ops,
        patches,
        final_text_bytes,
        memory_bytes,
        encoded_bytes,
        text_gzip_bytes,
        encoded_gzip_bytes,
        breakdown,
    }
}

fn main() {
    let traces_dir = Path::new("../editing-traces/sequential_traces");
    let iterations = 1;

    let traces = [
        "automerge-paper.json.gz",
        "rustcode.json.gz",
        "sveltecomponent.json.gz",
        "seph-blog1.json.gz",
        "clownschool_flat.json.gz",
        "friendsforever_flat.json.gz",
        "json-crdt-blog-post.json.gz",
    ];

    println!("Running each trace {} times\n", iterations);

    println!("Performance");
    println!(
        "{:<25} {:>10} {:>10} {:>10} {:>10} {:>8} {:>10} {:>12} {:>12}",
        "Trace",
        "Avg(ms)",
        "StdDev%",
        "Min(ms)",
        "Max(ms)",
        "Correct",
        "Runs",
        "Ops/sec",
        "Patches/sec"
    );
    println!("{}", "-".repeat(117));

    let mut all_stats: Vec<(&str, RunStats)> = Vec::new();

    for trace_name in traces {
        let path = traces_dir.join(trace_name);
        let display_name = trace_name.trim_end_matches(".json.gz");
        if path.exists() {
            let data = load_testing_data(path.to_str().unwrap());
            let stats = run_trace(&data, iterations);

            println!(
                "{:<25} {:>10.2} {:>9.1}% {:>10.2} {:>10.2} {:>8} {:>10} {:>12.0} {:>12.0}",
                display_name,
                stats.avg_ms(),
                stats.std_dev_percent(),
                stats.min_ms(),
                stats.max_ms(),
                if stats.correct { "T" } else { "F" },
                stats.run_count,
                stats.ops_per_sec(),
                stats.patches_per_sec()
            );
            all_stats.push((display_name, stats));
        } else {
            println!("{:<25} File not found: {:?}", display_name, path);
        }
    }

    println!("\nStorage (bytes; ratios are over final UTF-8 text size)");
    println!(
        "{:<25} {:>10} {:>10} {:>8} {:>10} {:>8} {:>10} {:>10} {:>9}",
        "Trace", "Text", "Memory", "Mem/x", "Encoded", "Enc/x", "Text+gz", "Enc+gz", "Enc/Enc+gz",
    );
    println!("{}", "-".repeat(110));

    for (name, stats) in &all_stats {
        let text = stats.final_text_bytes.max(1) as f64;
        let enc_gz = stats.encoded_gzip_bytes.max(1) as f64;
        println!(
            "{:<25} {:>10} {:>10} {:>7.2}x {:>10} {:>7.2}x {:>10} {:>10} {:>8.2}x",
            name,
            stats.final_text_bytes,
            stats.memory_bytes,
            stats.memory_bytes as f64 / text,
            stats.encoded_bytes,
            stats.encoded_bytes as f64 / text,
            stats.text_gzip_bytes,
            stats.encoded_gzip_bytes,
            stats.encoded_bytes as f64 / enc_gz,
        );
    }

    println!("\nEncoded byte breakdown by section");
    println!(
        "{:<25} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "Trace",
        "Total",
        "Dict",
        "Roots",
        "Runs",
        "RunText",
        "Befores",
        "RmRunF",
        "RmRunB",
        "RmSing",
        "RmBef",
        "RmRoot",
    );
    println!("{}", "-".repeat(140));
    for (name, stats) in &all_stats {
        let b = &stats.breakdown;
        println!(
            "{:<25} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            name,
            stats.encoded_bytes,
            b.dict_header,
            b.roots,
            b.runs,
            b.runs_text,
            b.befores,
            b.forward_remove_runs,
            b.backward_remove_runs,
            b.single_run_removes,
            b.before_removes,
            b.root_removes,
        );
    }
    println!(
        "  RunText is the actual character bytes (UTF-8) inside the Runs section — \
         everything else is structural overhead."
    );

    println!("\nByte breakdown as % of encoding");
    println!(
        "{:<25} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}",
        "Trace",
        "Dict%",
        "Roots%",
        "Runs%",
        "Text%",
        "Bef%",
        "RmRunF%",
        "RmRunB%",
        "RmSing%",
        "RmBef%",
        "RmRoot%",
    );
    println!("{}", "-".repeat(115));
    for (name, stats) in &all_stats {
        let b = &stats.breakdown;
        let t = stats.encoded_bytes.max(1) as f64;
        let pct = |x: usize| 100.0 * x as f64 / t;
        println!(
            "{:<25} {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}%",
            name,
            pct(b.dict_header),
            pct(b.roots),
            pct(b.runs),
            pct(b.runs_text),
            pct(b.befores),
            pct(b.forward_remove_runs),
            pct(b.backward_remove_runs),
            pct(b.single_run_removes),
            pct(b.before_removes),
            pct(b.root_removes),
        );
    }
}
