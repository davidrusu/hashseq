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
    run_size_dist: RunSizeDist,
}

#[derive(Default)]
struct RunSizeDist {
    total_chars: usize,
    min: usize,
    median: usize,
    p99: usize,
    p99_5: usize,
    p99_9: usize,
    max: usize,
    avg: f64,
}

fn run_size_dist(seq: &HashSeq) -> RunSizeDist {
    let mut sizes: Vec<usize> = seq.runs.values().map(|r| r.len()).collect();
    sizes.sort();
    let num = sizes.len();
    if num == 0 {
        return RunSizeDist::default();
    }
    let total_chars: usize = sizes.iter().sum();
    let percentile = |p: f64| {
        let idx = ((num as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
        sizes[idx.min(num - 1)]
    };
    RunSizeDist {
        total_chars,
        min: sizes[0],
        median: percentile(50.0),
        p99: percentile(99.0),
        p99_5: percentile(99.5),
        p99_9: percentile(99.9),
        max: sizes[num - 1],
        avg: total_chars as f64 / num as f64,
    }
}

#[derive(Default)]
struct ByteBreakdown {
    dict_header: usize,
    runs: usize,
    runs_text: usize,
    befores: usize,
    forward_remove_runs: usize,
    backward_remove_runs: usize,
    single_run_removes: usize,
    other_removes: usize,
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
    const TAG_INSERT_AFTER: u8 = 0x01;
    const TAG_INSERT_BEFORE: u8 = 0x02;
    const TAG_REMOVE: u8 = 0x03;

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
    /// Skip a tagged ref: low bit 1 = positional (run_idx, elem_idx) — one
    /// more varint follows; low bit 0 = dict index — mark it referenced.
    fn skip_ref(bytes: &[u8], pos: &mut usize, referenced: &mut [bool]) {
        let (v, sz) = decode_varint(&bytes[*pos..]).expect("ref");
        *pos += sz;
        if v & 1 == 1 {
            skip_varint(bytes, pos); // elem_idx
        } else {
            let idx = v >> 1;
            assert!(idx < referenced.len(), "dict ref {idx} out of bounds");
            referenced[idx] = true;
        }
    }
    fn skip_ref_set(bytes: &[u8], pos: &mut usize, referenced: &mut [bool]) {
        let n = read_varint(bytes, pos);
        for _ in 0..n {
            skip_ref(bytes, pos, referenced);
        }
    }
    fn skip_utf8_char(bytes: &[u8], pos: &mut usize) {
        let (_, sz) = decode_utf8_char(&bytes[*pos..]).expect("char");
        *pos += sz;
    }

    let mut b = ByteBreakdown::default();
    let mut pos = 0;

    // Header: origin id (32 bytes, implicit dict entry 0), then the dict:
    // varint(num_ids) + num_ids * 32.
    let dict_start = pos;
    pos += 32; // origin
    let num_ids = read_varint(bytes, &mut pos);
    pos += num_ids * 32;
    b.dict_header = pos - dict_start;
    let mut referenced: Vec<bool> = vec![false; num_ids + 1];
    referenced[0] = true; // the origin is referenced by definition

    // Runs: one dependency-ordered section, per-run first_op tag.
    // { u8 tag, ref anchor, ref_set first_extra_deps, string, interior deps }
    {
        let runs_start = pos;
        let num_runs = read_varint(bytes, &mut pos);
        for _ in 0..num_runs {
            let s = pos;
            let tag = bytes[pos];
            pos += 1;
            skip_ref(bytes, &mut pos, &mut referenced);
            skip_ref_set(bytes, &mut pos, &mut referenced);
            let (run_text, sz) = decode_string(&bytes[pos..]).expect("string");
            pos += sz;
            b.runs_text += run_text.len();
            // interior extra-deps: count + (offset, ref_set)
            let n_interior = read_varint(bytes, &mut pos);
            for _ in 0..n_interior {
                skip_varint(bytes, &mut pos); // offset (positional)
                skip_ref_set(bytes, &mut pos, &mut referenced);
            }
            // attribute to the runs/befores columns by tag (0x01 = before)
            if tag == 0x01 {
                b.befores += pos - s;
            } else {
                b.runs += pos - s;
            }
        }
        // the section-count varint itself goes unattributed (couple of bytes)
        let _ = runs_start;
    }

    // Forward remove runs: varint(num) + num * { idx_set first_extra_deps, varint run_idx, varint start, varint end }
    let s = pos;
    let num_forward = read_varint(bytes, &mut pos);
    for _ in 0..num_forward {
        skip_ref_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx (positional)
        skip_varint(bytes, &mut pos); // start_idx (positional)
        skip_varint(bytes, &mut pos); // end_idx (positional)
    }
    b.forward_remove_runs = pos - s;

    // Backward remove runs: same shape
    let s = pos;
    let num_backward = read_varint(bytes, &mut pos);
    for _ in 0..num_backward {
        skip_ref_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx
        skip_varint(bytes, &mut pos); // start_idx
        skip_varint(bytes, &mut pos); // end_idx
    }
    b.backward_remove_runs = pos - s;

    // Single-run standalone removes: varint(num) + num * { idx_set extra_deps, varint run_idx, varint elem_idx }
    let s = pos;
    let num_single = read_varint(bytes, &mut pos);
    for _ in 0..num_single {
        skip_ref_set(bytes, &mut pos, &mut referenced);
        skip_varint(bytes, &mut pos); // run_idx
        skip_varint(bytes, &mut pos); // elem_idx
    }
    b.single_run_removes = pos - s;

    // Other removes: varint(num) + num * { idx_set extra_deps, varint n, n * tagged target }
    let s = pos;
    let num_other = read_varint(bytes, &mut pos);
    for _ in 0..num_other {
        skip_ref_set(bytes, &mut pos, &mut referenced);
        let n = read_varint(bytes, &mut pos);
        for _ in 0..n {
            skip_ref(bytes, &mut pos, &mut referenced); // target
        }
    }
    b.other_removes = pos - s;

    // Orphans: varint(num) + num * tagged HashNode
    let s = pos;
    let num_orphans = read_varint(bytes, &mut pos);
    for _ in 0..num_orphans {
        let tag = bytes[pos];
        pos += 1;
        match tag {
            TAG_INSERT_AFTER | TAG_INSERT_BEFORE => {
                skip_ref_set(bytes, &mut pos, &mut referenced);
                skip_ref(bytes, &mut pos, &mut referenced);
                skip_utf8_char(bytes, &mut pos);
            }
            TAG_REMOVE => {
                skip_ref_set(bytes, &mut pos, &mut referenced);
                let n = read_varint(bytes, &mut pos);
                for _ in 0..n {
                    skip_ref(bytes, &mut pos, &mut referenced);
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

// TEMPORARY: per-component memory breakdown. Remove before commit.
fn measure_alloc<T>(f: impl FnOnce() -> T) -> usize {
    let before = GLOBAL.stats().bytes_allocated;
    let v = f();
    let after = GLOBAL.stats().bytes_allocated;
    std::hint::black_box(&v);
    after.saturating_sub(before)
}

fn memory_breakdown(seq: &HashSeq, total: usize, label: &str) {
    let runs = measure_alloc(|| seq.runs.clone());
    let elements = measure_alloc(|| {
        seq.runs
            .values()
            .map(|r| r.elements.clone())
            .collect::<Vec<_>>()
    });
    let run_strings = measure_alloc(|| {
        seq.runs
            .values()
            .map(|r| r.text.clone())
            .collect::<Vec<_>>()
    });
    let intern = measure_alloc(|| (seq.ids.clone(), seq.locs.clone(), seq.removed.clone()));
    let befores = measure_alloc(|| seq.befores_by_anchor.clone());
    let afters = measure_alloc(|| seq.afters.clone());

    let removes = measure_alloc(|| seq.remove_nodes.clone());
    let remove_runs = measure_alloc(|| seq.remove_runs.clone());

    let rest = total.saturating_sub(runs + intern + befores + afters + removes + remove_runs);
    let n_elems: usize = seq.runs.values().map(|r| r.len()).sum();
    let n_removes = seq.remove_nodes.len();
    eprintln!(
        "MEM {label}: total={total} runs={runs} (elements={elements} text={run_strings}) \
         intern_vecs={intern} befores={befores} afters={afters} \
         remove_nodes={removes} remove_runs={remove_runs} \
         id_map+index+rest={rest} [elems={n_elems} removes={n_removes}]"
    );
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
    memory_breakdown(&seq, memory_bytes, "trace");
    let encoded = encode_hashseq(&seq);
    let encoded_bytes = encoded.len();
    let breakdown = byte_breakdown(&encoded);
    let text: String = seq.iter().collect();
    let text_gzip_bytes = gzip_size(text.as_bytes());
    let encoded_gzip_bytes = gzip_size(&encoded);
    let run_size_dist = run_size_dist(&seq);

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
        run_size_dist,
    }
}

fn main() {
    let traces_dir = Path::new("../editing-traces/sequential_traces");
    let iterations = 50;

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
        "{:<25} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
        "Trace",
        "Total",
        "Dict",
        "Runs",
        "RunText",
        "Befores",
        "RmRunF",
        "RmRunB",
        "RmSing",
        "RmOther",
    );
    println!("{}", "-".repeat(120));
    for (name, stats) in &all_stats {
        let b = &stats.breakdown;
        println!(
            "{:<25} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9} {:>9}",
            name,
            stats.encoded_bytes,
            b.dict_header,
            b.runs,
            b.runs_text,
            b.befores,
            b.forward_remove_runs,
            b.backward_remove_runs,
            b.single_run_removes,
            b.other_removes,
        );
    }
    println!(
        "  RunText is the actual character bytes (UTF-8) inside the Runs section — \
         everything else is structural overhead."
    );

    println!("\nByte breakdown as % of encoding");
    println!(
        "{:<25} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}",
        "Trace", "Dict%", "Runs%", "Text%", "Bef%", "RmRunF%", "RmRunB%", "RmSing%", "RmOther%",
    );
    println!("{}", "-".repeat(100));
    for (name, stats) in &all_stats {
        let b = &stats.breakdown;
        let t = stats.encoded_bytes.max(1) as f64;
        let pct = |x: usize| 100.0 * x as f64 / t;
        println!(
            "{:<25} {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}% {:>6.1}%",
            name,
            pct(b.dict_header),
            pct(b.runs),
            pct(b.runs_text),
            pct(b.befores),
            pct(b.forward_remove_runs),
            pct(b.backward_remove_runs),
            pct(b.single_run_removes),
            pct(b.other_removes),
        );
    }

    println!("\nRun size distribution (chars per run)");
    println!(
        "{:<25} {:>8} {:>10} {:>5} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7}",
        "Trace", "Runs", "TotalChrs", "Min", "Median", "p99", "p99.5", "p99.9", "Max", "Avg",
    );
    println!("{}", "-".repeat(96));
    for (name, stats) in &all_stats {
        let d = &stats.run_size_dist;
        println!(
            "{:<25} {:>8} {:>10} {:>5} {:>7} {:>7} {:>7} {:>7} {:>7} {:>7.2}",
            name,
            stats.run_count,
            d.total_chars,
            d.min,
            d.median,
            d.p99,
            d.p99_5,
            d.p99_9,
            d.max,
            d.avg,
        );
    }
}
