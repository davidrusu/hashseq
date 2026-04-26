use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::Instant;

use flate2::read::GzDecoder;
use hashseq::{HashSeq, encode_hashseq, encode_hashseq_dict};
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
    encoded_dict_bytes: usize,
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
    let encoded_bytes = encode_hashseq(&seq).len();
    let encoded_dict_bytes = encode_hashseq_dict(&seq).len();

    RunStats {
        times_ms,
        correct,
        run_count,
        ops,
        patches,
        final_text_bytes,
        memory_bytes,
        encoded_bytes,
        encoded_dict_bytes,
    }
}

fn main() {
    let traces_dir = Path::new("../editing-traces/sequential_traces");
    let iterations = 3;

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
        "{:<25} {:>10} {:>10} {:>8} {:>10} {:>8} {:>10} {:>8}",
        "Trace", "Text", "Memory", "Mem/x", "Encoded", "Enc/x", "EncDict", "Dict/x",
    );
    println!("{}", "-".repeat(96));

    for (name, stats) in &all_stats {
        let text = stats.final_text_bytes.max(1) as f64;
        println!(
            "{:<25} {:>10} {:>10} {:>7.2}x {:>10} {:>7.2}x {:>10} {:>7.2}x",
            name,
            stats.final_text_bytes,
            stats.memory_bytes,
            stats.memory_bytes as f64 / text,
            stats.encoded_bytes,
            stats.encoded_bytes as f64 / text,
            stats.encoded_dict_bytes,
            stats.encoded_dict_bytes as f64 / text,
        );
    }
}
