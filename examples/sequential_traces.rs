use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use std::time::Instant;

use flate2::read::GzDecoder;
use hashseq::HashSeq;
use serde::Deserialize;

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

fn run_trace_once(data: &TestData) -> (std::time::Duration, bool, usize) {
    let mut seq = HashSeq::default();

    let start = Instant::now();
    for TestPatch(pos, del, ins) in data.patches() {
        seq.remove_batch(*pos, *del);
        seq.insert_batch(*pos, ins.chars());
    }
    let elapsed = start.elapsed();

    let result: String = seq.iter().collect();
    let correct = result == data.end_content;
    let run_count = seq.runs.len();

    (elapsed, correct, run_count)
}

fn run_trace(data: &TestData, iterations: usize) -> RunStats {
    let ops = data.op_count();
    let patches = data.patch_count();

    let mut times_ms = Vec::with_capacity(iterations);
    let mut correct = true;
    let mut run_count = 0;

    for _ in 0..iterations {
        let (elapsed, iter_correct, iter_run_count) = run_trace_once(data);
        times_ms.push(elapsed.as_secs_f64() * 1000.0);
        correct = correct && iter_correct;
        run_count = iter_run_count;
    }

    RunStats {
        times_ms,
        correct,
        run_count,
        ops,
        patches,
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

    for trace_name in traces {
        let path = traces_dir.join(trace_name);
        if path.exists() {
            let data = load_testing_data(path.to_str().unwrap());
            let stats = run_trace(&data, iterations);

            let display_name = trace_name.trim_end_matches(".json.gz");
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
        } else {
            let display_name = trace_name.trim_end_matches(".json.gz");
            println!("{:<25} File not found: {:?}", display_name, path);
        }
    }
}
