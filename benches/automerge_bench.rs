use std::{
    collections::BTreeMap,
    fs::File,
    io::{self, BufRead},
};

use criterion::{criterion_group, criterion_main, Criterion};
use hashseq::HashSeq;
use serde::Deserialize;

#[allow(unused)]
#[derive(Deserialize)]
enum AutomergeAction {
    #[serde(rename = "set")]
    Set,
    #[serde(rename = "del")]
    Del,
    #[serde(rename = "makeText")]
    MakeText,
    #[serde(rename = "makeMap")]
    MakeMap,
}

#[allow(unused)]
#[derive(Deserialize)]
struct AutomergeOp {
    action: AutomergeAction,
    obj: String,
    insert: Option<bool>,
    key: String,
    value: Option<String>,
    pred: Vec<String>,
}

#[allow(unused)]
#[derive(Deserialize)]
struct AutomergeEvent {
    actor: String,
    seq: u32,
    deps: BTreeMap<String, u32>,
    message: String,
    #[serde(rename = "startOp")]
    start_op: u32,
    time: u64,
    ops: Vec<AutomergeOp>,
}

#[allow(unused)]
fn load_automerge_events() -> Vec<AutomergeEvent> {
    // It's assumed you have https://github.com/automerge/automerge-perf
    // cloned next to this repository and `edit-history/paper.json.gz` decompressed

    let file = File::open("../automerge-perf/edit-history/paper.json")
        .expect("Clone https://github.com/automerge/automerge-perf and `gzip -d paper.json.gz`");

    let mut events = Vec::new();
    for (i, line) in io::BufReader::new(file).lines().enumerate() {
        let line = line.expect("Failed to read line");
        events.push(serde_json::from_str(&line).expect("Failed to decode line"))
    }

    events
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Trace {
    Insert(usize, usize, char),
    Delete(usize, usize),
}

fn load_automerge_trace() -> Vec<Trace> {
    let file = File::open("../automerge-perf/edit-by-index/trace.json")
        .expect("Failed to find trace file");

    serde_json::from_reader(io::BufReader::new(file)).expect("Failed to parse trace")
}

fn automerge_bench(c: &mut Criterion) {
    let trace = load_automerge_trace();

    #[cfg(not(target_os = "macos"))]
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let mut group = c.benchmark_group("automerge");
    group.sample_size(10);
    group.bench_function("load automerge trace", |b| {
        b.iter(|| {
            let mut seq = HashSeq::default();

            for (i, event) in trace.iter().enumerate() {
                if i % 10000 == 0 {
                    println!("Processing {i}'th event");
                }

                match event {
                    Trace::Insert(idx, _, c) => seq.insert(*idx, *c),
                    Trace::Delete(idx, _) => seq.remove(*idx),
                }
            }
        });
    });

    #[cfg(not(target_os = "macos"))]
    if let Ok(report) = guard.report().build() {
        let file = std::fs::File::create("automerge-index-fg.svg").unwrap();
        report.flamegraph(file).unwrap();
    };
}

criterion_group!(benches, automerge_bench);
criterion_main!(benches);
