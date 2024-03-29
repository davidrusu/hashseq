use std::{
    fs::File,
    io::{self, Write},
};

use ::hashseq::HashSeq;
use indicatif::ProgressBar;
use serde::Deserialize;

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

fn automerge_trace(n: usize) -> HashSeq {
    let trace = load_automerge_trace();

    let mut seq = HashSeq::default();

    let guard = pprof::ProfilerGuard::new(500).unwrap();

    let progress = ProgressBar::new((trace.len() * n) as u64);
    for _ in 0..n {
        for event in trace.iter() {
            progress.inc(1);

            match event {
                Trace::Insert(idx, _, c) => seq.insert(*idx, *c),
                Trace::Delete(idx, _) => seq.remove(*idx),
            }
        }
    }

    if let Ok(report) = guard.report().build() {
        let file =
            File::create("automerge-trace-fg.svg").expect("Failed to create flamegraph file");
        report
            .flamegraph(file)
            .expect("Failed to generate flamegraph");
    };

    seq
}

fn main() {
    let seq = automerge_trace(10);

    let doc = String::from_iter(seq.iter());

    // let doc_bytes = doc.as_bytes().len();
    // let seq_bytes = bincode::serialize(&seq).unwrap().len();

    // println!(
    //     "doc: {doc_bytes}, seq: {seq_bytes}, overhead: {:.2}x",
    //     seq_bytes as f64 / doc_bytes as f64
    // );

    let mut file = File::create("automerge.latex").expect("Failed to create output file");
    write!(file, "{doc}").expect("Failed to write final document");
}
