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

fn automerge_trace() -> HashSeq {
    let trace = load_automerge_trace();

    let mut seq = HashSeq::default();

    #[cfg(not(target_os = "macos"))]
    let guard = pprof::ProfilerGuard::new(100).unwrap();

    let progress = ProgressBar::new(trace.len() as u64);
    for (i, event) in trace.iter().enumerate() {
        if i % 10000 == 0 {
            let idx = match event {
                Trace::Insert(idx, _, _) => idx,
                Trace::Delete(idx, _) => idx,
            };
            let doc = String::from_iter(seq.iter());
            println!(
                "\033c{}",
                &doc[idx.saturating_sub(5000)..(idx + 1000).min(doc.len())]
            );
            println!(
                "markers={} hit={} miss={}",
                seq.markers.len(),
                seq.cache_hit,
                seq.cache_miss
            )
        }
        progress.inc(1);

        match event {
            Trace::Insert(idx, _, c) => seq.insert(*idx, *c),
            Trace::Delete(idx, _) => seq.remove(*idx),
        }
    }

    #[cfg(not(target_os = "macos"))]
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
    let seq = automerge_trace();

    let doc = String::from_iter(seq.iter());

    let mut file = File::create("automerge.latex").expect("Failed to create output file");
    write!(file, "{}", doc).expect("Failed to write final document");
}
