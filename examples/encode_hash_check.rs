//! A/B check: print a hash of `encode_hashseq` bytes for each editing trace.
//! Run in two builds (e.g. HEAD worktree vs working tree) and diff the output
//! to prove an encoder refactor is byte-neutral.
use std::io::Read;

use hashseq::HashSeq;
use serde::Deserialize;

#[derive(Deserialize)]
struct TestPatch(usize, usize, String);

#[derive(Deserialize)]
struct TestTxn {
    patches: Vec<TestPatch>,
}

#[derive(Deserialize)]
struct TestData {
    txns: Vec<TestTxn>,
}

fn main() {
    let dir = std::path::Path::new("../editing-traces/sequential_traces");
    for name in [
        "automerge-paper.json.gz",
        "rustcode.json.gz",
        "sveltecomponent.json.gz",
        "seph-blog1.json.gz",
        "clownschool_flat.json.gz",
        "friendsforever_flat.json.gz",
        "json-crdt-blog-post.json.gz",
    ] {
        let path = dir.join(name);
        let file = std::fs::File::open(&path).expect("trace file");
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(file)
            .read_to_end(&mut raw)
            .expect("gunzip");
        let data: TestData = serde_json::from_slice(&raw).expect("json");

        let mut seq = HashSeq::default();
        for TestPatch(pos, del, ins) in data.txns.iter().flat_map(|t| t.patches.iter()) {
            if *del > 0 {
                seq.remove_batch(*pos, *del);
            }
            if !ins.is_empty() {
                seq.insert_batch(*pos, ins.chars());
            }
        }
        let bytes = hashseq::encoding::encode_hashseq(&seq);
        let digest = blake3::hash(&bytes);
        println!("{name}: {} bytes, blake3 {}", bytes.len(), digest.to_hex());
    }
}
