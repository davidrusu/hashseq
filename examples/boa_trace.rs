use std::fs;
use std::io::Write;
use std::time::Instant;

use boa_engine::{Context, JsString, Source};
use flate2::Compression;
use flate2::write::GzEncoder;
use hashseq::{encode_hashseq, HashSeq};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

#[derive(Debug)]
enum Trace {
    Insert(usize, String),
    Delete(usize),
}

fn main() {
    let start_time = Instant::now();

    // Read the JavaScript file
    println!("Reading JavaScript file...");
    let read_start = Instant::now();
    let js_content = fs::read_to_string("../automerge-perf/edit-by-index/editing-trace.js")
        .expect("Failed to read editing-trace.js");
    println!("File read in: {:?}", read_start.elapsed());
    println!("File size: {} bytes", js_content.len());

    // Create a new JavaScript context
    let mut context = Context::default();

    // Since the file uses const declarations, we need to make them global
    // by wrapping them or evaluating them differently
    let wrapped_js = format!(
        "{js_content}
        // Make variables accessible globally
        globalThis.edits = edits;
        globalThis.finalText = finalText;
        "
    );

    // Evaluate the JavaScript code
    println!("\nEvaluating JavaScript code...");
    let eval_start = Instant::now();
    context
        .eval(Source::from_bytes(&wrapped_js))
        .expect("Failed to evaluate JavaScript");
    println!("JavaScript evaluated in: {:?}", eval_start.elapsed());

    // Extract variables and parse data
    println!("\nExtracting and parsing data...");
    let parse_start = Instant::now();

    // Extract the 'edits' variable
    let edits_value = context
        .global_object()
        .get(JsString::from("edits"), &mut context)
        .expect("Failed to get 'edits' variable");

    // Extract the 'finalText' variable
    let final_text_value = context
        .global_object()
        .get(JsString::from("finalText"), &mut context)
        .expect("Failed to get 'finalText' variable");

    // Convert finalText to a Rust String
    let final_text = final_text_value
        .as_string()
        .expect("finalText should be a string")
        .to_std_string()
        .expect("Failed to convert to std string");

    // Convert edits array to Rust Vec<Trace>
    let edits_array = edits_value.as_object().expect("edits should be an object");

    // Get array length using JS property access
    let length_value = edits_array
        .get(JsString::from("length"), &mut context)
        .expect("Failed to get array length");

    let length = length_value
        .to_number(&mut context)
        .expect("Length should be a number") as usize;

    let mut trace: Vec<Trace> = Vec::new();

    for i in 0..length {
        let edit = edits_array
            .get(i as u32, &mut context)
            .expect("Failed to get array element");

        let edit_array = edit.as_object().expect("edit should be an array");

        let edit_length_value = edit_array
            .get(JsString::from("length"), &mut context)
            .expect("Failed to get edit array length");

        let edit_length = edit_length_value
            .to_number(&mut context)
            .expect("Edit length should be a number") as usize;

        if edit_length == 3 {
            // This is an insert operation [index, _, char]
            let index = edit_array
                .get(0u32, &mut context)
                .expect("Failed to get index")
                .to_number(&mut context)
                .expect("Index should be a number") as usize;

            let char_value = edit_array
                .get(2u32, &mut context)
                .expect("Failed to get character");

            let character = char_value
                .as_string()
                .expect("Character should be a string")
                .to_std_string()
                .expect("Failed to convert to std string");

            trace.push(Trace::Insert(index, character));
        } else if edit_length == 2 {
            // This is a delete operation [index, _]
            let index = edit_array
                .get(0u32, &mut context)
                .expect("Failed to get index")
                .to_number(&mut context)
                .expect("Index should be a number") as usize;

            trace.push(Trace::Delete(index));
        }
    }

    println!("Data parsed in: {:?}", parse_start.elapsed());
    println!("Number of edits: {}", trace.len());

    // Count operation types
    let inserts = trace.iter().filter(|t| matches!(t, Trace::Insert(_, _))).count();
    let deletes = trace.iter().filter(|t| matches!(t, Trace::Delete(_))).count();
    println!("Inserts: {}, Deletes: {} ({:.1}% deletes)", inserts, deletes, 100.0 * deletes as f64 / trace.len() as f64);

    // Now apply the trace to a HashSeq
    println!("\nApplying trace to HashSeq...");
    let trace_start = Instant::now();
    let mut seq = HashSeq::default();

    let repeats = 1;
    for n in 0..repeats {
        for (i, event) in trace.iter().enumerate() {
            if i % 50000 == 0 && i > 0 {
                let elapsed = trace_start.elapsed();
                let rate = (i + trace.len() * n) as f64 / elapsed.as_secs_f64();
                println!(
                    "Progress: {}/{} ({:.0} edits/sec)",
                    (i + trace.len() * n),
                    trace.len() * repeats,
                    rate
                );
            }

            match event {
                Trace::Insert(idx, c) => {
                    seq.insert_batch(*idx, c.chars());
                }
                Trace::Delete(idx) => {
                    seq.remove(*idx);
                }
            }
        }
    }

    let trace_elapsed = trace_start.elapsed();
    println!("Trace applied in: {trace_elapsed:?}");
    println!(
        "Average: {:.0} edits/sec",
        (trace.len() * repeats) as f64 / trace_elapsed.as_secs_f64()
    );

    // Verify the result
    println!("\nVerifying result...");
    let verify_start = Instant::now();
    let reconstructed_text = String::from_iter(seq.iter());
    println!("Text reconstructed in: {:?}", verify_start.elapsed());

    // Memory usage analysis
    println!("\nMemory usage:");
    let final_text_bytes = reconstructed_text.len();
    println!("Final text: {final_text_bytes} bytes");

    // Measure memory usage before and then after drop (Archimedes principle)
    let memory_before = GLOBAL.stats().bytes_allocated;
    let seq2 = seq.clone();
    let memory_after = GLOBAL.stats().bytes_allocated;
    let estimated_memory = memory_after.saturating_sub(memory_before);
    if seq2.len() > seq.len() {
        println!("bad");
    }
    // Calculate in-memory overhead
    println!("Hashseq memory usage: {} bytes", estimated_memory);
    let overhead_ratio = estimated_memory as f64 / final_text_bytes as f64;
    println!("Memory overhead: {overhead_ratio:.2}x the final text size");
    println!(
        "Overhead per character: {:.2} bytes",
        estimated_memory as f64 / reconstructed_text.len() as f64
    );

    // Run size distribution
    println!("\nRun size distribution:");
    let mut run_sizes: Vec<usize> = seq.runs.values().map(|r| r.len()).collect();
    run_sizes.sort();
    let num_runs = run_sizes.len();
    let total_chars_in_runs: usize = run_sizes.iter().sum();
    println!("Number of runs: {}", num_runs);
    if !run_sizes.is_empty() {
        let percentile = |p: f64| -> usize {
            let idx = ((num_runs as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
            run_sizes[idx.min(num_runs - 1)]
        };
        println!("  100%   (max): {} chars", run_sizes[num_runs - 1]);
        println!("  99.9%:        {} chars", percentile(99.9));
        println!("  99.5%:        {} chars", percentile(99.5));
        println!("  99%:          {} chars", percentile(99.0));
        println!("  50%  (median): {} chars", percentile(50.0));
        println!("  min:          {} chars", run_sizes[0]);
        println!("Total chars in runs: {}", total_chars_in_runs);
        println!("Avg run size: {:.2} chars", total_chars_in_runs as f64 / num_runs as f64);
    }

    // Analyze run mergeability - how many runs could be "unsplit"
    println!("\nRun merge analysis:");
    // Build a map from last element of each run to the run
    let mut last_elem_to_run: std::collections::HashMap<hashseq::Id, &hashseq::Run> =
        std::collections::HashMap::new();
    for run in seq.runs.values() {
        last_elem_to_run.insert(run.last_id(), run);
    }

    // Count runs whose insert_after is the last element of another run
    // AND has no extra dependencies (simple chain)
    let mut mergeable_runs = 0;
    let mut mergeable_chars = 0;
    let mut chain_lengths: Vec<usize> = Vec::new();

    for run in seq.runs.values() {
        // Check if this run's insert_after points to the last elem of another run
        // AND this run has no extra dependencies on its first element
        if run.first_extra_deps.is_empty() {
            if let Some(parent_run) = last_elem_to_run.get(&run.insert_after) {
                // This run could potentially be merged with parent_run
                mergeable_runs += 1;
                mergeable_chars += run.len();
            }
        }
    }

    // Calculate chain lengths (how deep can we merge)
    for run in seq.runs.values() {
        // Only count chains starting from runs that are NOT children of other runs
        let is_child = run.first_extra_deps.is_empty()
            && last_elem_to_run.contains_key(&run.insert_after);
        if is_child {
            continue; // Skip, we'll count from the root
        }

        // Walk down the chain
        let mut chain_len = run.len();
        let mut current_last = run.last_id();

        // Find children that continue this chain
        loop {
            let mut found_child = false;
            for child_run in seq.runs.values() {
                if child_run.insert_after == current_last && child_run.first_extra_deps.is_empty() {
                    chain_len += child_run.len();
                    current_last = child_run.last_id();
                    found_child = true;
                    break;
                }
            }
            if !found_child {
                break;
            }
        }
        chain_lengths.push(chain_len);
    }

    chain_lengths.sort();
    println!("Mergeable runs: {} ({:.1}% of runs)", mergeable_runs, 100.0 * mergeable_runs as f64 / num_runs as f64);
    println!("Mergeable chars: {} ({:.1}% of chars in runs)", mergeable_chars, 100.0 * mergeable_chars as f64 / total_chars_in_runs as f64);

    if !chain_lengths.is_empty() {
        let total_chains = chain_lengths.len();
        let merged_total: usize = chain_lengths.iter().sum();
        println!("After merging: {} chains (was {} runs)", total_chains, num_runs);
        println!("Potential ID savings: {} IDs ({} bytes)",
            num_runs - total_chains,
            (num_runs - total_chains) * 32);

        let chain_percentile = |p: f64| -> usize {
            let idx = ((total_chains as f64 * p / 100.0).ceil() as usize).saturating_sub(1);
            chain_lengths[idx.min(total_chains - 1)]
        };
        println!("Merged chain sizes:");
        println!("  100%   (max): {} chars", chain_lengths[total_chains - 1]);
        println!("  99%:          {} chars", chain_percentile(99.0));
        println!("  50%  (median): {} chars", chain_percentile(50.0));
        println!("  Avg:          {:.2} chars", merged_total as f64 / total_chains as f64);

        // Estimate encoded size with merged runs
        // Per run overhead: 1 (tag) + 32 (insert_after) + 1 (deps len=0) + varint(str len) + str
        // Savings = eliminated_runs * ~34 bytes overhead
        let eliminated_runs = num_runs - total_chains;
        let estimated_savings = eliminated_runs * 34; // Conservative estimate
        println!("Estimated encoding savings: ~{} bytes ({:.1}% reduction)",
            estimated_savings,
            100.0 * estimated_savings as f64 / 5975779.0); // Using known encoded size
    }

    // Encoded size analysis
    println!("\nEncoded size:");
    let encode_start = Instant::now();
    let encoded = encode_hashseq(&seq);
    let encode_elapsed = encode_start.elapsed();
    let encoded_size = encoded.len();
    println!("Encoded in: {:?}", encode_elapsed);
    println!("Encoded size: {} bytes", encoded_size);
    let encoded_overhead_ratio = encoded_size as f64 / final_text_bytes as f64;
    println!("Encoded overhead: {encoded_overhead_ratio:.2}x the final text size");
    println!(
        "Encoded overhead per character: {:.2} bytes",
        encoded_size as f64 / reconstructed_text.len() as f64
    );

    // Byte breakdown by parsing the actual encoded bytes
    println!("\nEncoding breakdown (measured):");

    // Helper to decode varint and return (value, bytes_consumed)
    let decode_varint = |bytes: &[u8]| -> (usize, usize) {
        let mut result: usize = 0;
        let mut shift = 0;
        let mut pos = 0;
        loop {
            let byte = bytes[pos];
            pos += 1;
            result |= ((byte & 0x7F) as usize) << shift;
            if byte & 0x80 == 0 {
                return (result, pos);
            }
            shift += 7;
        }
    };

    // Helper to skip an IdSet and return bytes consumed
    let skip_id_set = |bytes: &[u8]| -> usize {
        let (count, mut pos) = decode_varint(bytes);
        pos += count * 32; // each ID is 32 bytes
        pos
    };

    // Helper to skip a UTF-8 char and return bytes consumed
    let skip_utf8_char = |bytes: &[u8]| -> usize {
        match bytes[0] {
            0x00..=0x7F => 1,
            0xC0..=0xDF => 2,
            0xE0..=0xEF => 3,
            0xF0..=0xF7 => 4,
            _ => 1,
        }
    };

    // Helper to skip a string and return bytes consumed
    let skip_string = |bytes: &[u8]| -> usize {
        let (len, varint_size) = decode_varint(bytes);
        varint_size + len
    };

    let mut pos = 0;
    let bytes = &encoded[..];

    // Parse roots section
    let roots_start = pos;
    let (num_roots, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_roots {
        pos += skip_id_set(&bytes[pos..]); // extra_deps
        pos += skip_utf8_char(&bytes[pos..]); // char
    }
    let roots_total = pos - roots_start;

    // Parse runs section
    let runs_start = pos;
    let (num_runs_encoded, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_runs_encoded {
        pos += 32; // insert_after ID
        pos += skip_id_set(&bytes[pos..]); // first_extra_deps
        pos += skip_string(&bytes[pos..]); // run string
    }
    let runs_total = pos - runs_start;

    // Parse befores section
    let befores_start = pos;
    let (num_befores, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_befores {
        pos += skip_id_set(&bytes[pos..]); // extra_deps
        pos += 32; // anchor ID
        pos += skip_utf8_char(&bytes[pos..]); // char
    }
    let befores_total = pos - befores_start;

    // Parse forward remove runs section: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    let removes_start = pos;
    let forward_runs_start = pos;
    let (num_forward_runs, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_forward_runs {
        pos += skip_id_set(&bytes[pos..]); // first_extra_deps
        let (_, size) = decode_varint(&bytes[pos..]); // run_idx
        pos += size;
        let (_, size) = decode_varint(&bytes[pos..]); // start_idx
        pos += size;
        let (_, size) = decode_varint(&bytes[pos..]); // end_idx
        pos += size;
    }
    let forward_runs_total = pos - forward_runs_start;

    // Parse backward remove runs section: [count][first_extra_deps, run_idx, start_idx, end_idx]...
    let backward_runs_start = pos;
    let (num_backward_runs, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_backward_runs {
        pos += skip_id_set(&bytes[pos..]); // first_extra_deps
        let (_, size) = decode_varint(&bytes[pos..]); // run_idx
        pos += size;
        let (_, size) = decode_varint(&bytes[pos..]); // start_idx
        pos += size;
        let (_, size) = decode_varint(&bytes[pos..]); // end_idx
        pos += size;
    }
    let backward_runs_total = pos - backward_runs_start;
    let num_remove_runs = num_forward_runs + num_backward_runs;

    // Parse single-run removes: [count][extra_deps, run_idx, elem_idx]...
    let single_run_start = pos;
    let (num_single_run, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_single_run {
        pos += skip_id_set(&bytes[pos..]); // extra_deps
        let (_, size) = decode_varint(&bytes[pos..]); // run_idx
        pos += size;
        let (_, size) = decode_varint(&bytes[pos..]); // elem_idx
        pos += size;
    }
    let single_run_total = pos - single_run_start;

    // Parse before removes: [count][extra_deps, before_idx]...
    let before_removes_start = pos;
    let (num_before_removes, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_before_removes {
        pos += skip_id_set(&bytes[pos..]); // extra_deps
        let (_, size) = decode_varint(&bytes[pos..]); // before_idx
        pos += size;
    }
    let before_removes_total = pos - before_removes_start;

    // Parse root removes: [count][extra_deps, root_idx]...
    let root_removes_start = pos;
    let (num_root_removes, size) = decode_varint(&bytes[pos..]);
    pos += size;
    for _ in 0..num_root_removes {
        pos += skip_id_set(&bytes[pos..]); // extra_deps
        let (_, size) = decode_varint(&bytes[pos..]); // root_idx
        pos += size;
    }
    let root_removes_total = pos - root_removes_start;
    let num_standalone = num_single_run + num_before_removes + num_root_removes;
    let removes_total = pos - removes_start;

    // Parse orphans section
    let orphans_start = pos;
    let (num_orphans, size) = decode_varint(&bytes[pos..]);
    pos += size;
    // Skip orphan parsing for now (usually 0)
    let orphans_total = pos - orphans_start;

    println!("  Roots:        {:>10} bytes ({:>5.1}%) - {} nodes",
        roots_total, 100.0 * roots_total as f64 / encoded_size as f64, num_roots);
    println!("  Runs:         {:>10} bytes ({:>5.1}%) - {} runs, {} chars",
        runs_total, 100.0 * runs_total as f64 / encoded_size as f64,
        num_runs_encoded, total_chars_in_runs);
    println!("  Befores:      {:>10} bytes ({:>5.1}%) - {} nodes",
        befores_total, 100.0 * befores_total as f64 / encoded_size as f64, num_befores);
    println!("  Removes:      {:>10} bytes ({:>5.1}%)",
        removes_total, 100.0 * removes_total as f64 / encoded_size as f64);
    println!("    ├─ fwd runs:  {:>10} bytes ({:>5.1}%) - {} runs",
        forward_runs_total, 100.0 * forward_runs_total as f64 / encoded_size as f64, num_forward_runs);
    println!("    ├─ bwd runs:  {:>10} bytes ({:>5.1}%) - {} runs",
        backward_runs_total, 100.0 * backward_runs_total as f64 / encoded_size as f64, num_backward_runs);
    println!("    ├─ single:    {:>10} bytes ({:>5.1}%) - {} removes",
        single_run_total, 100.0 * single_run_total as f64 / encoded_size as f64, num_single_run);
    println!("    ├─ befores:   {:>10} bytes ({:>5.1}%) - {} removes",
        before_removes_total, 100.0 * before_removes_total as f64 / encoded_size as f64, num_before_removes);
    println!("    └─ roots:     {:>10} bytes ({:>5.1}%) - {} removes",
        root_removes_total, 100.0 * root_removes_total as f64 / encoded_size as f64, num_root_removes);
    println!("  Orphans:      {:>10} bytes ({:>5.1}%) - {} nodes",
        orphans_total, 100.0 * orphans_total as f64 / encoded_size as f64, num_orphans);
    println!("  ─────────────────────────────────────");
    println!("  Total:        {:>10} bytes (parsed: {}, actual: {})",
        roots_total + runs_total + befores_total + removes_total + orphans_total,
        pos, encoded_size);

    // Test gzip compression
    println!("\nGzip compression test:");
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&encoded).unwrap();
    let gzipped = encoder.finish().unwrap();
    println!("  Encoded + gzip:    {:>10} bytes ({:.1}x encoded)", gzipped.len(), encoded_size as f64 / gzipped.len() as f64);

    // Compare to just gzipping the final text
    let mut text_encoder = GzEncoder::new(Vec::new(), Compression::default());
    text_encoder.write_all(final_text.as_bytes()).unwrap();
    let text_gzipped = text_encoder.finish().unwrap();
    println!("  Final text + gzip: {:>10} bytes ({:.1}x text)", text_gzipped.len(), final_text_bytes as f64 / text_gzipped.len() as f64);
    println!("  Overhead vs gzipped text: {:.2}x", gzipped.len() as f64 / text_gzipped.len() as f64);

    println!("\nResults:");
    println!("Original length: {} characters", final_text.len());
    println!(
        "Reconstructed length: {} characters",
        reconstructed_text.len()
    );
    println!("Texts match: {}", reconstructed_text == final_text);

    if reconstructed_text != final_text {
        // Find the first difference
        let chars1: Vec<char> = final_text.chars().collect();
        let chars2: Vec<char> = reconstructed_text.chars().collect();

        for (i, (c1, c2)) in chars1.iter().zip(chars2.iter()).enumerate() {
            if c1 != c2 {
                println!("First difference at position {i}: '{c1}' vs '{c2}'");
                break;
            }
        }
    }

    println!("\nTotal time: {:?}", start_time.elapsed());
}
