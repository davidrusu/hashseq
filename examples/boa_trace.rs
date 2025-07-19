use std::fs;
use std::time::Instant;

use boa_engine::{Context, JsString, Source};
use hashseq::HashSeq;

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

    // Now apply the trace to a HashSeq
    println!("\nApplying trace to HashSeq...");
    let trace_start = Instant::now();
    let mut seq = HashSeq::default();

    for (i, event) in trace.iter().enumerate() {
        if i % 50000 == 0 && i > 0 {
            let elapsed = trace_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("Progress: {}/{} ({:.0} edits/sec)", i, trace.len(), rate);
        }

        match event {
            Trace::Insert(idx, c) => {
                // Convert the string to a char (assuming single character)
                if let Some(ch) = c.chars().next() {
                    seq.insert(*idx, ch);
                }
            }
            Trace::Delete(idx) => {
                seq.remove(*idx);
            }
        }
    }

    let trace_elapsed = trace_start.elapsed();
    println!("Trace applied in: {trace_elapsed:?}");
    println!(
        "Average: {:.0} edits/sec",
        trace.len() as f64 / trace_elapsed.as_secs_f64()
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
    
    // Direct memory size of the HashSeq structure
    let seq_size = std::mem::size_of_val(&seq);
    println!("HashSeq struct size: {seq_size} bytes");
    
    // Estimate memory usage based on internal data structures
    let nodes_count = seq.nodes.len();
    let removed_count = seq.removed_inserts.len();
    println!("Nodes in HashSeq: {nodes_count}");
    println!("Removed inserts: {removed_count}");
    
    // Rough estimation: each node might use ~48-64 bytes (Id + HashNode + overhead)
    // This is a conservative estimate for heap-allocated data
    let estimated_node_size = 56; // bytes per node
    let estimated_memory = nodes_count * estimated_node_size + removed_count * 8; // 8 bytes per Id
    println!("Estimated memory usage: {estimated_memory} bytes");
    
    // Calculate overhead
    let overhead_ratio = estimated_memory as f64 / final_text_bytes as f64;
    println!("Memory overhead: {overhead_ratio:.2}x the final text size");
    println!("Overhead per character: {:.2} bytes", estimated_memory as f64 / reconstructed_text.len() as f64);

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
