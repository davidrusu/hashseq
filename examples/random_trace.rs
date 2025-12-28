use std::time::Instant;

use ::hashseq::HashSeq;
use rand::{Rng, RngCore};
use stats_alloc::{INSTRUMENTED_SYSTEM, StatsAlloc};

#[global_allocator]
static GLOBAL: &StatsAlloc<std::alloc::System> = &INSTRUMENTED_SYSTEM;

#[derive(Debug)]
enum Trace {
    Insert(usize, char),
    Delete(usize),
}

fn random_trace(length: usize) -> (String, Vec<Trace>) {
    let mut rng = rand::thread_rng();
    let mut content = String::new();
    let mut trace = Vec::with_capacity(length);

    for _ in 0..length {
        match rng.next_u32() % 2 {
            _ if content.is_empty() => {
                // insert since content is empty
                let c = rng.sample(rand::distributions::Alphanumeric) as char;
                content.push(c);
                trace.push(Trace::Insert(0, c));
            }
            0 => {
                // delete
                let pos = rng.next_u32() as usize % content.len();
                content.remove(pos);
                trace.push(Trace::Delete(pos));
            }
            1 => {
                // insert
                let pos = rng.next_u32() as usize % content.len();
                let c = rng.sample(rand::distributions::Alphanumeric) as char;
                content.insert(pos, c);
                trace.push(Trace::Insert(pos, c));
            }
            _ => unreachable!(),
        }
    }

    (content, trace)
}

fn main() {
    let start_time = Instant::now();
    let length = 100_000;

    println!("Generating random trace of {length} operations...");
    let gen_start = Instant::now();
    let (expected_content, trace) = random_trace(length);
    let gen_elapsed = gen_start.elapsed();
    println!("Trace generated in: {gen_elapsed:?}");
    println!(
        "Expected final string length: {} characters",
        expected_content.len()
    );

    println!("\nApplying trace to HashSeq...");
    let trace_start = Instant::now();
    let mut seq = HashSeq::default();

    for (i, event) in trace.iter().enumerate() {
        if i % 25000 == 0 && i > 0 {
            let elapsed = trace_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("Progress: {}/{} ({:.0} edits/sec)", i, trace.len(), rate);
        }

        match event {
            Trace::Insert(idx, c) => seq.insert(*idx, *c),
            Trace::Delete(idx) => seq.remove(*idx),
        }
    }

    let trace_elapsed = trace_start.elapsed();
    println!("Trace applied in: {trace_elapsed:?}");
    println!(
        "Average: {:.0} edits/sec",
        trace.len() as f64 / trace_elapsed.as_secs_f64()
    );

    println!("\nVerifying result...");
    let verify_start = Instant::now();
    let result = String::from_iter(seq.iter());
    println!("Text reconstructed in: {:?}", verify_start.elapsed());

    // Memory usage analysis
    println!("\nMemory usage:");
    let final_text_bytes = result.len();
    println!("Final text: {final_text_bytes} bytes");

    // Measure memory usage by cloning and measuring allocation delta
    let memory_before = GLOBAL.stats().bytes_allocated;
    let seq2 = seq.clone();
    let memory_after = GLOBAL.stats().bytes_allocated;
    let measured_memory = memory_after.saturating_sub(memory_before);
    // Use seq2 to prevent optimization from eliding the clone
    if seq2.len() > seq.len() {
        println!("bad");
    }

    println!("HashSeq memory usage: {} bytes", measured_memory);
    let overhead_ratio = measured_memory as f64 / final_text_bytes as f64;
    println!("Memory overhead: {overhead_ratio:.2}x the final text size");
    println!(
        "Overhead per character: {:.2} bytes",
        measured_memory as f64 / result.len() as f64
    );

    println!("\nResults:");
    println!("Expected length: {} characters", expected_content.len());
    println!("Result length: {} characters", result.len());
    println!("Strings match: {}", expected_content == result);

    if expected_content != result {
        // Find the first difference
        for (i, (c1, c2)) in expected_content.chars().zip(result.chars()).enumerate() {
            if c1 != c2 {
                println!("First difference at position {i}: '{c1}' vs '{c2}'");
                break;
            }
        }
    }

    println!("\nTotal time: {:?}", start_time.elapsed());
}
