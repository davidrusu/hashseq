use ::hashseq::HashSeq;

fn main() {
    let mut seq = HashSeq::default();

    seq.insert(0, 'a');
    seq.insert(1, 'b');
    seq.insert(2, 'c');

    let result: String = seq.iter().collect();
    assert_eq!(result, "abc");
    println!("result of inserting 'a', 'b', 'c': {}", result);
}
