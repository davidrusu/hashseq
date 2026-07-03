// examples/grammar_vectors.rs — print the GRAMMAR_SPEC test vectors
use hashseq::value::{NEW_KV, NEW_SEQ, TOMBSTONE, char_value_id};
use hashseq::{Anchor, HashNode, Id, Op, Payload, object_id};
use std::collections::BTreeSet;

fn hex(id: &Id) -> String {
    ::hex::encode(id.0)
}

fn main() {
    println!("TOMBSTONE = {}", hex(&TOMBSTONE));
    println!("NEW_SEQ   = {}", hex(&NEW_SEQ));
    println!("NEW_KV   = {}", hex(&NEW_KV));
    println!("value_id('a') = {}", hex(&char_value_id('a')));
    let x = Id([0x11; 32]);
    println!("object_id(0x11*32) = {}", hex(&object_id(&x)));

    let origin = Id([0x00; 32]);
    let insert = HashNode {
        pins: BTreeSet::new(),
        op: Op::Insert {
            at: Anchor::After(origin),
            payload: Payload::Char('a'),
        },
    };
    println!("insert_a_after_zero_origin = {}", hex(&insert.id()));
    let remove = HashNode {
        pins: BTreeSet::new(),
        op: Op::Remove(BTreeSet::from_iter([insert.id()])),
    };
    println!("remove_of_that_insert      = {}", hex(&remove.id()));
    let mv = HashNode {
        pins: BTreeSet::new(),
        op: Op::Move {
            target: insert.id(),
            to: Anchor::Before(origin),
            overwrites: BTreeSet::new(),
        },
    };
    println!("move_of_that_insert        = {}", hex(&mv.id()));
    let put = HashNode {
        pins: BTreeSet::from_iter([origin]),
        op: Op::Put {
            key: char_value_id('k'),
            value: *TOMBSTONE,
            overwrites: BTreeSet::new(),
        },
    };
    println!("put_k_tombstone_pin_zero   = {}", hex(&put.id()));
}
