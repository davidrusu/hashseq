//! GRAMMAR_SPEC.md test vectors — locked. Any change to these values is an
//! identity hard fork and must be deliberate (context-string bump).
use hashseq::value::{KIND_KV, KIND_SEQ, TOMBSTONE, Value, char_value_id};
use hashseq::{Anchor, HashNode, Id, Op, Payload, object_id};
use std::collections::BTreeSet;

fn hx(id: &Id) -> String {
    hex::encode(id.0)
}

#[test]
fn derived_constants_are_locked() {
    assert_eq!(
        hx(&TOMBSTONE),
        "0100000000000000000000000000000000000000000000000000000000000000"
    );
    assert_eq!(
        hx(&char_value_id('a')),
        "0203610000000000000000000000000000000000000000000000000000000000"
    );
    // The identity-form boundary: a 15-byte artifact is its own id, a
    // 16-byte one is hashed.
    assert_eq!(
        hx(&Value::String("x".repeat(14)).value_id()),
        "0f04787878787878787878787878787800000000000000000000000000000000"
    );
    assert_eq!(
        hx(&Value::String("x".repeat(15)).value_id()),
        "a9361588e0f7f0a7645285a5229fa64588d56d4ad5572efbf79d439d086e0b16"
    );
    assert_eq!(
        hx(&object_id(KIND_SEQ, &Id([0x11; 32]))),
        "dec2ca1db8abc0150e54eac174fdbf56a0ffeb833d83ba0d53eb91e4b063b58b"
    );
    assert_eq!(
        hx(&object_id(KIND_KV, &Id([0x11; 32]))),
        "d17caee6e539818d5cf8c5f5087d3e6ad43797cf2674b3196b3b4c0dc601f757"
    );
}

#[test]
fn node_preimages_are_locked() {
    let origin = Id([0x00; 32]);
    let insert = HashNode {
        pins: BTreeSet::new(),
        op: Op::Insert {
            at: Anchor::After(origin),
            payload: Payload::Char('a'),
        },
    };
    assert_eq!(
        hx(&insert.id()),
        "d3a27cd3533aa80075c856bc33d5f2a6faee839be84506626594ac4322dcdfa2"
    );
    let remove = HashNode {
        pins: BTreeSet::new(),
        op: Op::Remove(BTreeSet::from_iter([insert.id()])),
    };
    assert_eq!(
        hx(&remove.id()),
        "1f739bfc1cd26ce72f410f6af7d62b75e4e75cc99bac90973b5539070cafef3e"
    );
    let mv = HashNode {
        pins: BTreeSet::new(),
        op: Op::Move {
            target: insert.id(),
            to: Anchor::Before(origin),
            overwrites: BTreeSet::new(),
        },
    };
    assert_eq!(
        hx(&mv.id()),
        "9e6e16085d8ff7d374c1f81f363d4190244ad446899da61a157b360ec019a621"
    );
    let put = HashNode {
        pins: BTreeSet::from_iter([origin]),
        op: Op::Put {
            key: char_value_id('k'),
            value: *TOMBSTONE,
            overwrites: BTreeSet::new(),
        },
    };
    assert_eq!(
        hx(&put.id()),
        "4533c5edf5b7c7cdd956eb76f39dc8cbc4290d375010ec97bfd095c659e4ce4d"
    );
}
