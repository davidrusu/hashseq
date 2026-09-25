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
        "727ed23ef98ab2cefcb93482e388c1681fb4320b4fb4e2d6d86c4b91b72b984a"
    );
    let remove = HashNode {
        pins: BTreeSet::new(),
        op: Op::Remove(BTreeSet::from_iter([insert.id()])),
    };
    assert_eq!(
        hx(&remove.id()),
        "45fb743e2035d8b2419358e3132e1639f6236581a84c2034ad1710f1b2f08e6b"
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
        "f1c0ee1237aaf7be2263f9674bb600e0d5e604cce6e7a63a3691de5aebcd487a"
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
        "517f394b1f2f4134a5096ac64e95d9891c2675e4c2039e377827817a0bfb2c93"
    );
    // Both value-field forms in one preimage: a hashed key (`0x20 ‖ id`)
    // and an identity-form value (`len ‖ artifact`).
    let put_long = HashNode {
        pins: BTreeSet::from_iter([origin]),
        op: Op::Put {
            key: Value::String("x".repeat(15)).value_id(),
            value: Value::String("x".repeat(14)).value_id(),
            overwrites: BTreeSet::new(),
        },
    };
    assert_eq!(
        hx(&put_long.id()),
        "bc713a67d53f9485334cbe2d44cfb193ff0c93a6c9354dd45a7dcf445918c47d"
    );
}
