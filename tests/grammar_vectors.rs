//! GRAMMAR_SPEC.md test vectors — locked. Any change to these values is an
//! identity hard fork and must be deliberate (context-string bump).
use hashseq::value::{NEW_KV, NEW_SEQ, TOMBSTONE, VK_NEW_KV, VK_NEW_SEQ, char_value_id};
use hashseq::{Anchor, HashNode, Id, Op, Payload, object_id};
use std::collections::BTreeSet;

fn hx(id: &Id) -> String {
    hex::encode(id.0)
}

#[test]
fn derived_constants_are_locked() {
    assert_eq!(
        hx(&TOMBSTONE),
        "37e7b9a9496baa6bc45fc76168e02a70e2b640a7ae2ca826fb5990f48f772f8a"
    );
    assert_eq!(
        hx(&NEW_SEQ),
        "8fff7f38a876c8f8dc821a2acd0027539f496194f366d1c7401f2e1d765d0ef7"
    );
    assert_eq!(
        hx(&NEW_KV),
        "76796526efce6c555148595918fd9cf934753cd7e06f8f22e26b7f2501c60e26"
    );
    assert_eq!(
        hx(&char_value_id('a')),
        "555c4ad3f1f89bacc6d46a3d7c6cf897f83e8c0500da8f2dc9a46fc85a740638"
    );
    assert_eq!(
        hx(&object_id(VK_NEW_SEQ, &Id([0x11; 32]))),
        "175531dbcc017f332d4b2f3e2903100ec7990a25d61c65446e1899cda75d2932"
    );
    assert_eq!(
        hx(&object_id(VK_NEW_KV, &Id([0x11; 32]))),
        "638a668baf72db186b5874f128314deddf1c7148ea644ca10dc87bb28ad885c8"
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
        "796e3d6b9739303167ce099a5e801545aee245227e1d0c483592fc839a3e66d2"
    );
    let remove = HashNode {
        pins: BTreeSet::new(),
        op: Op::Remove(BTreeSet::from_iter([insert.id()])),
    };
    assert_eq!(
        hx(&remove.id()),
        "d4758f38bc31acaafd5412c51024fb487b71fe85ac212775337cc32b033054c3"
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
        "7380372f8478f820e04005cc1623df522408f612934db03ded6b9e27a35d3ffb"
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
        "d6a4f360e484441bec208b2510bf4412b74b2f8ea258f09275799ae485cb80a5"
    );
}
