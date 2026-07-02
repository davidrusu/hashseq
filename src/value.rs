//! Value artifacts: content-addressed values (GRAMMAR_SPEC.md).
//!
//! A value artifact is a kind-tagged canonical byte encoding; its identity is
//! `value_id = BLAKE3::derive_key(VALUE_CONTEXT, artifact_bytes)`. Op payloads,
//! map keys, and map values commit to values by these ids (HASHSEQ_SPEC.md
//! "Payload"): the preimage always carries the 32-byte id, while transport
//! inlines artifacts at or below the hash size.
//!
//! There is no length prefix inside an artifact — it is a leaf, hashed whole,
//! framed by whatever carries it.

use std::sync::LazyLock;

use crate::Id;

/// Domain-separation contexts (GRAMMAR_SPEC.md "Contexts and id functions").
/// One context per id class; kinds are tags inside the encodings. Bumping a
/// context string is an identity hard fork.
pub const NODE_CONTEXT: &str = "hashweb v1 node id";
pub const VALUE_CONTEXT: &str = "hashweb v1 value id";
pub const OBJECT_CONTEXT: &str = "hashweb v1 object id";

/// Value artifact kind tags (GRAMMAR_SPEC.md value artifact grammar).
pub const VK_TOMBSTONE: u8 = 0;
pub const VK_BOOL: u8 = 1;
pub const VK_INT: u8 = 2;
pub const VK_CHAR: u8 = 3;
pub const VK_STRING: u8 = 4;
pub const VK_BYTES: u8 = 5;
pub const VK_F64: u8 = 6;
pub const VK_NEW_SEQ: u8 = 7;
pub const VK_NEW_MAP: u8 = 8;

static VALUE_HASHER: LazyLock<blake3::Hasher> =
    LazyLock::new(|| blake3::Hasher::new_derive_key(VALUE_CONTEXT));

static OBJECT_HASHER: LazyLock<blake3::Hasher> =
    LazyLock::new(|| blake3::Hasher::new_derive_key(OBJECT_CONTEXT));

/// `value_id` of raw canonical artifact bytes (tag ‖ payload).
pub fn value_id_of_bytes(artifact: &[u8]) -> Id {
    let mut hasher = VALUE_HASHER.clone();
    hasher.update(artifact);
    Id(*hasher.finalize().as_bytes())
}

/// An object's origin id, derived from its creation op id
/// (GRAMMAR_SPEC.md: `object_id(X) = derive_key(OBJECT_CONTEXT, id(X))`).
/// The origin is a virtual node — never an op — so a creation op has no dual
/// role: refs to `X` mean the parent element, refs to `object_id(X)` mean the
/// child object.
pub fn object_id(creation_op: &Id) -> Id {
    let mut hasher = OBJECT_HASHER.clone();
    hasher.update(&creation_op.0);
    Id(*hasher.finalize().as_bytes())
}

/// A value artifact, in memory. Canonical bytes are `encode` below; identity
/// is `value_id()`. Artifacts at or below 32 encoded bytes ride inline on the
/// wire; identity is by id either way (transport never changes a preimage).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Tombstone,
    Bool(bool),
    Int(i64),
    Char(char),
    String(String),
    Bytes(Vec<u8>),
    /// IEEE 754 bits, verbatim — every bit pattern is a distinct value.
    F64(u64),
    NewSeq,
    NewMap,
}

impl Value {
    /// Canonical artifact bytes: `kind:varint ‖ payload`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Value::Tombstone => buf.push(VK_TOMBSTONE),
            Value::Bool(b) => {
                buf.push(VK_BOOL);
                buf.push(*b as u8);
            }
            Value::Int(i) => {
                buf.push(VK_INT);
                encode_zigzag(*i, buf);
            }
            Value::Char(c) => {
                buf.push(VK_CHAR);
                let mut tmp = [0u8; 4];
                buf.extend_from_slice(c.encode_utf8(&mut tmp).as_bytes());
            }
            Value::String(s) => {
                buf.push(VK_STRING);
                buf.extend_from_slice(s.as_bytes());
            }
            Value::Bytes(b) => {
                buf.push(VK_BYTES);
                buf.extend_from_slice(b);
            }
            Value::F64(bits) => {
                buf.push(VK_F64);
                buf.extend_from_slice(&bits.to_le_bytes());
            }
            Value::NewSeq => buf.push(VK_NEW_SEQ),
            Value::NewMap => buf.push(VK_NEW_MAP),
        }
    }

    /// Decode canonical artifact bytes (the exact framing-provided slice).
    /// Unknown kinds are the caller's concern (carry opaquely).
    pub fn decode(bytes: &[u8]) -> Option<Value> {
        let (&tag, rest) = bytes.split_first()?;
        Some(match tag {
            VK_TOMBSTONE if rest.is_empty() => Value::Tombstone,
            VK_BOOL => match rest {
                [0x00] => Value::Bool(false),
                [0x01] => Value::Bool(true),
                _ => return None,
            },
            VK_INT => {
                let (v, n) = decode_zigzag(rest)?;
                if n != rest.len() {
                    return None; // trailing bytes
                }
                Value::Int(v)
            }
            VK_CHAR => {
                let s = std::str::from_utf8(rest).ok()?;
                let mut chars = s.chars();
                let c = chars.next()?;
                if chars.next().is_some() {
                    return None; // exactly one scalar
                }
                Value::Char(c)
            }
            VK_STRING => Value::String(std::str::from_utf8(rest).ok()?.to_owned()),
            VK_BYTES => Value::Bytes(rest.to_vec()),
            VK_F64 => {
                let bits: [u8; 8] = rest.try_into().ok()?;
                Value::F64(u64::from_le_bytes(bits))
            }
            VK_NEW_SEQ if rest.is_empty() => Value::NewSeq,
            VK_NEW_MAP if rest.is_empty() => Value::NewMap,
            _ => return None,
        })
    }

    pub fn encoded(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        self.encode(&mut buf);
        buf
    }

    pub fn value_id(&self) -> Id {
        if let Value::Char(c) = self {
            return char_value_id(*c);
        }
        value_id_of_bytes(&self.encoded())
    }
}

fn encode_zigzag(v: i64, buf: &mut Vec<u8>) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    loop {
        let mut byte = (z & 0x7F) as u8;
        z >>= 7;
        if z != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if z == 0 {
            break;
        }
    }
}

fn decode_zigzag(bytes: &[u8]) -> Option<(i64, usize)> {
    let mut z: u64 = 0;
    let mut shift = 0u32;
    for (i, &b) in bytes.iter().enumerate() {
        if shift >= 64 {
            return None;
        }
        z |= ((b & 0x7F) as u64) << shift;
        if b & 0x80 == 0 {
            // minimal-form check: the last byte of a multibyte varint must be
            // non-zero (GRAMMAR_SPEC.md canonicality meta-rules).
            if i > 0 && b == 0 {
                return None;
            }
            let v = ((z >> 1) as i64) ^ -((z & 1) as i64);
            return Some((v, i + 1));
        }
        shift += 7;
    }
    None
}

// ---- well-known derived constants (computed, never magic ids) ----

/// `TOMBSTONE = value_id([VK_TOMBSTONE])`.
pub static TOMBSTONE: LazyLock<Id> = LazyLock::new(|| value_id_of_bytes(&[VK_TOMBSTONE]));
/// `NEW_SEQ = value_id([VK_NEW_SEQ])` — the seq-creation artifact.
pub static NEW_SEQ: LazyLock<Id> = LazyLock::new(|| value_id_of_bytes(&[VK_NEW_SEQ]));
/// `NEW_MAP = value_id([VK_NEW_MAP])` — the map-creation artifact.
pub static NEW_MAP: LazyLock<Id> = LazyLock::new(|| value_id_of_bytes(&[VK_NEW_MAP]));

// ---- char value-id cache ----
//
// Text is the hot path: every insert's preimage hashes its payload's value
// id, so char→value_id must be effectively free. Char artifacts are a fixed
// universe: ASCII rides a precomputed table; the rest go through a
// thread-local memo (one BLAKE3 of ≤5 bytes on first sight per thread).

static ASCII_VALUE_IDS: LazyLock<[Id; 128]> = LazyLock::new(|| {
    std::array::from_fn(|i| {
        let c = i as u8 as char;
        value_id_of_bytes(&Value::Char(c).encoded())
    })
});

thread_local! {
    static CHAR_MEMO: std::cell::RefCell<rustc_hash::FxHashMap<char, Id>> =
        std::cell::RefCell::new(rustc_hash::FxHashMap::default());
}

/// `value_id` of a char artifact, cached.
#[inline]
pub fn char_value_id(c: char) -> Id {
    if (c as u32) < 128 {
        return ASCII_VALUE_IDS[c as usize];
    }
    CHAR_MEMO.with(|m| {
        if let Some(id) = m.borrow().get(&c) {
            return *id;
        }
        let mut tmp = [0u8; 5];
        tmp[0] = VK_CHAR;
        let n = c.encode_utf8(&mut tmp[1..]).len();
        let id = value_id_of_bytes(&tmp[..1 + n]);
        m.borrow_mut().insert(c, id);
        id
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifacts_roundtrip() {
        let values = [
            Value::Tombstone,
            Value::Bool(false),
            Value::Bool(true),
            Value::Int(0),
            Value::Int(-1),
            Value::Int(i64::MAX),
            Value::Int(i64::MIN),
            Value::Char('a'),
            Value::Char('🦀'),
            Value::Char('\u{0}'),
            Value::String("hello".into()),
            Value::String(String::new()),
            Value::Bytes(vec![0, 1, 2, 255]),
            Value::Bytes(Vec::new()),
            Value::F64(1.5f64.to_bits()),
            Value::F64(f64::NAN.to_bits()),
            Value::NewSeq,
            Value::NewMap,
        ];
        for v in values {
            let bytes = v.encoded();
            let back = Value::decode(&bytes).expect("decodes");
            assert_eq!(back, v, "roundtrip failed for {v:?}");
        }
    }

    #[test]
    fn distinct_values_distinct_ids() {
        // String "a" and Char 'a' and Bytes [b'a'] are distinct values.
        let ids = [
            Value::Char('a').value_id(),
            Value::String("a".into()).value_id(),
            Value::Bytes(vec![b'a']).value_id(),
            Value::Tombstone.value_id(),
            Value::NewSeq.value_id(),
            Value::NewMap.value_id(),
        ];
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn char_cache_matches_direct_derivation() {
        for c in ['a', 'Z', ' ', '\n', '\u{7f}', 'é', '🦀', '中'] {
            let direct = value_id_of_bytes(&Value::Char(c).encoded());
            assert_eq!(char_value_id(c), direct, "cache drift for {c:?}");
            assert_eq!(Value::Char(c).value_id(), direct);
        }
    }

    #[test]
    fn well_known_constants_are_derived() {
        assert_eq!(*TOMBSTONE, Value::Tombstone.value_id());
        assert_eq!(*NEW_SEQ, Value::NewSeq.value_id());
        assert_eq!(*NEW_MAP, Value::NewMap.value_id());
        // and all distinct
        assert_ne!(*TOMBSTONE, *NEW_SEQ);
        assert_ne!(*NEW_SEQ, *NEW_MAP);
    }

    #[test]
    fn object_ids_are_distinct_from_their_creation_ops() {
        let x = Id([7; 32]);
        let oid = object_id(&x);
        assert_ne!(oid, x);
        // deterministic
        assert_eq!(oid, object_id(&x));
    }

    #[test]
    fn zigzag_rejects_non_minimal() {
        // 0x80 0x00 encodes 0 non-minimally.
        assert!(decode_zigzag(&[0x80, 0x00]).is_none());
        assert!(decode_zigzag(&[0x00]).is_some());
    }
}
