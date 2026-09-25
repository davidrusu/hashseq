//! Value artifacts: content-addressed values (GRAMMAR_SPEC.md).
//!
//! A value artifact is a kind-tagged canonical byte encoding. Its identity
//! is `value_id`: an artifact of at most `IDENTITY_MAX` bytes is its own id
//! (`len ‖ artifact ‖ 0…`, the identity form), anything longer is
//! `BLAKE3::derive_key(VALUE_CONTEXT, artifact_bytes)`. Op payloads, map
//! keys, and map values commit to values by these ids (HASHSEQ_SPEC.md
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

/// Object kind tags — the byte inside `object_id`'s derivation and the
/// family wire's object framing. A separate namespace from value kinds:
/// objects are not values.
pub const KIND_KV: u8 = 0;
pub const KIND_SEQ: u8 = 1;

// Pre-hashed derive-key context keys: `Hasher::new_from_context_key` yields
// output identical to `Hasher::new_derive_key(ctx)` (same derive_key
// construction) but skips both re-hashing the context and cloning a ~1.9 KB
// template hasher — measured ~20 ns/op on the id hot path.
static VALUE_KEY: LazyLock<blake3::hazmat::ContextKey> =
    LazyLock::new(|| blake3::hazmat::hash_derive_key_context(VALUE_CONTEXT));

static OBJECT_KEY: LazyLock<blake3::hazmat::ContextKey> =
    LazyLock::new(|| blake3::hazmat::hash_derive_key_context(OBJECT_CONTEXT));

/// Artifacts this long or shorter are their own value id (the identity
/// form). The bound keeps at least 16 bytes of every identity id zero, so
/// the identity ids are at most ~2^120 of the 2^256 id space: landing a
/// BLAKE3 value id on one costs ≥ 2^128 work, BLAKE3's collision bound.
pub const IDENTITY_MAX: usize = 15;

/// `value_id` of raw canonical artifact bytes (tag ‖ payload): the identity
/// form when the artifact fits, BLAKE3 otherwise. Kind-agnostic — which
/// form applies depends on length alone, so a new small artifact kind
/// changes no existing id and an old replica derives new kinds' ids too.
pub fn value_id_of_bytes(artifact: &[u8]) -> Id {
    use blake3::hazmat::HasherExt;
    if let Some(id) = identity_id(artifact) {
        return id;
    }
    let mut hasher = blake3::Hasher::new_from_context_key(&VALUE_KEY);
    hasher.update(artifact);
    Id(*hasher.finalize().as_bytes())
}

/// The identity form, `len ‖ artifact ‖ 0^(31 − len)`, for
/// `1 ≤ len ≤ IDENTITY_MAX`.
#[inline]
fn identity_id(artifact: &[u8]) -> Option<Id> {
    let len = artifact.len();
    if !(1..=IDENTITY_MAX).contains(&len) {
        return None;
    }
    let mut id = [0u8; 32];
    id[0] = len as u8;
    id[1..1 + len].copy_from_slice(artifact);
    Some(Id(id))
}

/// The artifact an identity-form value id carries, or `None` for any other
/// id (a BLAKE3 value id, a node id, garbage). Total and pure: every
/// replica reads the same bytes out of the same id, holding nothing.
pub fn identity_artifact(id: &Id) -> Option<&[u8]> {
    let len = id.0[0] as usize;
    if !(1..=IDENTITY_MAX).contains(&len) {
        return None;
    }
    let (artifact, pad) = id.0[1..].split_at(len);
    pad.iter().all(|b| *b == 0).then_some(artifact)
}

/// Is `id` an identity-form value id? Such values are never stored: the id
/// is the bytes.
pub fn is_identity(id: &Id) -> bool {
    identity_artifact(id).is_some()
}

/// An object's store address:
/// `object_id(kind, origin) = derive_key(OBJECT_CONTEXT, kind_tag ‖ origin)`.
///
/// The origin is the op-level anchor (an arbitrary 32-byte value the
/// object's creator chose — often another op's id, which welds the new
/// object into that op's causal closure); the object id is the
/// store-level address (routing envelope + index) and never appears in a
/// preimage. The kind tag (`KIND_SEQ` / `KIND_KV`) is inside the
/// derivation, so the same origin opened as a Seq and as a Kv are two
/// *different* objects — kind mis-agreement is unrepresentable.
pub fn object_id(kind_tag: u8, seed: &Id) -> Id {
    use blake3::hazmat::HasherExt;
    let mut hasher = blake3::Hasher::new_from_context_key(&OBJECT_KEY);
    hasher.update(&[kind_tag]);
    hasher.update(&seed.0);
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
                let mut tmp = [0u8; 5];
                buf.extend_from_slice(char_artifact(*c, &mut tmp));
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
            _ => return None,
        })
    }

    pub fn encoded(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        self.encode(&mut buf);
        buf
    }

    /// The value an identity-form id carries (`None` for a hashed id, or
    /// an identity artifact of a kind this build does not know).
    pub fn from_identity_id(id: &Id) -> Option<Value> {
        Value::decode(identity_artifact(id)?)
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
        // The 10th byte carries the top bit only: anything more would be
        // shifted out (`[0xFF×9, 0x7F]` must not alias `i64::MIN`).
        if shift == 63 && b > 1 {
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

/// The char a value id names, if it is a char's (identity-form) id.
pub fn char_of_value_id(id: &Id) -> Option<char> {
    match Value::from_identity_id(id)? {
        Value::Char(c) => Some(c),
        _ => None,
    }
}

/// A char's canonical artifact bytes (`VK_CHAR ‖ utf8`), written into a
/// caller's stack buffer — the one layout `Value::encode`, `char_value_id`
/// and the wire's inline payload form all share.
#[inline]
pub(crate) fn char_artifact(c: char, tmp: &mut [u8; 5]) -> &[u8] {
    tmp[0] = VK_CHAR;
    let n = c.encode_utf8(&mut tmp[1..]).len();
    &tmp[..1 + n]
}

/// `value_id` of a char artifact: its identity form (≤ 5 bytes), no hash.
#[inline]
pub fn char_value_id(c: char) -> Id {
    // `identity_id(char_artifact(c))`, written in place (the typing path).
    let mut id = [0u8; 32];
    if c.is_ascii() {
        id[0] = 2;
        id[1] = VK_CHAR;
        id[2] = c as u8;
        return Id(id);
    }
    let n = c.encode_utf8(&mut id[2..6]).len();
    id[0] = 1 + n as u8;
    id[1] = VK_CHAR;
    Id(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The identity form round-trips every artifact that fits and never
    /// claims one that does not; the parser rejects every other layout.
    #[quickcheck_macros::quickcheck]
    fn prop_identity_form_is_exact(artifact: Vec<u8>) -> bool {
        let id = value_id_of_bytes(&artifact);
        match identity_artifact(&id) {
            Some(bytes) => (1..=IDENTITY_MAX).contains(&artifact.len()) && bytes == &artifact[..],
            None => !(1..=IDENTITY_MAX).contains(&artifact.len()),
        }
    }

    #[test]
    fn identity_parser_rejects_malformed_layouts() {
        let mut id = value_id_of_bytes(&[VK_INT, 0]).0;
        assert_eq!(identity_artifact(&Id(id)), Some(&[VK_INT, 0][..]));
        id[31] = 1; // non-zero padding
        assert_eq!(identity_artifact(&Id(id)), None);
        assert_eq!(identity_artifact(&Id([0; 32])), None, "len 0");
        let mut long = [0u8; 32];
        long[0] = IDENTITY_MAX as u8 + 1;
        assert_eq!(identity_artifact(&Id(long)), None, "len past the bound");
        // Trailing zero bytes inside the artifact are framed by the length.
        let a = value_id_of_bytes(&[VK_BYTES, 0]);
        let b = value_id_of_bytes(&[VK_BYTES]);
        assert_ne!(a, b);
        assert_eq!(Value::from_identity_id(&a), Some(Value::Bytes(vec![0])));
        assert_eq!(Value::from_identity_id(&b), Some(Value::Bytes(vec![])));
    }

    #[test]
    fn char_of_value_id_inverts_every_char_id() {
        for c in [
            '\u{0}',
            'a',
            '\u{7F}',
            '\u{80}',
            'é',
            '\u{D7FF}',
            '\u{E000}',
            '🦀',
            char::MAX,
        ] {
            assert_eq!(char_of_value_id(&char_value_id(c)), Some(c));
        }
        assert_eq!(
            char_of_value_id(&Value::String("é".into()).value_id()),
            None
        );
        assert_eq!(char_of_value_id(&Id([0; 32])), None);
    }

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
        ];
        for (i, a) in ids.iter().enumerate() {
            for b in &ids[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    #[test]
    fn char_value_id_matches_direct_derivation() {
        for c in ['a', 'Z', ' ', '\n', '\u{7f}', 'é', '🦀', '中'] {
            let direct = value_id_of_bytes(&Value::Char(c).encoded());
            assert_eq!(char_value_id(c), direct, "char id drift for {c:?}");
            assert_eq!(Value::Char(c).value_id(), direct);
        }
    }

    #[test]
    fn well_known_constants_are_derived() {
        assert_eq!(*TOMBSTONE, Value::Tombstone.value_id());
    }

    #[test]
    fn object_ids_are_distinct_from_their_origins() {
        let x = Id([7; 32]);
        let oid = object_id(KIND_SEQ, &x);
        assert_ne!(oid, x);
        // deterministic
        assert_eq!(oid, object_id(KIND_SEQ, &x));
        // the kind is inside the derivation: same origin, different object
        assert_ne!(oid, object_id(KIND_KV, &x));
    }

    #[test]
    fn zigzag_rejects_non_minimal() {
        // 0x80 0x00 encodes 0 non-minimally.
        assert!(decode_zigzag(&[0x80, 0x00]).is_none());
        assert!(decode_zigzag(&[0x00]).is_some());
    }

    /// One Value, one artifact id: an overflowing 10th byte must not decode
    /// to a value whose canonical bytes differ (two ids for `i64::MIN`).
    #[test]
    fn zigzag_rejects_overflowing_tenth_byte() {
        let mut bytes = vec![VK_INT];
        bytes.extend_from_slice(&[0xFF; 9]);
        bytes.push(0x7F);
        assert_eq!(Value::decode(&bytes), None);
        // Eleven bytes: the continuation runs past 64 bits.
        let mut long = vec![VK_INT];
        long.extend_from_slice(&[0x80; 10]);
        long.push(0x01);
        assert_eq!(Value::decode(&long), None);
        // The canonical form of i64::MIN is exactly 9×0xFF then 0x01.
        let min = Value::Int(i64::MIN).encoded();
        assert_eq!(
            &min[1..],
            &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]
        );
        assert_eq!(Value::decode(&min), Some(Value::Int(i64::MIN)));
    }
}
