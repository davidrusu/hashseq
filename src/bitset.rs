//! A compact growable bitset — one bit per node handle, packed into `u64`
//! words. Replaces the per-handle `Vec<bool>` tombstone map (8× smaller).
//!
//! Bits are append-only and, once set, never cleared (tombstones are
//! monotonic), so the API is just `push` / `set` / `get`. Indexing out of
//! range panics, matching `Vec<bool>` indexing.

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BitSet {
    words: Vec<u64>,
    len: usize,
}

impl BitSet {
    /// Append one bit, growing the set by one.
    #[inline]
    pub fn push(&mut self, bit: bool) {
        if self.len.is_multiple_of(64) {
            self.words.push(0);
        }
        let i = self.len;
        self.len += 1;
        if bit {
            self.set(i);
        }
    }

    /// Set bit `i` to 1. Panics if `i` is out of range.
    #[inline]
    pub fn set(&mut self, i: usize) {
        assert!(i < self.len, "BitSet index {i} out of range (len {})", self.len);
        self.words[i / 64] |= 1u64 << (i % 64);
    }

    /// Whether bit `i` is set. Panics if `i` is out of range.
    #[inline]
    pub fn get(&self, i: usize) -> bool {
        assert!(i < self.len, "BitSet index {i} out of range (len {})", self.len);
        (self.words[i / 64] >> (i % 64)) & 1 == 1
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_get_set_across_word_boundaries() {
        let mut b = BitSet::default();
        // Push a pattern that straddles several u64 words.
        let pattern: Vec<bool> = (0..200).map(|i| i % 7 == 0).collect();
        for &bit in &pattern {
            b.push(bit);
        }
        assert_eq!(b.len(), 200);
        for (i, &bit) in pattern.iter().enumerate() {
            assert_eq!(b.get(i), bit, "bit {i}");
        }
        // Setting is monotonic and idempotent.
        b.set(1);
        b.set(1);
        assert!(b.get(1));
        assert!(!b.get(2));
    }

    #[test]
    #[should_panic]
    fn get_out_of_range_panics() {
        let mut b = BitSet::default();
        b.push(true);
        b.get(1);
    }

    #[test]
    fn matches_vec_bool_under_random_ops() {
        // BitSet must behave exactly like the Vec<bool> it replaces.
        let mut bits = BitSet::default();
        let mut model: Vec<bool> = Vec::new();
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut rng = || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..1000 {
            if model.is_empty() || rng() % 2 == 0 {
                let bit = rng() % 2 == 0;
                bits.push(bit);
                model.push(bit);
            } else {
                let i = (rng() as usize) % model.len();
                bits.set(i);
                model[i] = true;
            }
        }
        assert_eq!(bits.len(), model.len());
        for (i, &m) in model.iter().enumerate() {
            assert_eq!(bits.get(i), m, "bit {i}");
        }
    }
}
