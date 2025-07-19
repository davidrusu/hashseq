use std::hash::{Hash, Hasher};

const PROBES: usize = 4;
const BLOOM_SIZE: usize = 1024;
const MAX_RUN: usize = 100;

type Bloom = bitmaps::Bitmap<BLOOM_SIZE>;
type H = std::hash::DefaultHasher;

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BloomList<T> {
    filter: Bloom,
    list: Vec<T>,
}

impl<T: Hash + Eq> BloomList<T> {
    fn len(&self) -> usize {
        self.list.len()
    }

    fn probably_in(&self, v: &T) -> bool {
        let b = bloom(v);
        bloom_test(self.filter, b)
    }

    fn position(&self, v: &T) -> Option<usize> {
        self.list.iter().position(|x| x == v)
    }

    fn insert(&mut self, idx: usize, v: T) {
        assert!(idx <= self.len());
        let b = bloom(&v);

        self.filter |= b;
        self.list.insert(idx, v);
    }

    fn remove(&mut self, idx: usize) -> T {
        let v = self.list.remove(idx);
        let b = bloom(&v);

        let mut b_common = b;
        for other in &self.list {
            b_common &= bloom(other);
        }

        b_common.invert();
        let mut mask = b & b_common;
        mask.invert();
        self.filter &= mask;
        v
    }
}

struct BloomNode {
    size: usize,
    filter: Bloom,
}

#[derive(Default)]
struct PBT<T> {
    levels: Vec<Vec<BloomNode>>,
    leaves: Vec<BloomList<T>>,
}

impl<T: Hash + Eq> PBT<T> {
    pub fn position(&self, value: &T) -> Option<usize> {
        let bloom = bloom(value);

        let mut boundary: Vec<(usize, usize)> = self
            .levels
            .get(0)
            .map(|r| {
                Vec::from_iter(r.iter().enumerate().scan(0, |state, (i, (s, r))| {
                    *state += s;
                    Some((*state, i))
                }))
            })
            .unwrap_or_default();

        for level in &self.levels {
            for (s, b) in std::mem::take(&mut boundary) {
                let (size, filter) = &level[b];
                if bloom_test(*filter, bloom) {
                    boundary.extend([(s, b * 2), (s, b * 2 + 1)]);
                }
            }
            if boundary.is_empty() {
                break;
            }
        }

        for (size, boundary) in boundary {
            let leaf = &self.leaves[boundary];
            if leaf.probably_in(&value) {
                if let Some(p) = leaf.position(&value) {
                    // There's a bug here.
                    // This assumes that all leafs are exactly MAX_RUN length.
                    // this is not the case when the leafs are dynamically growing
                    // up to MAX_RUN and then splitting into two leaves.
                    return Some(boundary * MAX_RUN + p);
                }
            }
        }

        None
    }

    pub fn insert(&mut self, idx: usize, value: T) {
        let bloom = bloom(value);

        let mut boundary = Vec::from_iter(0..self.levels.get(0).map(Vec::len).unwrap_or_default());
        for level in &self.levels {
            for b in std::mem::take(&mut boundary) {
                if bloom_test(level[b], bloom) {
                    boundary.extend([b * 2, b * 2 + 1]);
                }
            }
            if boundary.is_empty() {
                break;
            }
        }

        for boundary in boundary {
            let leaf = &self.leaves[boundary];
            if leaf.probably_in(&value) {
                if let Some(p) = leaf.position(&value) {
                    return Some(boundary * MAX_RUN + p);
                }
            }
        }
    }
}

fn bloom(v: impl Hash) -> Bloom {
    let mut field = Bloom::default();
    let mut h = H::default();
    v.hash(&mut h);
    for i in 0..PROBES {
        i.hash(&mut h);
        let p = h.finish() as usize % BLOOM_SIZE;
        field.set(p, true);
    }

    field
}

fn bloom_test(filter: Bloom, candidate: Bloom) -> bool {
    (filter & candidate) == candidate
}

#[cfg(test)]
mod tests {
    use super::*;

    use quickcheck_macros::quickcheck;

    #[test]
    fn test_collision_frequency() {
        const N: usize = 10;
        let elems = MAX_RUN;
        let mut collisions = [0u16; N];
        for n in 0..N {
            let mut filter = Bloom::default();
            for i in (n * elems)..((n + 1) * elems) {
                let i_b = bloom(i);
                if filter & i_b == i_b {
                    collisions[n] += 1;
                }
                filter |= i_b;
            }
        }

        let normed_collisions = collisions
            .iter()
            .map(|c| *c as f64 / elems as f64)
            .collect::<Vec<f64>>();
        let collision_mean = normed_collisions.iter().sum::<f64>() / N as f64;
        let collision_var = normed_collisions
            .iter()
            .map(|c| (*c - collision_mean).powi(2))
            .sum::<f64>()
            / N as f64;

        println!("collision mean: {collision_mean:.4}, var: {collision_var:.4}");
        assert!(collision_var < 1e-4);
        assert!(collision_mean < 1e-2);
    }

    #[quickcheck]
    fn test_bloom_list_model(ops: Vec<(bool, usize, u32)>) {
        let mut model = Vec::<u32>::default();
        let mut bloom_list = BloomList::<u32>::default();

        for op in ops {
            match op {
                (true, idx, value) => {
                    // insert
                    let idx = if model.is_empty() {
                        0
                    } else {
                        idx % model.len()
                    };
                    model.insert(idx, value);
                    bloom_list.insert(idx, value);
                }
                (false, idx, _) => {
                    if model.is_empty() {
                        continue;
                    }
                    // insert
                    let idx = idx % model.len();
                    model.remove(idx);
                    bloom_list.remove(idx);
                }
            }
        }
    }
}
