//! Diff-to-ops inference: a char-level Myers diff replayed as CRDT ops, and a
//! line-level rendering of the same engine for display.

use std::fmt::Write as _;

use hashseq::HashSeq;

/// An edit script step over old (`a`) and new (`b`) token slices.
/// `Keep`/`Del` count tokens of `a`; `Ins` counts tokens of `b`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Edit {
    Keep(usize),
    Del(usize),
    Ins(usize),
}

pub fn edit_totals(edits: &[Edit]) -> (usize, usize) {
    let mut ins = 0;
    let mut del = 0;
    for e in edits {
        match e {
            Edit::Ins(n) => ins += n,
            Edit::Del(n) => del += n,
            Edit::Keep(_) => {}
        }
    }
    (ins, del)
}

/// Replay an edit script as CRDT ops. Removes don't advance the visible
/// position (survivors shift left); inserts do.
pub fn apply_edits(seq: &mut HashSeq, edits: &[Edit], new: &[char]) {
    let mut pos = 0;
    let mut bi = 0;
    for e in edits {
        match *e {
            Edit::Keep(n) => {
                pos += n;
                bi += n;
            }
            Edit::Del(n) => {
                seq.remove_batch(pos, n);
            }
            Edit::Ins(n) => {
                seq.insert_batch(pos, new[bi..bi + n].iter().copied());
                pos += n;
                bi += n;
            }
        }
    }
}

/// Content overlap for move detection: the number of chars an edit script
/// between the two texts keeps, and that count as a fraction of both texts
/// (1.0 = identical).
pub fn shared_chars(a: &[char], b: &[char]) -> (usize, f64) {
    if a.is_empty() && b.is_empty() {
        return (0, 1.0);
    }
    let (ins, del) = edit_totals(&diff_edits(a, b));
    let kept = a.len() - del;
    debug_assert_eq!(kept, b.len() - ins);
    (kept, (2 * kept) as f64 / (a.len() + b.len()) as f64)
}

/// Myers gets quadratic in the edit distance; past this we fall back to
/// replacing the whole (prefix/suffix-trimmed) middle in one del+ins.
const MYERS_MAX_D: usize = 1000;

pub fn diff_edits<T: PartialEq>(a: &[T], b: &[T]) -> Vec<Edit> {
    let mut pre = 0;
    while pre < a.len() && pre < b.len() && a[pre] == b[pre] {
        pre += 1;
    }
    let mut suf = 0;
    while suf < a.len() - pre && suf < b.len() - pre && a[a.len() - 1 - suf] == b[b.len() - 1 - suf]
    {
        suf += 1;
    }
    let mid_a = &a[pre..a.len() - suf];
    let mid_b = &b[pre..b.len() - suf];

    let mut edits = Vec::new();
    if pre > 0 {
        edits.push(Edit::Keep(pre));
    }
    let middle = myers(mid_a, mid_b).unwrap_or_else(|| {
        let mut coarse = Vec::new();
        if !mid_a.is_empty() {
            coarse.push(Edit::Del(mid_a.len()));
        }
        if !mid_b.is_empty() {
            coarse.push(Edit::Ins(mid_b.len()));
        }
        coarse
    });
    edits.extend(middle);
    if suf > 0 {
        edits.push(Edit::Keep(suf));
    }
    coalesce(edits)
}

fn coalesce(edits: Vec<Edit>) -> Vec<Edit> {
    let mut out: Vec<Edit> = Vec::with_capacity(edits.len());
    for e in edits {
        match (out.last_mut(), e) {
            (_, Edit::Keep(0) | Edit::Del(0) | Edit::Ins(0)) => {}
            (Some(Edit::Keep(m)), Edit::Keep(n)) => *m += n,
            (Some(Edit::Del(m)), Edit::Del(n)) => *m += n,
            (Some(Edit::Ins(m)), Edit::Ins(n)) => *m += n,
            _ => out.push(e),
        }
    }
    out
}

/// Greedy O(ND) Myers with a backtrack trace. `None` if the edit distance
/// exceeds `MYERS_MAX_D` (caller falls back to a coarse replace).
fn myers<T: PartialEq>(a: &[T], b: &[T]) -> Option<Vec<Edit>> {
    let n = a.len() as isize;
    let m = b.len() as isize;
    if n == 0 || m == 0 {
        // Handled by the caller's coarse path, which emits the same script.
        return None;
    }
    let cap = MYERS_MAX_D.min((n + m) as usize);
    let idx = |k: isize| (k + cap as isize) as usize;

    let mut v = vec![0isize; 2 * cap + 1];
    let mut trace: Vec<Vec<isize>> = Vec::new();
    let mut found_d = None;
    'search: for d in 0..=(cap as isize) {
        trace.push(v.clone());
        let mut k = -d;
        while k <= d {
            let mut x = if k == -d || (k != d && v[idx(k - 1)] < v[idx(k + 1)]) {
                v[idx(k + 1)]
            } else {
                v[idx(k - 1)] + 1
            };
            let mut y = x - k;
            while x < n && y < m && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            v[idx(k)] = x;
            if x >= n && y >= m {
                found_d = Some(d);
                break 'search;
            }
            k += 2;
        }
    }
    let found_d = found_d?;

    // Backtrack from (n, m); trace[d] is the V state entering depth d.
    let mut steps: Vec<Edit> = Vec::new();
    let (mut x, mut y) = (n, m);
    for d in (0..=found_d).rev() {
        let v = &trace[d as usize];
        let k = x - y;
        let prev_k = if k == -d || (k != d && v[idx(k - 1)] < v[idx(k + 1)]) {
            k + 1
        } else {
            k - 1
        };
        let prev_x = v[idx(prev_k)];
        let prev_y = prev_x - prev_k;
        while x > prev_x && y > prev_y {
            steps.push(Edit::Keep(1));
            x -= 1;
            y -= 1;
        }
        if d > 0 {
            if x == prev_x + 1 && y == prev_y {
                steps.push(Edit::Del(1));
            } else {
                steps.push(Edit::Ins(1));
            }
        }
        x = prev_x;
        y = prev_y;
    }
    steps.reverse();
    Some(steps)
}

/// Lines for `render_line_diff`: each keeps its terminator (so a trailing
/// newline or a CR-only change is a real difference), split like git.
pub fn lines(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

/// One diff line; a final line without a terminator gets git's marker.
fn write_line(out: &mut String, sign: char, line: &str) {
    let _ = write!(out, "{sign} {line}");
    if !line.ends_with('\n') {
        out.push_str("\n\\ No newline at end of file\n");
    }
}

pub fn render_line_diff(old: &[&str], new: &[&str]) -> String {
    let edits = diff_edits(old, new);
    let mut out = String::new();
    let (mut ai, mut bi) = (0usize, 0usize);
    let mut i = 0;
    while i < edits.len() {
        match edits[i] {
            Edit::Keep(n) => {
                ai += n;
                bi += n;
                i += 1;
            }
            _ => {
                // Group a run of consecutive Del/Ins into one hunk.
                let (hunk_a, hunk_b) = (ai, bi);
                let mut dels: Vec<&str> = Vec::new();
                let mut inss: Vec<&str> = Vec::new();
                while i < edits.len() {
                    match edits[i] {
                        Edit::Del(n) => {
                            dels.extend(&old[ai..ai + n]);
                            ai += n;
                        }
                        Edit::Ins(n) => {
                            inss.extend(&new[bi..bi + n]);
                            bi += n;
                        }
                        Edit::Keep(_) => break,
                    }
                    i += 1;
                }
                let _ = writeln!(
                    out,
                    "@@ -{},{} +{},{} @@",
                    hunk_a + 1,
                    dels.len(),
                    hunk_b + 1,
                    inss.len()
                );
                for line in dels {
                    write_line(&mut out, '-', line);
                }
                for line in inss {
                    write_line(&mut out, '+', line);
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply_script(a: &[char], b: &[char], edits: &[Edit]) -> Vec<char> {
        let mut out: Vec<char> = Vec::new();
        let (mut ai, mut bi) = (0, 0);
        for e in edits {
            match *e {
                Edit::Keep(n) => {
                    out.extend(&a[ai..ai + n]);
                    ai += n;
                    bi += n;
                }
                Edit::Del(n) => ai += n,
                Edit::Ins(n) => {
                    out.extend(&b[bi..bi + n]);
                    bi += n;
                }
            }
        }
        assert_eq!(ai, a.len(), "script must consume all of a");
        assert_eq!(bi, b.len(), "script must consume all of b");
        out
    }

    fn check(a: &str, b: &str) {
        let av: Vec<char> = a.chars().collect();
        let bv: Vec<char> = b.chars().collect();
        let edits = diff_edits(&av, &bv);
        assert_eq!(apply_script(&av, &bv, &edits), bv, "{a:?} -> {b:?} via {edits:?}");
    }

    #[test]
    fn diff_roundtrips() {
        check("", "");
        check("", "abc");
        check("abc", "");
        check("abc", "abc");
        check("abc", "axc");
        check("kitten", "sitting");
        check("the quick brown fox", "the slow brown ox");
        check("aaaa", "aa");
        check("ab\ncd\nef", "ab\nxx\nef\ngh");
    }

    #[test]
    fn diff_roundtrips_randomized() {
        use rand::{Rng, SeedableRng, rngs::StdRng};
        let mut rng = StdRng::seed_from_u64(0x6e6f6f6c); // "nool"
        for _ in 0..500 {
            let len_a = rng.gen_range(0..80);
            let a: String = (0..len_a).map(|_| (b'a' + rng.gen_range(0..4)) as char).collect();
            // Mutate a into b so diffs have realistic shared structure.
            let mut b: Vec<char> = a.chars().collect();
            for _ in 0..rng.gen_range(0..10) {
                if b.is_empty() || rng.gen_bool(0.5) {
                    let at = rng.gen_range(0..=b.len());
                    b.insert(at, (b'a' + rng.gen_range(0..4)) as char);
                } else {
                    let at = rng.gen_range(0..b.len());
                    b.remove(at);
                }
            }
            check(&a, &b.iter().collect::<String>());
        }
    }

    #[test]
    fn line_diff_shows_trailing_newline_changes() {
        let out = render_line_diff(&lines("a\nb\n"), &lines("a\nb"));
        assert_eq!(out, "@@ -2,1 +2,1 @@\n- b\n+ b\n\\ No newline at end of file\n");
        let out = render_line_diff(&lines("a\r\n"), &lines("a\n"));
        assert_eq!(out, "@@ -1,1 +1,1 @@\n- a\r\n+ a\n");
        assert_eq!(render_line_diff(&lines("a\n"), &lines("a\n")), "");
    }

    #[test]
    fn commit_infers_ops_that_realize_the_new_text() {
        use hashseq::Id;
        let mut seq = HashSeq::new(Id::default());
        seq.insert_batch(0, "hello cruel world".chars());
        let old: Vec<char> = seq.iter().collect();
        let new: Vec<char> = "hello kind world!".chars().collect();
        let edits = diff_edits(&old, &new);
        apply_edits(&mut seq, &edits, &new);
        assert_eq!(seq.iter().collect::<String>(), "hello kind world!");
    }
}
