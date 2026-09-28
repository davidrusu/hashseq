//! Single-file mode: `note.md` + `note.md.nool`, no repo. Active whenever the
//! current directory is not inside a nool repo.

use hashseq::encoding::{decode_hashseq, encode_hashseq};
use hashseq::value::KIND_SEQ;
use hashseq::{HashSeq, Id, Outcome};

use crate::diff::{apply_edits, diff_edits, edit_totals, lines, render_line_diff};
use crate::{SIDECAR_MAGIC, USAGE, delta, random_id, short_id, strip_magic, write_atomic};

pub fn dispatch(cmd: &str, args: &[String]) -> Result<(), String> {
    match cmd {
        "track" => with_one_file(cmd, args, track),
        "status" => status(args),
        "commit" => commit_many(args),
        "diff" => diff_cmd(args),
        "merge" => merge_cmd(args),
        "delta" => delta_cmd(args),
        "apply" => apply_cmd(args),
        "cat" => with_one_file(cmd, args, cat),
        "revert" => with_one_file(cmd, args, revert),
        "info" => with_one_file(cmd, args, info),
        "init" => unreachable!("init is handled in main"),
        other => Err(format!("unknown command `{other}`\n\n{USAGE}")),
    }
}

fn with_one_file(
    cmd: &str,
    args: &[String],
    f: fn(&str) -> Result<(), String>,
) -> Result<(), String> {
    match args {
        [file] => f(file).map_err(|e| format!("{cmd} {file}: {e}")),
        _ => Err(format!("`{cmd}` takes exactly one file\n\n{USAGE}")),
    }
}

fn sidecar_path(file: &str) -> String {
    format!("{file}.nool")
}

fn read_working(file: &str) -> Result<String, String> {
    std::fs::read_to_string(file).map_err(|e| format!("reading {file}: {e}"))
}

fn load_seq(sidecar: &str) -> Result<HashSeq, String> {
    let bytes = match std::fs::read(sidecar) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err(format!(
                "{sidecar} not found — is this file tracked? (nool track)"
            ));
        }
        Err(e) => return Err(format!("reading {sidecar}: {e}")),
    };
    decode_hashseq(strip_magic(&bytes, SIDECAR_MAGIC, sidecar)?)
        .map_err(|e| format!("decoding {sidecar}: {e:?}"))
}

fn store_seq(sidecar: &str, seq: &HashSeq) -> Result<(), String> {
    let mut bytes = SIDECAR_MAGIC.to_vec();
    bytes.extend_from_slice(&encode_hashseq(seq));
    write_atomic(std::path::Path::new(sidecar), &bytes)
}

/// Rewrite `file` from a history just saved to its sidecar. On failure the
/// sidecar is ahead of the file, and committing the stale file would record
/// the difference as edits — `nool revert` finishes the write instead.
fn write_realized(file: &str, seq: &HashSeq) -> Result<(), String> {
    write_atomic(std::path::Path::new(file), realize(seq).as_bytes()).map_err(|e| {
        format!("{e}\nnool: {file}.nool is updated but {file} is not — fix the problem and run `nool revert {file}`")
    })
}

fn realize(seq: &HashSeq) -> String {
    seq.iter().collect()
}

// ---- commands ----

fn track(file: &str) -> Result<(), String> {
    let sidecar = sidecar_path(file);
    if std::fs::exists(&sidecar).unwrap_or(false) {
        return Err(format!("{sidecar} already exists"));
    }
    let content = read_working(file)?;
    let doc_id = random_id()?;
    let mut seq = HashSeq::new(doc_id);
    seq.insert_batch(0, content.chars());
    store_seq(&sidecar, &seq)?;
    println!(
        "tracking {file} — doc {} ({} chars)",
        short_id(&doc_id),
        seq.len()
    );
    Ok(())
}

fn commit_many(files: &[String]) -> Result<(), String> {
    let (files, bare) = if files.is_empty() {
        (tracked_in_cwd()?, true)
    } else {
        (files.to_vec(), false)
    };
    let mut failed = Vec::new();
    for file in &files {
        // A bare commit skips sidecars without a working file (a deleted
        // file, or a peer's history dropped here to merge).
        if bare && !std::fs::exists(file).unwrap_or(false) {
            continue;
        }
        if let Err(e) = commit(file) {
            eprintln!("nool: commit {file}: {e}");
            failed.push(file.as_str());
        }
    }
    match failed.as_slice() {
        [] => Ok(()),
        _ => Err(format!("not committed: {}", failed.join(", "))),
    }
}

fn commit(file: &str) -> Result<(), String> {
    let sidecar = sidecar_path(file);
    let mut seq = load_seq(&sidecar)?;
    let old: Vec<char> = seq.iter().collect();
    let new: Vec<char> = read_working(file)?.chars().collect();
    let edits = diff_edits(&old, &new);
    let (ins, del) = edit_totals(&edits);
    if ins == 0 && del == 0 {
        println!("{file}: no changes");
        return Ok(());
    }
    apply_edits(&mut seq, &edits, &new);
    debug_assert!(seq.iter().eq(new.iter().copied()));
    store_seq(&sidecar, &seq)?;
    println!("{file}: committed +{ins} −{del} chars");
    Ok(())
}

fn status(files: &[String]) -> Result<(), String> {
    let files = if files.is_empty() {
        tracked_in_cwd()?
    } else {
        files.to_vec()
    };
    if files.is_empty() {
        println!("no tracked files here (nool track <file>)");
        return Ok(());
    }
    for file in &files {
        let line = match status_of(file) {
            Ok(s) => s,
            Err(e) => format!("error: {e}"),
        };
        println!("{file}: {line}");
    }
    Ok(())
}

fn status_of(file: &str) -> Result<String, String> {
    let seq = load_seq(&sidecar_path(file))?;
    if !std::fs::exists(file).unwrap_or(false) {
        return Ok("no working file (deleted, or a stray sidecar; nool revert restores it)".into());
    }
    let old: Vec<char> = seq.iter().collect();
    let new: Vec<char> = read_working(file)?.chars().collect();
    let (ins, del) = edit_totals(&diff_edits(&old, &new));
    if ins == 0 && del == 0 {
        Ok("clean".into())
    } else {
        Ok(format!("modified (+{ins} −{del} chars uncommitted)"))
    }
}

/// Every `<file>.nool` in the current directory names a tracked `<file>`.
fn tracked_in_cwd() -> Result<Vec<String>, String> {
    let entries = std::fs::read_dir(".").map_err(|e| format!("reading current dir: {e}"))?;
    let mut files: Vec<String> = entries
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|name| name.strip_suffix(".nool").map(str::to_owned))
        .filter(|file| !file.is_empty()) // a stray `.nool` dir names no file
        .collect();
    files.sort();
    Ok(files)
}

pub fn diff_cmd(args: &[String]) -> Result<(), String> {
    let (left, right, left_origin, right_origin) = match args {
        // Uncommitted edits: last-committed realization vs the working file.
        [file] => {
            let seq = load_seq(&sidecar_path(file))?;
            (realize(&seq), read_working(file)?, None, None)
        }
        [a, b] => {
            let (left, lo) = diff_side(a)?;
            let (right, ro) = diff_side(b)?;
            (left, right, lo, ro)
        }
        _ => {
            return Err(format!(
                "usage: nool diff <file> | nool diff <a> <b>\n\n{USAGE}"
            ));
        }
    };
    if let (Some(lo), Some(ro)) = (left_origin, right_origin)
        && lo != ro
    {
        eprintln!(
            "nool: note: comparing different documents ({} vs {})",
            short_id(&lo),
            short_id(&ro)
        );
    }
    print!("{}", render_line_diff(&lines(&left), &lines(&right)));
    Ok(())
}

/// A diff operand: a `.nool` history (realized) or a plain file (read as-is).
fn diff_side(path: &str) -> Result<(String, Option<Id>), String> {
    if path.ends_with(".nool") {
        let seq = load_seq(path)?;
        Ok((realize(&seq), Some(seq.origin())))
    } else {
        Ok((read_working(path)?, None))
    }
}

fn merge_cmd(args: &[String]) -> Result<(), String> {
    let [file, theirs_path] = args else {
        return Err(format!("usage: nool merge <file> <theirs.nool>\n\n{USAGE}"));
    };
    let sidecar = sidecar_path(file);
    let mut ours = load_seq(&sidecar)?;

    if realize(&ours) != read_working(file)? {
        return Err(format!(
            "{file} has uncommitted edits — run `nool commit {file}` first"
        ));
    }

    let theirs = load_seq(theirs_path)?;

    if ours.origin() != theirs.origin() {
        return Err(format!(
            "different documents: {file} is doc {}, {theirs_path} is doc {}",
            short_id(&ours.origin()),
            short_id(&theirs.origin())
        ));
    }

    let before = ours.len();
    ours.merge(theirs);
    store_seq(&sidecar, &ours)?;
    write_realized(file, &ours)?;
    println!(
        "{file}: merged {theirs_path} ({} → {} chars, {} tips)",
        before,
        ours.len(),
        ours.tips().len()
    );
    Ok(())
}

/// `nool delta <receiver.nool> <source.nool> <out>`: the ops the receiver is
/// missing, written to a delta file it can apply to reach the union.
fn delta_cmd(args: &[String]) -> Result<(), String> {
    let [recv, src, out] = args else {
        return Err(format!(
            "usage: nool delta <receiver.nool> <source.nool> <out>\n\n{USAGE}"
        ));
    };
    let base = load_seq(recv)?;
    let have = load_seq(src)?;
    if base.origin() != have.origin() {
        return Err(format!(
            "different documents: {recv} is doc {}, {src} is doc {}",
            short_id(&base.origin()),
            short_id(&have.origin())
        ));
    }
    let missing = have.delta_for(&base.clock());
    let ops = missing.len();
    let groups = if missing.is_empty() {
        Vec::new()
    } else {
        vec![(KIND_SEQ, have.origin(), missing)]
    };
    let bytes = delta::encode_file(&groups, &[]);
    std::fs::write(out, &bytes).map_err(|e| format!("writing {out}: {e}"))?;
    if ops == 0 {
        println!("{out}: empty delta — {recv} already has everything");
    } else {
        println!(
            "{out}: {ops} op(s), {} bytes — apply with `nool apply <file> {out}`",
            bytes.len()
        );
    }
    Ok(())
}

/// `nool apply <file> <delta>`: deliver a delta's ops into `<file>.nool`,
/// then re-realize the working file.
fn apply_cmd(args: &[String]) -> Result<(), String> {
    let [file, delta_path] = args else {
        return Err(format!("usage: nool apply <file> <delta>\n\n{USAGE}"));
    };
    let sidecar = sidecar_path(file);
    let seq = load_seq(&sidecar)?;
    if realize(&seq) != read_working(file)? {
        return Err(format!(
            "{file} has uncommitted edits — run `nool commit {file}` first"
        ));
    }
    let bytes = std::fs::read(delta_path).map_err(|e| format!("reading {delta_path}: {e}"))?;
    // Text ops carry chars, never artifacts: only the op groups matter.
    let groups = delta::parse_file(&bytes)
        .and_then(|(_, msg)| delta::decode_groups(msg))
        .map_err(|e| format!("{delta_path}: {e}"))?;
    for (kind, origin, _) in &groups {
        if *kind != KIND_SEQ || *origin != seq.origin() {
            return Err(format!(
                "{delta_path} addresses a different document (doc {}, ours is {})",
                short_id(origin),
                short_id(&seq.origin())
            ));
        }
    }
    let mut seq = seq;
    let applied_before = seq.all_nodes().len();
    let delivered = groups
        .into_iter()
        .flat_map(|(_, _, nodes)| nodes)
        .filter(|node| seq.apply(node.clone()).is_ok_and(Outcome::is_news))
        .count();
    if delivered == 0 {
        println!("{file}: nothing new — already converged");
        return Ok(());
    }
    store_seq(&sidecar, &seq)?;
    write_realized(file, &seq)?;
    let applied = seq.all_nodes().len() - applied_before;
    let orphaned = seq.orphans().count();
    let mut note = String::new();
    if applied > delivered {
        note += &format!(" (incl. {} previously orphaned)", applied - delivered);
    }
    if orphaned > 0 {
        note += &format!(", {orphaned} orphaned awaiting earlier ops");
    }
    println!(
        "{file}: {delivered} new op(s): {applied} applied{note} ({} chars, {} tips)",
        seq.len(),
        seq.tips().len()
    );
    Ok(())
}

fn cat(file: &str) -> Result<(), String> {
    let seq = load_seq(&sidecar_path(file))?;
    print!("{}", realize(&seq));
    Ok(())
}

fn revert(file: &str) -> Result<(), String> {
    let seq = load_seq(&sidecar_path(file))?;
    write_atomic(std::path::Path::new(file), realize(&seq).as_bytes())?;
    println!("{file}: restored to last commit ({} chars)", seq.len());
    Ok(())
}

fn info(file: &str) -> Result<(), String> {
    let sidecar = sidecar_path(file);
    let seq = load_seq(&sidecar)?;
    let sidecar_bytes = std::fs::metadata(&sidecar).map(|m| m.len()).unwrap_or(0);
    println!("doc:     {}", hex::encode(seq.origin().0));
    println!("content: {} chars", seq.len());
    println!("sidecar: {sidecar_bytes} bytes");
    let tips: Vec<String> = seq.tips().iter().map(short_id).collect();
    println!("tips:    {}", tips.join(" "));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::{apply_edits, diff_edits};
    use hashseq::encoding::{decode_hashseq, encode_hashseq};

    #[test]
    fn divergent_commits_merge_without_conflict() {
        let doc = Id([7u8; 32]);
        let mut a = HashSeq::new(doc);
        a.insert_batch(0, "shared draft\n".chars());
        let mut b = decode_hashseq(&encode_hashseq(&a)).unwrap();

        let new_a: Vec<char> = "shared draft, edited by a\n".chars().collect();
        let old_a: Vec<char> = a.iter().collect();
        apply_edits(&mut a, &diff_edits(&old_a, &new_a), &new_a);

        let new_b: Vec<char> = "b says: shared draft\n".chars().collect();
        let old_b: Vec<char> = b.iter().collect();
        apply_edits(&mut b, &diff_edits(&old_b, &new_b), &new_b);

        let mut ab = decode_hashseq(&encode_hashseq(&a)).unwrap();
        ab.merge(decode_hashseq(&encode_hashseq(&b)).unwrap());
        let mut ba = decode_hashseq(&encode_hashseq(&b)).unwrap();
        ba.merge(decode_hashseq(&encode_hashseq(&a)).unwrap());

        let merged: String = ab.iter().collect();
        assert_eq!(merged, ba.iter().collect::<String>());
        assert!(merged.contains("edited by a"));
        assert!(merged.contains("b says:"));
    }
}
