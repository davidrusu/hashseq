//! Single-file mode: `note.md` + `note.md.nool`, no repo. Active whenever the
//! current directory is not inside a nool repo.

use hashseq::encoding::{apply_delta, decode_hashseq, encode_hashseq};
use hashseq::value::KIND_SEQ;
use hashseq::{HashNode, HashSeq, HashWeb, Id};

use crate::diff::{apply_edits, diff_edits, edit_totals, lines, render_line_diff};
use crate::{USAGE, delta, random_id, short_id, write_atomic};

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

fn with_one_file(cmd: &str, args: &[String], f: fn(&str) -> Result<(), String>) -> Result<(), String> {
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
            return Err(format!("{sidecar} not found — is this file tracked? (nool track)"));
        }
        Err(e) => return Err(format!("reading {sidecar}: {e}")),
    };
    decode_hashseq(&bytes).map_err(|e| format!("decoding {sidecar}: {e:?}"))
}

fn store_seq(sidecar: &str, seq: &HashSeq) -> Result<(), String> {
    write_atomic(std::path::Path::new(sidecar), &encode_hashseq(seq))
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
    let files = if files.is_empty() { tracked_in_cwd()? } else { files.to_vec() };
    for file in &files {
        commit(file).map_err(|e| format!("commit {file}: {e}"))?;
    }
    Ok(())
}

fn commit(file: &str) -> Result<(), String> {
    let sidecar = sidecar_path(file);
    let mut seq = load_seq(&sidecar)?;
    let old: Vec<char> = realize(&seq).chars().collect();
    let new: Vec<char> = read_working(file)?.chars().collect();
    let edits = diff_edits(&old, &new);
    let (ins, del) = edit_totals(&edits);
    if ins == 0 && del == 0 {
        println!("{file}: no changes");
        return Ok(());
    }
    apply_edits(&mut seq, &edits, &new);
    debug_assert_eq!(realize(&seq).chars().collect::<Vec<_>>(), new);
    store_seq(&sidecar, &seq)?;
    println!("{file}: committed +{ins} −{del} chars");
    Ok(())
}

fn status(files: &[String]) -> Result<(), String> {
    let files = if files.is_empty() { tracked_in_cwd()? } else { files.to_vec() };
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
        return Ok("missing (nool revert to restore)".into());
    }
    let old: Vec<char> = realize(&seq).chars().collect();
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
        _ => return Err(format!("usage: nool diff <file> | nool diff <a> <b>\n\n{USAGE}")),
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

    let working: Vec<char> = read_working(file)?.chars().collect();
    if realize(&ours).chars().collect::<Vec<_>>() != working {
        return Err(format!("{file} has uncommitted edits — run `nool commit {file}` first"));
    }

    let theirs_bytes =
        std::fs::read(theirs_path).map_err(|e| format!("reading {theirs_path}: {e}"))?;
    let theirs =
        decode_hashseq(&theirs_bytes).map_err(|e| format!("decoding {theirs_path}: {e:?}"))?;

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
    std::fs::write(file, realize(&ours)).map_err(|e| format!("writing {file}: {e}"))?;
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
        return Err(format!("usage: nool delta <receiver.nool> <source.nool> <out>\n\n{USAGE}"));
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
    let missing: Vec<HashNode> = have
        .all_nodes()
        .into_iter()
        .filter(|(id, _)| !base.contains_node(id))
        .map(|(_, node)| node)
        .collect();
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
        println!("{out}: {ops} op(s), {} bytes — apply with `nool apply <file> {out}`", bytes.len());
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
        return Err(format!("{file} has uncommitted edits — run `nool commit {file}` first"));
    }
    let bytes = std::fs::read(delta_path).map_err(|e| format!("reading {delta_path}: {e}"))?;
    let (artifacts, msg) = delta::parse_file(&bytes)?;
    for (kind, origin, _) in delta::group_heads(msg)? {
        if kind != KIND_SEQ || origin != seq.origin() {
            return Err(format!(
                "{delta_path} addresses a different document (doc {}, ours is {})",
                short_id(&origin),
                short_id(&seq.origin())
            ));
        }
    }
    // Rebuild the seq inside a temp web so the delta rides the standard
    // apply path (idempotent, parks out-of-order nodes), then clone it out.
    // Previously parked orphans ride along too (all_nodes excludes them), so
    // a delta that arrives ahead of its dependencies survives to be unparked.
    let mut web = HashWeb::new();
    let obj = web.create_seq(seq.origin());
    let applied_before = seq.all_nodes();
    let applied_before_len = applied_before.len();
    for (id, node) in applied_before {
        web.apply_to_with_id(obj, id, node);
    }
    for node in seq.orphans() {
        web.apply_to(obj, node.clone());
    }
    for artifact in artifacts {
        web.provide_artifact_bytes(artifact);
    }
    let delivered = apply_delta(&mut web, msg).map_err(|e| format!("applying {delta_path}: {e:?}"))?;
    if delivered == 0 {
        println!("{file}: nothing new — already converged");
        return Ok(());
    }
    let merged = web.seq(&obj).expect("created above").clone();
    store_seq(&sidecar, &merged)?;
    std::fs::write(file, realize(&merged)).map_err(|e| format!("writing {file}: {e}"))?;
    let applied = merged.all_nodes().len() - applied_before_len;
    let parked = merged.orphans().count();
    let mut note = String::new();
    if applied > delivered {
        note += &format!(" (incl. {} previously parked)", applied - delivered);
    }
    if parked > 0 {
        note += &format!(", {parked} parked awaiting earlier ops");
    }
    println!(
        "{file}: {delivered} new op(s): {applied} applied{note} ({} chars, {} tips)",
        merged.len(),
        merged.tips().len()
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
    std::fs::write(file, realize(&seq)).map_err(|e| format!("writing {file}: {e}"))?;
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
