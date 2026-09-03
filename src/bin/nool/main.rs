//! nool — a file's history, kept alongside it.
//!
//! Tamil: நூல் — thread; also a book or treatise.
//!
//! Two modes, picked automatically:
//!
//! - **Single-file mode** (default): the working file (`note.md`) stays a
//!   plain file owned by whatever editor touches it; the sidecar
//!   (`note.md.nool`) is an encoded HashSeq holding the full causal history.
//! - **Repo mode**: inside a directory with `.nool/` (created by `nool init`),
//!   a whole tree is tracked in one HashWeb — a root HashKv registry maps
//!   paths to per-file HashSeq objects, and `merge` unions everything at
//!   once: edits, added files, deleted files.
//!
//! In both modes `commit` infers insert/remove ops by diffing the working
//! file against its realization, and `merge` never conflicts at the text
//! level.

mod delta;
mod diff;
mod repo;
mod sidecar;

use std::process::exit;

use hashseq::Id;

pub const USAGE: &str = "\
nool — a file's history, kept alongside it

single-file mode (outside a repo):
  nool track <file>            start tracking (creates <file>.nool)
  nool status [<file>...]      list tracked files and whether they have uncommitted edits
  nool commit [<file>...]      record uncommitted edits into <file>.nool
  nool diff <file>             show uncommitted edits
  nool diff <a> <b>            compare two files (`.nool` args are realized from history)
  nool merge <file> <theirs>   merge another .nool history into <file>
  nool delta <recv> <src> <out>  write the ops <recv>.nool is missing from <src>.nool
  nool apply <file> <delta>    apply a delta file to <file>'s history
  nool cat <file>              print the last-committed content
  nool revert <file>           discard uncommitted edits (restore <file> from <file>.nool)
  nool info <file>             doc id, length, history tips

repo mode (inside a directory with .nool/):
  nool init                    create a repo here (clone one with plain cp -r)
  nool track <file>...         start tracking files in the repo
  nool status                  all tracked files and their state
  nool commit [<file>...]      record edits (all tracked files by default;
                               a bare commit also records detected moves)
  nool diff [<file>...]        show uncommitted edits per file
  nool merge <other-repo>      merge a cloned repo (edits, adds, deletes, moves)
  nool delta <recv> [<src>] <out>  write the ops <recv> is missing (src: this repo)
  nool apply <delta>           apply a delta file to this repo
  nool mv <from> <to>          move/rename a tracked file (history preserved)
  nool rm <file>...            record a file's deletion
  nool cat <file>              print the last-committed content
  nool revert [<file>...]      restore working files from the repo
  nool info                    repo id, store size, per-file summary
";

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some(cmd) = args.first().map(String::as_str) else {
        print!("{USAGE}");
        return;
    };
    if cmd == "--help" || cmd == "-h" {
        print!("{USAGE}");
        return;
    }
    let result = if cmd == "init" {
        repo::init()
    } else if let Some(root) = repo::find_repo() {
        repo::dispatch(cmd, &args[1..], root)
    } else {
        sidecar::dispatch(cmd, &args[1..])
    };
    if let Err(msg) = result {
        eprintln!("nool: {msg}");
        exit(1);
    }
}

pub fn short_id(id: &Id) -> String {
    hex::encode(&id.0[..4])
}

pub fn random_id() -> Result<Id, String> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).map_err(|e| format!("generating id: {e}"))?;
    Ok(Id(bytes))
}

/// Write via a pid-suffixed temp file + fsync + rename so an interrupted
/// save can't truncate history and concurrent invocations don't share a tmp.
pub fn write_atomic(path: &std::path::Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write as _;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = std::path::PathBuf::from(tmp);
    let mut file = std::fs::File::create(&tmp).map_err(|e| format!("writing {}: {e}", tmp.display()))?;
    let written = file
        .write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| format!("writing {}: {e}", tmp.display()));
    drop(file);
    if let Err(e) = written {
        let _ = std::fs::remove_file(&tmp);
        return Err(e);
    }
    std::fs::rename(&tmp, path).map_err(|e| format!("renaming {}: {e}", tmp.display()))
}

/// Split `--flag` out of a command's args.
pub fn take_flag(args: &[String], flag: &str) -> (Vec<String>, bool) {
    let rest: Vec<String> = args.iter().filter(|a| *a != flag).cloned().collect();
    let found = rest.len() != args.len();
    (rest, found)
}
