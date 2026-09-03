//! Repo mode: a `.nool/` directory tracks a whole tree, ala git.
//!
//! The entire repo is one HashWeb in `.nool/store`. A root HashKv (the
//! "registry", object id in `.nool/root`) maps path strings to the object id
//! of each file's HashSeq. Everything merges as one unit: edits within files,
//! files added, files removed. Concurrently tracking the same path on two
//! replicas surfaces as a multi-value register conflict; we keep the history
//! with the lowest value id (deterministic on every replica) and flag it.
//!
//! Cloning a repo is `cp -r`: copy the directory, `.nool` included.

use std::collections::BTreeMap;
use std::path::{Component, Path, PathBuf};

use hashseq::encoding::{apply_delta, decode_hashweb, encode_hashweb};
use hashseq::hashkv::Read;
use hashseq::value::{KIND_KV, object_id};
use hashseq::{HashWeb, Id, Value};

use crate::diff::{apply_edits, diff_edits, edit_totals, lines, render_line_diff, shared_chars};
use crate::{USAGE, delta, random_id, short_id, take_flag, write_atomic};

pub fn dispatch(cmd: &str, args: &[String], root: PathBuf) -> Result<(), String> {
    // The two-operand `.nool`-file diff still belongs to sidecar mode even
    // inside a repo (comparing loose histories someone sent you).
    if cmd == "diff" && args.len() == 2 && args.iter().any(|a| a.ends_with(".nool")) {
        return crate::sidecar::diff_cmd(args);
    }
    let mut repo = Repo::load(root)?;
    match cmd {
        "track" => repo.track(args),
        "status" => repo.status(),
        "commit" => repo.commit(args),
        "diff" => repo.diff(args),
        "merge" => repo.merge_cmd(args),
        "delta" => repo.delta_cmd(args),
        "apply" => repo.apply_cmd(args),
        "mv" => repo.mv(args),
        "rm" => repo.rm(args),
        "cat" => repo.cat(args),
        "revert" => repo.revert(args),
        "info" => repo.info(),
        other => Err(format!("unknown command `{other}`\n\n{USAGE}")),
    }
}

/// Walk up from the current directory looking for a `.nool/` repo dir.
pub fn find_repo() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    loop {
        if dir.join(".nool").join("store").is_file() {
            return Some(dir);
        }
        if !dir.pop() {
            return None;
        }
    }
}

pub fn init() -> Result<(), String> {
    if let Some(existing) = find_repo() {
        return Err(format!("already inside a nool repo at {}", existing.display()));
    }
    let root = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    std::fs::create_dir_all(root.join(".nool")).map_err(|e| format!("creating .nool: {e}"))?;
    let mut web = HashWeb::new();
    let registry = web.create_kv(random_id()?);
    let repo = Repo { root, web, registry };
    // `root` first: a dir only counts as a repo once `store` exists.
    std::fs::write(repo.root.join(".nool/root"), hex::encode(registry.0))
        .map_err(|e| format!("writing .nool/root: {e}"))?;
    repo.save()?;
    println!(
        "initialized empty nool repo in {} (registry {})",
        home_rel(&repo.root.join(".nool")),
        short_id(&registry)
    );
    Ok(())
}

struct Repo {
    root: PathBuf,
    web: HashWeb,
    registry: Id,
}

#[derive(Debug, Clone, Copy)]
struct Tracked {
    obj: Id,
    conflicted: bool,
}

/// A missing tracked file pairs with an untracked file as a move when their
/// contents share at least this fraction of characters — and at least
/// `MOVE_MIN_SHARED` of them, so two one-line files don't pair on a `\n`.
const MOVE_SIMILARITY: f64 = 0.5;
const MOVE_MIN_SHARED: usize = 16;

/// The working tree scanned against the store (see `Repo::stage`).
struct Stage {
    /// Tracked, present, content differs.
    modified: Vec<(String, Tracked)>,
    /// Detected moves: (from, to, tracked entry of `from`).
    moves: Vec<(String, String, Tracked)>,
    /// Tracked, gone from disk, no move pairing.
    missing: Vec<String>,
    /// On disk, not in the registry, no move pairing.
    untracked: Vec<String>,
}

impl Repo {
    fn load(root: PathBuf) -> Result<Self, String> {
        let registry = read_registry_id(&root.join(".nool/root"))?;
        let bytes = std::fs::read(root.join(".nool/store"))
            .map_err(|e| format!("reading .nool/store: {e}"))?;
        let web = decode_hashweb(&bytes).map_err(|e| format!("decoding .nool/store: {e:?}"))?;
        if web.kv(&registry).is_none() {
            return Err(format!("registry {} missing from store", short_id(&registry)));
        }
        Ok(Self { root, web, registry })
    }

    fn save(&self) -> Result<(), String> {
        write_atomic(&self.root.join(".nool/store"), &encode_hashweb(&self.web))
    }

    /// Repo-relative path (with `/` separators) for a user-supplied path.
    /// Compared physically: symlinks in the arg's parent and in the root are
    /// resolved (macOS `/tmp` is `/private/tmp`), so an absolute spelling of
    /// a file inside the repo is accepted however the user reached it. A
    /// parent that doesn't exist yet (`mv` into a new dir) is compared
    /// lexically.
    fn rel(&self, arg: &str) -> Result<String, String> {
        let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
        let full = cwd.join(arg);
        let full = match (full.parent().and_then(|p| p.canonicalize().ok()), full.file_name()) {
            (Some(parent), Some(name)) => parent.join(name),
            _ => lexical_normalize(&full).ok_or_else(|| format!("{arg}: escapes the filesystem root"))?,
        };
        let root = self.root.canonicalize().unwrap_or_else(|_| self.root.clone());
        let parts: Vec<String> = full
            .strip_prefix(&root)
            .map_err(|_| format!("{arg}: outside the repo at {}", home_rel(&self.root)))?
            .components()
            .map(|c| c.as_os_str().to_string_lossy().into_owned())
            .collect();
        if parts.is_empty() {
            return Err(format!("{arg}: is the repo root itself"));
        }
        let rel = parts.join("/");
        if rel == ".nool" || rel.starts_with(".nool/") {
            return Err(format!("{arg}: .nool is nool's own state"));
        }
        Ok(rel)
    }

    fn abs(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }

    /// The live registry: path -> tracked file, conflicts resolved to the
    /// lowest value id (same pick on every replica). Keys that don't name a
    /// safe repo-relative path are left out (see `safe_key`); `status`
    /// reports them.
    fn tracked(&self) -> BTreeMap<String, Tracked> {
        tracked_files(&self.web, &self.registry)
            .into_iter()
            .map(|(path, (obj, conflicted))| (path, Tracked { obj, conflicted }))
            .collect()
    }

    fn realize(&self, obj: &Id) -> Result<String, String> {
        let seq = self
            .web
            .seq(obj)
            .ok_or_else(|| format!("object {} missing from store", short_id(obj)))?;
        Ok(seq.iter().collect())
    }

    fn read_disk(&self, rel: &str) -> Result<Option<String>, String> {
        match std::fs::read_to_string(self.abs(rel)) {
            Ok(s) => Ok(Some(s)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("reading {rel}: {e}")),
        }
    }

    /// Resolve command args to tracked paths; no args means every tracked file.
    fn targets(&self, args: &[String]) -> Result<Vec<(String, Tracked)>, String> {
        let tracked = self.tracked();
        if args.is_empty() {
            return Ok(tracked.into_iter().collect());
        }
        let mut out = Vec::new();
        for arg in args {
            let rel = self.rel(arg)?;
            let t = tracked
                .get(&rel)
                .ok_or_else(|| format!("{rel}: not tracked (nool track)"))?;
            out.push((rel, *t));
        }
        Ok(out)
    }

    /// Register `obj` at `path` per PLACEMENT_SPEC's two-ops-one-gesture: a
    /// registry put claims the slot (the link atom), and a `Place` in the
    /// object's own DAG claims membership, superseding any prior placement.
    /// On a move the old registry entry is NOT deleted — it goes dead by the
    /// membership rule in `tracked_files` and remains a ghost.
    fn register_at(&mut self, rel: &str, obj: Id) {
        let kv = self.web.kv_mut(&self.registry).expect("checked at load");
        let put = kv.put(Value::String(rel.to_owned()), Value::Bytes(obj.0.to_vec()));
        let put_id = put.id();
        if let Some(seq) = self.web.seq_mut(&obj) {
            seq.place(put_id);
        }
    }

    // ---- commands ----

    fn track(&mut self, args: &[String]) -> Result<(), String> {
        if args.is_empty() {
            return Err(format!("usage: nool track <file>...\n\n{USAGE}"));
        }
        let tracked = self.tracked();
        let mut added = 0;
        for arg in args {
            let rel = self.rel(arg)?;
            if tracked.contains_key(&rel) {
                println!("{rel}: already tracked");
                continue;
            }
            let content = std::fs::read_to_string(self.abs(&rel))
                .map_err(|e| format!("reading {rel}: {e}"))?;
            let obj = self.web.create_seq(random_id()?);
            let seq = self.web.seq_mut(&obj).expect("just created");
            seq.insert_batch(0, content.chars());
            let chars = seq.len();
            self.register_at(&rel, obj);
            println!("tracking {rel} — {chars} chars");
            added += 1;
        }
        if added > 0 {
            self.save()?;
        }
        Ok(())
    }

    fn mv(&mut self, args: &[String]) -> Result<(), String> {
        let [from, to] = args else {
            return Err(format!("usage: nool mv <from> <to>\n\n{USAGE}"));
        };
        let from_rel = self.rel(from)?;
        let to_rel = self.rel(to)?;
        let tracked = self.tracked();
        let t = *tracked
            .get(&from_rel)
            .ok_or_else(|| format!("{from_rel}: not tracked"))?;
        if tracked.contains_key(&to_rel) {
            return Err(format!("{to_rel}: already tracked"));
        }
        let from_on_disk = std::fs::exists(self.abs(&from_rel)).unwrap_or(false);
        let to_on_disk = std::fs::exists(self.abs(&to_rel)).unwrap_or(false);
        match (from_on_disk, to_on_disk) {
            (true, true) => {
                return Err(format!("both {from_rel} and {to_rel} exist on disk"));
            }
            (false, false) => {
                return Err(format!("{from_rel}: not on disk ({to_rel} isn't either)"));
            }
            (true, false) => {
                let to_abs = self.abs(&to_rel);
                if let Some(parent) = to_abs.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("creating {}: {e}", parent.display()))?;
                }
                std::fs::rename(self.abs(&from_rel), to_abs)
                    .map_err(|e| format!("renaming {from_rel}: {e}"))?;
            }
            // Already moved on disk — just record it.
            (false, true) => {}
        }
        self.register_at(&to_rel, t.obj);
        self.save()?;
        println!("{from_rel} -> {to_rel}");
        Ok(())
    }

    fn commit(&mut self, args: &[String]) -> Result<(), String> {
        if !args.is_empty() {
            return self.commit_paths(args);
        }
        let stage = self.stage()?;
        let mut committed = 0;
        for (from, to, t) in &stage.moves {
            // A detected move gets the same encoding as `nool mv`.
            self.register_at(to, t.obj);
            println!("{from} -> {to}: move recorded");
            committed += 1;
        }
        let moved_targets = stage.moves.iter().map(|(_, to, t)| (to.clone(), *t));
        for (rel, t) in moved_targets.chain(stage.modified) {
            if self.commit_file(&rel, &t)? {
                committed += 1;
            }
        }
        for rel in &stage.missing {
            println!("{rel}: missing — `nool rm {rel}` to record the deletion");
        }
        if committed > 0 {
            self.save()?;
        } else {
            println!("nothing to commit");
        }
        Ok(())
    }

    fn commit_paths(&mut self, args: &[String]) -> Result<(), String> {
        let mut committed = 0;
        for (rel, t) in self.targets(args)? {
            if self.read_disk(&rel)?.is_none() {
                println!("{rel}: missing — `nool rm {rel}` to record the deletion");
                continue;
            }
            if self.commit_file(&rel, &t)? {
                committed += 1;
            } else {
                println!("{rel}: no changes");
            }
        }
        if committed > 0 {
            self.save()?;
        }
        Ok(())
    }

    /// Diff one working file against its committed realization and apply the
    /// inferred ops. `Ok(true)` iff anything changed. Does not save.
    fn commit_file(&mut self, rel: &str, t: &Tracked) -> Result<bool, String> {
        let Some(disk) = self.read_disk(rel)? else {
            return Ok(false);
        };
        let old: Vec<char> = self.realize(&t.obj)?.chars().collect();
        let new: Vec<char> = disk.chars().collect();
        let edits = diff_edits(&old, &new);
        let (ins, del) = edit_totals(&edits);
        if ins == 0 && del == 0 {
            return Ok(false);
        }
        let seq = self.web.seq_mut(&t.obj).expect("realized above");
        apply_edits(seq, &edits, &new);
        println!("{rel}: committed +{ins} −{del} chars");
        Ok(true)
    }

    /// The stage: the working tree scanned against the store. Missing tracked
    /// paths are paired with untracked files by content similarity — a pair
    /// at or above `MOVE_SIMILARITY` is a detected move.
    fn stage(&self) -> Result<Stage, String> {
        let tracked = self.tracked();
        let mut modified = Vec::new();
        let mut missing: Vec<String> = Vec::new();
        for (rel, t) in &tracked {
            match self.read_disk(rel)? {
                None => missing.push(rel.clone()),
                Some(disk) => {
                    if disk != self.realize(&t.obj)? {
                        modified.push((rel.clone(), *t));
                    }
                }
            }
        }
        let mut untracked = self.scan_untracked(&tracked)?;
        let mut moves = Vec::new();
        let mut still_missing = Vec::new();
        for rel in missing {
            let committed: Vec<char> = self.realize(&tracked[&rel].obj)?.chars().collect();
            let best = untracked
                .iter()
                .enumerate()
                .filter_map(|(i, cand)| {
                    let content: Vec<char> = self.read_disk(cand).ok()??.chars().collect();
                    let (shared, sim) = shared_chars(&committed, &content);
                    (shared >= MOVE_MIN_SHARED).then_some((i, sim))
                })
                .max_by(|a, b| a.1.total_cmp(&b.1));
            match best {
                Some((i, sim)) if sim >= MOVE_SIMILARITY => {
                    let to = untracked.remove(i);
                    moves.push((rel.clone(), to, tracked[&rel]));
                }
                _ => still_missing.push(rel),
            }
        }
        Ok(Stage { modified, moves, missing: still_missing, untracked })
    }

    /// Working-tree scan for files not in the registry. Hidden files and
    /// directories (dot-prefixed, `.nool` included), loose `*.nool`
    /// sidecars, and symlinks (never followed) are skipped.
    fn scan_untracked(&self, tracked: &BTreeMap<String, Tracked>) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let entries =
                std::fs::read_dir(&dir).map_err(|e| format!("reading {}: {e}", dir.display()))?;
            for entry in entries {
                let entry = entry.map_err(|e| format!("reading {}: {e}", dir.display()))?;
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with('.') || name.ends_with(".nool") {
                    continue;
                }
                let kind = entry
                    .file_type()
                    .map_err(|e| format!("reading {}: {e}", entry.path().display()))?;
                let path = entry.path();
                if kind.is_dir() {
                    stack.push(path);
                } else if kind.is_file() {
                    let rel = path
                        .strip_prefix(&self.root)
                        .map_err(|_| "scan escaped the repo root".to_string())?
                        .to_string_lossy()
                        .into_owned();
                    if !tracked.contains_key(&rel) {
                        out.push(rel);
                    }
                }
            }
        }
        out.sort();
        Ok(out)
    }

    fn status(&self) -> Result<(), String> {
        let tracked = self.tracked();
        let stage = self.stage()?;
        println!(
            "repo {} — {} tracked file(s)",
            home_rel(&self.root),
            tracked.len()
        );
        for key in unsafe_keys(&self.web, &self.registry) {
            println!("  ignored unsafe path in registry: {key:?}");
        }
        let moved_from: Vec<&String> = stage.moves.iter().map(|(from, _, _)| from).collect();
        for (rel, t) in &tracked {
            if moved_from.contains(&rel) {
                continue; // reported below as a detected move
            }
            let state = match self.read_disk(rel)? {
                None => "missing — `nool rm` to record the deletion".to_string(),
                Some(disk) => {
                    let old: Vec<char> = self.realize(&t.obj)?.chars().collect();
                    let new: Vec<char> = disk.chars().collect();
                    let (ins, del) = edit_totals(&diff_edits(&old, &new));
                    if ins == 0 && del == 0 {
                        "clean".to_string()
                    } else {
                        format!("modified (+{ins} −{del} chars uncommitted)")
                    }
                }
            };
            let flag = if t.conflicted { " [registry conflict — resolved deterministically]" } else { "" };
            println!("  {rel}: {state}{flag}");
        }
        for (from, to, _) in &stage.moves {
            println!("  {from} -> {to}: move detected (commit to record)");
        }
        for rel in &stage.untracked {
            println!("  {rel}: untracked (nool track to add)");
        }
        Ok(())
    }

    fn diff(&self, args: &[String]) -> Result<(), String> {
        for (rel, t) in self.targets(args)? {
            let Some(disk) = self.read_disk(&rel)? else {
                println!("--- {rel} (missing)");
                continue;
            };
            let committed = self.realize(&t.obj)?;
            if committed == disk {
                continue;
            }
            println!("--- {rel}");
            print!("{}", render_line_diff(&lines(&committed), &lines(&disk)));
        }
        Ok(())
    }

    fn rm(&mut self, args: &[String]) -> Result<(), String> {
        let (args, force) = take_flag(args, "--force");
        if args.is_empty() {
            return Err(format!("usage: nool rm [--force] <file>...\n\n{USAGE}"));
        }
        let targets = self.targets(&args)?;
        if !force {
            // Deleting the working file would drop edits nothing has recorded.
            let mut dirty = Vec::new();
            for (rel, t) in &targets {
                if let Some(disk) = self.read_disk(rel)?
                    && disk != self.realize(&t.obj)?
                {
                    dirty.push(rel.clone());
                }
            }
            if !dirty.is_empty() {
                return Err(format!(
                    "uncommitted changes in: {} — commit, revert, or `nool rm --force`",
                    dirty.join(", ")
                ));
            }
        }
        for (rel, _) in targets {
            let kv = self.web.kv_mut(&self.registry).expect("checked at load");
            kv.del(Value::String(rel.clone()));
            match std::fs::remove_file(self.abs(&rel)) {
                Ok(()) => println!("{rel}: untracked and deleted"),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    println!("{rel}: deletion recorded")
                }
                Err(e) => return Err(format!("removing {rel}: {e}")),
            }
        }
        self.save()
    }

    fn cat(&self, args: &[String]) -> Result<(), String> {
        let [arg] = args else {
            return Err(format!("usage: nool cat <file>\n\n{USAGE}"));
        };
        let targets = self.targets(std::slice::from_ref(arg))?;
        print!("{}", self.realize(&targets[0].1.obj)?);
        Ok(())
    }

    fn revert(&self, args: &[String]) -> Result<(), String> {
        for (rel, t) in self.targets(args)? {
            let committed = self.realize(&t.obj)?;
            if self.read_disk(&rel)?.as_deref() == Some(committed.as_str()) {
                continue;
            }
            self.write_working(&rel, &committed)?;
            println!("{rel}: restored to last commit ({} chars)", committed.chars().count());
        }
        Ok(())
    }

    fn info(&self) -> Result<(), String> {
        let tracked = self.tracked();
        let store_bytes = std::fs::metadata(self.root.join(".nool/store"))
            .map(|m| m.len())
            .unwrap_or(0);
        println!("repo:     {}", home_rel(&self.root));
        println!("registry: {}", hex::encode(self.registry.0));
        println!("store:    {store_bytes} bytes, {} object(s)", self.web.object_count());
        println!("tracked:  {} file(s)", tracked.len());
        for (rel, t) in &tracked {
            let seq = self.web.seq(&t.obj);
            let (chars, tips) = seq.map(|s| (s.len(), s.tips().len())).unwrap_or((0, 0));
            println!("  {rel}: {chars} chars, {tips} tip(s), doc {}", short_id(&t.obj));
        }
        Ok(())
    }

    fn merge_cmd(&mut self, args: &[String]) -> Result<(), String> {
        let (args, force) = take_flag(args, "--force");
        let [other] = args.as_slice() else {
            return Err(format!("usage: nool merge [--force] <other-repo-dir | store-file>\n\n{USAGE}"));
        };
        self.require_clean()?;
        let theirs = self.load_other(Path::new(other))?;
        let before = self.tracked();
        self.web.merge(theirs);
        if !force {
            self.refuse_untracked_clobber(&before)?;
        }
        self.save()?;
        let (changed, deleted) = self.sync_or_hint(&before)?;
        println!(
            "merged {other}: {changed} file(s) changed, {deleted} deleted, {} tracked",
            self.tracked().len()
        );
        Ok(())
    }

    /// Write ops a receiver store is missing to a delta file. Two-operand
    /// form uses this repo as the source; three-operand form is a general
    /// difference between two stores. Applying the file to the receiver
    /// yields the union of both.
    fn delta_cmd(&self, args: &[String]) -> Result<(), String> {
        let (receiver, source, out) = match args {
            [recv, out] => (read_store(Path::new(recv))?, None, out),
            [recv, src, out] => (
                read_store(Path::new(recv))?,
                Some(read_store(Path::new(src))?),
                out,
            ),
            _ => {
                return Err(format!("usage: nool delta <receiver> [<source>] <out>\n\n{USAGE}"));
            }
        };
        let source = source.as_ref().unwrap_or(&self.web);
        if !share_registry(&receiver, source) {
            return Err(
                "unrelated repos: no shared registry — deltas only make sense between clones \
                 (repos share history by cloning, not by init twice)"
                    .into(),
            );
        }
        let (groups, artifacts) = delta::diff(&receiver, source);
        let ops: usize = groups.iter().map(|(_, _, nodes)| nodes.len()).sum();
        let bytes = delta::encode_file(&groups, &artifacts);
        std::fs::write(out, &bytes).map_err(|e| format!("writing {out}: {e}"))?;
        if ops == 0 {
            println!("{out}: empty delta — the receiver already has everything");
        } else {
            println!(
                "{out}: {ops} op(s) across {} object(s), {} artifact(s), {} bytes",
                groups.len(),
                artifacts.len(),
                bytes.len()
            );
        }
        Ok(())
    }

    fn apply_cmd(&mut self, args: &[String]) -> Result<(), String> {
        let (args, force) = take_flag(args, "--force");
        let [path] = args.as_slice() else {
            return Err(format!("usage: nool apply [--force] <delta-file>\n\n{USAGE}"));
        };
        self.require_clean()?;
        let bytes = std::fs::read(path).map_err(|e| format!("reading {path}: {e}"))?;
        let (artifacts, msg) = delta::parse_file(&bytes)?;
        // Lineage gate: a delta from an unrelated repo would plant a foreign
        // registry (or orphan objects nothing references) — reject both.
        let heads = delta::group_heads(msg)?;
        for (kind, origin, _) in &heads {
            let obj = object_id(*kind, origin);
            if *kind == KIND_KV && obj != self.registry {
                return Err(format!(
                    "{path} addresses a foreign registry ({}) — unrelated repo",
                    short_id(&obj)
                ));
            }
        }
        let any_known = heads.iter().any(|(kind, origin, _)| {
            let obj = object_id(*kind, origin);
            self.web.kv(&obj).is_some() || self.web.seq(&obj).is_some()
        });
        if !heads.is_empty() && !any_known {
            return Err(format!(
                "{path}: unrelated delta — none of its objects exist in this repo"
            ));
        }
        let before = self.tracked();
        for artifact in artifacts {
            self.web.provide_artifact_bytes(artifact);
        }
        let delivered =
            apply_delta(&mut self.web, msg).map_err(|e| format!("applying {path}: {e:?}"))?;
        if delivered == 0 {
            println!("{path}: nothing new — already converged");
            return Ok(());
        }
        if !force {
            self.refuse_untracked_clobber(&before)?;
        }
        self.save()?;
        let (changed, deleted) = self.sync_or_hint(&before)?;
        println!("applied {path}: {delivered} new op(s), {changed} file(s) changed, {deleted} deleted");
        Ok(())
    }

    fn require_clean(&self) -> Result<(), String> {
        let mut dirty = Vec::new();
        for (rel, t) in self.targets(&[])? {
            let clean = match self.read_disk(&rel)? {
                None => false,
                Some(disk) => disk == self.realize(&t.obj)?,
            };
            if !clean {
                dirty.push(rel);
            }
        }
        if dirty.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "uncommitted changes in: {} — commit or revert first",
                dirty.join(", ")
            ))
        }
    }

    /// Paths the merged store newly tracks that already exist on disk with
    /// other content — syncing would silently overwrite them. Checked before
    /// the store is saved, so a refusal leaves the repo untouched.
    fn refuse_untracked_clobber(&self, before: &BTreeMap<String, Tracked>) -> Result<(), String> {
        let mut clobbered = Vec::new();
        for (rel, t) in self.tracked() {
            if before.contains_key(&rel) {
                continue;
            }
            if let Some(disk) = self.read_disk(&rel)?
                && disk != self.realize(&t.obj)?
            {
                clobbered.push(rel);
            }
        }
        if clobbered.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "untracked working files would be overwritten: {} — move them away, or `--force` to overwrite",
                clobbered.join(", ")
            ))
        }
    }

    /// `sync_working_tree` after the store is saved: a failure leaves the
    /// store ahead of the tree, which `nool revert` finishes.
    fn sync_or_hint(&self, before: &BTreeMap<String, Tracked>) -> Result<(usize, usize), String> {
        self.sync_working_tree(before).map_err(|e| {
            format!("{e}\nnool: the store is updated but the working tree is not — fix the problem and run `nool revert`")
        })
    }

    /// After the store changed underneath a clean tree (merge, apply):
    /// delete unregistered files, write changed/new realizations, and note
    /// fresh registry conflicts. Returns (files changed, files deleted).
    fn sync_working_tree(&self, before: &BTreeMap<String, Tracked>) -> Result<(usize, usize), String> {
        let after = self.tracked();
        let mut deleted = 0;
        for rel in before.keys() {
            if !after.contains_key(rel) {
                match std::fs::remove_file(self.abs(rel)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(format!("removing {rel}: {e}")),
                }
                println!("{rel}: deleted");
                deleted += 1;
            }
        }
        let mut changed = 0;
        for (rel, t) in &after {
            let content = self.realize(&t.obj)?;
            let disk = self.read_disk(rel)?;
            if disk.as_deref() != Some(content.as_str()) {
                self.write_working(rel, &content)?;
                println!("{rel}: {}", if disk.is_some() { "updated" } else { "created" });
                changed += 1;
            }
            if t.conflicted && !before.get(rel).is_some_and(|b| b.conflicted) {
                println!("{rel}: concurrent registry conflict (track or move on both sides) — resolved the same way on every replica; nothing is lost from the store");
            }
        }
        Ok((changed, deleted))
    }

    /// The merge operand: a repo directory (registry must match) or a bare
    /// store file (assumed same lineage).
    fn load_other(&self, path: &Path) -> Result<HashWeb, String> {
        let (store_path, root_path) = if path.join(".nool/store").is_file() {
            (path.join(".nool/store"), Some(path.join(".nool/root")))
        } else {
            (path.to_path_buf(), None)
        };
        if let Some(root_path) = root_path {
            let their_registry = read_registry_id(&root_path)?;
            if their_registry != self.registry {
                return Err(format!(
                    "unrelated repo: registry {} vs ours {} (repos share history by cloning, not by init twice)",
                    short_id(&their_registry),
                    short_id(&self.registry)
                ));
            }
        }
        let bytes = std::fs::read(&store_path)
            .map_err(|e| format!("reading {}: {e}", store_path.display()))?;
        decode_hashweb(&bytes).map_err(|e| format!("decoding {}: {e:?}", store_path.display()))
    }

    fn write_working(&self, rel: &str, content: &str) -> Result<(), String> {
        let abs = self.abs(rel);
        if let Some(parent) = abs.parent() {
            std::fs::create_dir_all(parent).map_err(|e| format!("creating {}: {e}", parent.display()))?;
        }
        std::fs::write(&abs, content).map_err(|e| format!("writing {rel}: {e}"))
    }
}

/// Paths under $HOME render as `~/...` — repo roots show up in almost
/// every command's output, and the absolute spelling is noise.
fn home_rel(path: &Path) -> String {
    if let Some(home) = std::env::var_os("HOME")
        && let Ok(rest) = path.strip_prefix(&home)
    {
        return format!("~/{}", rest.display());
    }
    path.display().to_string()
}

/// Two stores are the same lineage iff they share a kv object — nool creates
/// exactly one kv per repo (the registry), so clones share it and independent
/// inits never do.
fn share_registry(a: &HashWeb, b: &HashWeb) -> bool {
    a.objects().any(|obj| a.kv(obj).is_some() && b.kv(obj).is_some())
}

/// A store operand: a repo directory (its `.nool/store`) or a bare store file.
fn read_store(path: &Path) -> Result<HashWeb, String> {
    let store = if path.join(".nool/store").is_file() {
        path.join(".nool/store")
    } else {
        path.to_path_buf()
    };
    let bytes =
        std::fs::read(&store).map_err(|e| format!("reading {}: {e}", store.display()))?;
    decode_hashweb(&bytes).map_err(|e| format!("decoding {}: {e:?}", store.display()))
}

fn read_registry_id(path: &Path) -> Result<Id, String> {
    let hex_str = std::fs::read_to_string(path)
        .map_err(|e| format!("reading {}: {e}", path.display()))?;
    let bytes = hex::decode(hex_str.trim()).map_err(|e| format!("{}: {e}", path.display()))?;
    let bytes: [u8; 32] = bytes
        .try_into()
        .map_err(|_| format!("{}: not a 32-byte id", path.display()))?;
    Ok(Id(bytes))
}

/// The lexical form of a path: `.`/`..` folded, no symlink resolution.
/// `None` if `..` climbs past the filesystem root.
fn lexical_normalize(path: &Path) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    return None;
                }
            }
            Component::RootDir | Component::Prefix(_) => out.push(c),
            Component::Normal(s) => out.push(s),
        }
    }
    Some(out)
}

/// Is a registry key safe to join onto the repo root? Registry keys arrive
/// from merged stores and applied deltas, not just `rel()`, so this is the
/// one gate before any key becomes a path on disk: relative, plain
/// components only (no `.`/`..`/empty/backslash), never inside `.nool`.
fn safe_key(key: &str) -> bool {
    if key.contains('\\') || key.contains('\0') {
        return false;
    }
    let mut segments = key.split('/');
    let first = segments.next().unwrap_or("");
    !matches!(first, "" | "." | ".." | ".nool") && segments.all(|s| !matches!(s, "" | "." | ".."))
}

fn as_obj_id(v: Value) -> Option<Id> {
    match v {
        Value::Bytes(b) => Some(Id(b.try_into().ok()?)),
        _ => None,
    }
}

/// Registry keys `tracked_files` skipped as unsafe (live entries only).
fn unsafe_keys(web: &HashWeb, registry: &Id) -> Vec<String> {
    tracked_files_checked(web, registry).1
}

fn tracked_files(web: &HashWeb, registry: &Id) -> BTreeMap<String, (Id, bool)> {
    tracked_files_checked(web, registry).0
}

/// Registry read: path -> (seq object id, conflicted). Value artifacts may
/// live in the registry kv's own store (local puts) or in the web-level store
/// (deposited there by `HashWeb::merge`), so resolution checks both.
///
/// Membership (PLACEMENT_SPEC): a registry put is only a *link atom*. When
/// the pointed-at object has authored `Place` ops, its placement register
/// elects a single home link, and a registry entry is live only if it is that
/// link — a moved-away path stays in the registry as a dead ghost. Under a
/// placement conflict (concurrent moves) the first id-sorted head wins, the
/// same pick on every replica. An object with no `Place` ops falls back to
/// the legacy-presence rule: the registry entry alone decides.
///
/// Returns (tracked, unsafe keys skipped): a live entry whose key fails
/// `safe_key` is never tracked, so it can't become a path outside the repo.
fn tracked_files_checked(web: &HashWeb, registry: &Id) -> (BTreeMap<String, (Id, bool)>, Vec<String>) {
    let mut out = BTreeMap::new();
    let mut skipped = Vec::new();
    let Some(kv) = web.kv(registry) else {
        return (out, skipped);
    };
    let resolve = |vid: &Id| kv.resolve(vid).or_else(|| web.resolve(vid));
    let key_ids: Vec<Id> = kv.keys().copied().collect();
    for key_id in key_ids {
        let Some(Value::String(path)) = resolve(&key_id) else { continue };
        let vids = match kv.read_id(&key_id) {
            Read::Absent => continue,
            Read::One(vid) => vec![vid],
            Read::Conflict(vids) => vids,
        };
        // Registry heads (put node ids) pair 1:1 with read_id's value ids:
        // both come back in head-id order and every head is a put.
        let puts: Vec<Id> = kv.heads(&key_id).to_vec();
        let mut live: Vec<(Id, Id, bool)> = Vec::new(); // (value id, obj, placement conflict)
        for (put_id, vid) in puts.iter().zip(&vids) {
            let Some(obj) = resolve(vid).and_then(as_obj_id) else { continue };
            match web.seq(&obj).map(|s| s.placement()) {
                Some(reg) if !reg.is_empty() => {
                    let home = reg.entry(&reg.heads()[0]).map(|e| e.placed_at);
                    if home == Some(*put_id) {
                        live.push((*vid, obj, reg.conflicted()));
                    }
                }
                _ => live.push((*vid, obj, false)),
            }
        }
        let conflicted = live.len() > 1 || live.iter().any(|(_, _, c)| *c);
        if let Some((_, obj, _)) = live.iter().min_by(|a, b| a.0.cmp(&b.0)) {
            if !safe_key(&path) {
                skipped.push(path);
                continue;
            }
            out.insert(path, (*obj, conflicted));
        }
    }
    (out, skipped)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hashseq::encoding::{decode_hashweb, encode_hashweb};

    pub fn track(web: &mut HashWeb, registry: &Id, path: &str, content: &str, seed: Id) -> Id {
        let obj = web.create_seq(seed);
        web.seq_mut(&obj).unwrap().insert_batch(0, content.chars());
        register_at(web, registry, path, obj);
        obj
    }

    /// Mirror of `Repo::register_at`: registry put + `Place` in the object.
    pub fn register_at(web: &mut HashWeb, registry: &Id, path: &str, obj: Id) {
        let put = web
            .kv_mut(registry)
            .unwrap()
            .put(Value::String(path.into()), Value::Bytes(obj.0.to_vec()));
        web.seq_mut(&obj).unwrap().place(put.id());
    }

    #[test]
    fn unsafe_registry_keys_are_never_tracked() {
        let mut web = HashWeb::new();
        let registry = web.create_kv(Id([1u8; 32]));
        let keys = [
            "../outside.txt",
            "/etc/passwd",
            ".nool/root",
            ".nool",
            "a/../../b",
            "a/./b",
            "",
            "a\\b",
            "docs/../ok.md",
        ];
        for (i, key) in keys.iter().enumerate() {
            track(&mut web, &registry, key, "x\n", Id([10 + i as u8; 32]));
        }
        track(&mut web, &registry, "docs/ok.md", "x\n", Id([99u8; 32]));
        let (tracked, skipped) = tracked_files_checked(&web, &registry);
        assert_eq!(tracked.keys().collect::<Vec<_>>(), ["docs/ok.md"]);
        assert_eq!(skipped.len(), keys.len());
        assert!(safe_key("a/b/c.md") && safe_key(".hidden") && safe_key("x.nool/y"));
    }

    #[test]
    fn adds_edits_and_removes_merge_across_replicas() {
        let mut a = HashWeb::new();
        let registry = a.create_kv(Id([1u8; 32]));
        track(&mut a, &registry, "readme.md", "hello\n", Id([2u8; 32]));
        // Clone to b (through the codec, like a real cp of the store).
        let mut b = decode_hashweb(&encode_hashweb(&a)).unwrap();

        // a edits readme; b adds a new file and deletes readme.
        let obj = tracked_files(&a, &registry)["readme.md"].0;
        a.seq_mut(&obj).unwrap().insert_batch(6, "world\n".chars());
        track(&mut b, &registry, "notes.md", "note\n", Id([3u8; 32]));
        b.kv_mut(&registry).unwrap().del(Value::String("readme.md".into()));

        let mut ab = decode_hashweb(&encode_hashweb(&a)).unwrap();
        ab.merge(decode_hashweb(&encode_hashweb(&b)).unwrap());
        let mut ba = decode_hashweb(&encode_hashweb(&b)).unwrap();
        ba.merge(decode_hashweb(&encode_hashweb(&a)).unwrap());

        let files_ab = tracked_files(&ab, &registry);
        let files_ba = tracked_files(&ba, &registry);
        // The registry converges: readme's delete wins over the concurrent
        // edit at the registry level (the edit history is still in the store).
        assert_eq!(
            files_ab.keys().collect::<Vec<_>>(),
            files_ba.keys().collect::<Vec<_>>()
        );
        assert!(files_ab.contains_key("notes.md"));
        assert!(!files_ab.contains_key("readme.md"));
        // And the edited history is retained even though unregistered.
        assert_eq!(ab.seq(&obj).unwrap().iter().collect::<String>(), "hello\nworld\n");
    }

    #[test]
    fn concurrent_track_of_same_path_resolves_identically() {
        let mut a = HashWeb::new();
        let registry = a.create_kv(Id([1u8; 32]));
        let mut b = decode_hashweb(&encode_hashweb(&a)).unwrap();

        track(&mut a, &registry, "plan.md", "a's plan\n", Id([2u8; 32]));
        track(&mut b, &registry, "plan.md", "b's plan\n", Id([3u8; 32]));

        let mut ab = decode_hashweb(&encode_hashweb(&a)).unwrap();
        ab.merge(decode_hashweb(&encode_hashweb(&b)).unwrap());
        let mut ba = decode_hashweb(&encode_hashweb(&b)).unwrap();
        ba.merge(decode_hashweb(&encode_hashweb(&a)).unwrap());

        let (obj_ab, conflicted_ab) = tracked_files(&ab, &registry)["plan.md"];
        let (obj_ba, conflicted_ba) = tracked_files(&ba, &registry)["plan.md"];
        assert!(conflicted_ab && conflicted_ba);
        // Both replicas pick the same winner, so working trees converge.
        assert_eq!(obj_ab, obj_ba);
        assert_eq!(
            ab.seq(&obj_ab).unwrap().iter().collect::<String>(),
            ba.seq(&obj_ba).unwrap().iter().collect::<String>()
        );
    }

    #[test]
    fn delta_carries_new_files_and_matches_full_merge() {
        use hashseq::encoding::apply_delta;

        let mut a = HashWeb::new();
        let registry = a.create_kv(Id([1u8; 32]));
        let obj = track(&mut a, &registry, "readme.md", "hello\n", Id([2u8; 32]));
        let mut b = decode_hashweb(&encode_hashweb(&a)).unwrap();

        // b diverges: edits readme, tracks a new nested file.
        b.seq_mut(&obj).unwrap().insert_batch(6, "again\n".chars());
        track(&mut b, &registry, "docs/new-notes.md", "an idea\n", Id([3u8; 32]));

        // Delta for receiver `a` from source `b`, through the file container.
        let (groups, artifacts) = crate::delta::diff(&a, &b);
        assert!(!artifacts.is_empty(), "registry puts must ship their artifacts");
        let file = crate::delta::encode_file(&groups, &artifacts);

        let mut via_delta = decode_hashweb(&encode_hashweb(&a)).unwrap();
        let (arts, msg) = crate::delta::parse_file(&file).unwrap();
        for artifact in arts {
            via_delta.provide_artifact_bytes(artifact);
        }
        let delivered = apply_delta(&mut via_delta, msg).unwrap();
        assert!(delivered > 0);
        // Replay delivers nothing.
        assert_eq!(apply_delta(&mut via_delta, msg).unwrap(), 0);

        let mut via_merge = decode_hashweb(&encode_hashweb(&a)).unwrap();
        via_merge.merge(decode_hashweb(&encode_hashweb(&b)).unwrap());

        let files_delta = tracked_files(&via_delta, &registry);
        let files_merge = tracked_files(&via_merge, &registry);
        assert_eq!(files_delta, files_merge);
        assert!(files_delta.contains_key("docs/new-notes.md"));
        for (obj, _) in files_delta.values() {
            assert_eq!(
                via_delta.seq(obj).unwrap().iter().collect::<String>(),
                via_merge.seq(obj).unwrap().iter().collect::<String>()
            );
        }
        assert_eq!(
            via_delta.seq(&obj).unwrap().iter().collect::<String>(),
            "hello\nagain\n"
        );
    }

    #[test]
    fn move_merges_with_concurrent_edit() {
        let mut a = HashWeb::new();
        let registry = a.create_kv(Id([1u8; 32]));
        let obj = track(&mut a, &registry, "draft.md", "text\n", Id([2u8; 32]));
        let mut b = decode_hashweb(&encode_hashweb(&a)).unwrap();

        // a moves the file; b concurrently edits its content.
        register_at(&mut a, &registry, "final.md", obj);
        b.seq_mut(&obj).unwrap().insert_batch(0, "more ".chars());

        let mut ab = decode_hashweb(&encode_hashweb(&a)).unwrap();
        ab.merge(decode_hashweb(&encode_hashweb(&b)).unwrap());
        let mut ba = decode_hashweb(&encode_hashweb(&b)).unwrap();
        ba.merge(decode_hashweb(&encode_hashweb(&a)).unwrap());

        for web in [&ab, &ba] {
            let files = tracked_files(web, &registry);
            // The move relocated the file (ghost at draft.md is dead), and
            // the concurrent edit landed in the same history.
            assert_eq!(files.keys().collect::<Vec<_>>(), ["final.md"]);
            assert_eq!(files["final.md"].0, obj);
            assert_eq!(web.seq(&obj).unwrap().iter().collect::<String>(), "more text\n");
        }
    }

    #[test]
    fn concurrent_moves_elect_one_home() {
        let mut a = HashWeb::new();
        let registry = a.create_kv(Id([1u8; 32]));
        let obj = track(&mut a, &registry, "f.md", "body\n", Id([2u8; 32]));
        let mut b = decode_hashweb(&encode_hashweb(&a)).unwrap();

        register_at(&mut a, &registry, "x.md", obj);
        register_at(&mut b, &registry, "y.md", obj);

        let mut ab = decode_hashweb(&encode_hashweb(&a)).unwrap();
        ab.merge(decode_hashweb(&encode_hashweb(&b)).unwrap());
        let mut ba = decode_hashweb(&encode_hashweb(&b)).unwrap();
        ba.merge(decode_hashweb(&encode_hashweb(&a)).unwrap());

        let files_ab = tracked_files(&ab, &registry);
        let files_ba = tracked_files(&ba, &registry);
        // One home elected, identically on both replicas — the file lives at
        // exactly one of the two destinations, never both (no duplication),
        // and never the source.
        assert_eq!(
            files_ab.keys().collect::<Vec<_>>(),
            files_ba.keys().collect::<Vec<_>>()
        );
        assert_eq!(files_ab.len(), 1);
        let (path, (winner, conflicted)) = files_ab.iter().next().unwrap();
        assert!(path == "x.md" || path == "y.md");
        assert_eq!(*winner, obj);
        assert!(conflicted, "concurrent move should be flagged");
    }
}
