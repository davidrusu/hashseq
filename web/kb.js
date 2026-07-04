// HashWeb knowledge base — a Notion-shaped app on a flat store of objects.
//
// Object model (all app convention, no store semantics — HASHWEB_SPEC.md
// "Op: opening and delivery"):
//   workspace kv     opened at a well-known origin (constant below)
//   ws["page:<r>"]   = ref → a page's origin (random 32 bytes)
//   page kv          opened at that origin
//   page["title"]    = string
//   page["body"]     = ref → the body seq's origin
//   page["page:<r>"] = ref → a subpage's origin (pages nest)
//
// Sync: whole-store canonical snapshots over BroadcastChannel + localStorage.
// Canonical encoding gives byte-equality as the convergence fixpoint: on
// receive we merge, re-encode, and re-broadcast only if our bytes differ —
// the ping-pong terminates exactly when both stores encode identically.

import init, { WasmHashWeb } from './pkg/hashseq.js';

await init();

// ---- conventions ----------------------------------------------------------

const WS_ORIGIN = (() => {
  const label = new TextEncoder().encode('hashweb-kb.workspace.v1');
  const bytes = new Uint8Array(32);
  bytes.set(label.slice(0, 32));
  return hex(bytes);
})();

const STORAGE_KEY = 'hashweb-kb-snapshot-v3'; // v3: threaded composite comments

function hex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function randOrigin() {
  const b = new Uint8Array(32);
  crypto.getRandomValues(b);
  return hex(b);
}

// ---- store boot ------------------------------------------------------------

function loadStore() {
  const b64 = localStorage.getItem(STORAGE_KEY);
  if (b64) {
    try {
      const raw = Uint8Array.from(atob(b64), (c) => c.charCodeAt(0));
      return WasmHashWeb.decode(raw);
    } catch (e) {
      console.warn('[kb] snapshot decode failed, starting fresh:', e);
    }
  }
  return new WasmHashWeb();
}

const web = loadStore();
const WS = web.createKv(WS_ORIGIN); // idempotent: open ≠ create

// A layout node is a seq. A LEAF is a text block. A CONTAINER's first
// element is this marker atom; its remaining elements are child-node refs.
// Orientation alternates by depth (body=vertical, then horizontal, …), so
// subdividing perpendicular to a parent is automatic — no orientation
// metadata to store or converge.
const CONTAINER_MARK = web.provideBytes(new Uint8Array([0x1a, 0x63, 0x74, 0x72]));
function nodeIsContainer(origin) {
  const obj = web.createSeq(origin);
  return web.textLen(obj) > 0 && web.payloadAt(obj, 0) === CONTAINER_MARK;
}
function childOffset(nodeOrigin) {
  return nodeOrigin === currentBody0() || !nodeIsContainer(nodeOrigin) ? 0 : 1;
}
function currentBody0() {
  return typeof currentBodyOrigin !== 'undefined' ? currentBodyOrigin : null;
}
// ---- containment registers (PLACEMENT_SPEC.md) --------------------------------
//
// Membership is decided by each node's placement register, not by the
// union of link atoms: a link atom is live iff the node's register names
// it. Moves insert a new atom and re-claim — the old atom stays as a dead
// ghost (the freeze fallback may land on it); only DELETION tombstones.
// Nodes with no register history (pre-Place data) keep the legacy
// presence rule with the deterministic duplicate heal.

const TOMB_ID = WasmHashWeb.tombstoneId();

// Register reads memoized per render; any local write or remote merge
// invalidates (placeObjAt / render() clear). Keyed by OBJECT id — layout
// nodes are seqs, pages are kvs, the register rides either.
const placementMemo = new Map();
function placementOfObj(obj) {
  let pl = placementMemo.get(obj);
  if (!pl) {
    pl = JSON.parse(web.placementOf(obj));
    placementMemo.set(obj, pl);
  }
  return pl;
}
function placementInfo(origin) {
  return placementOfObj(web.createSeq(origin));
}

/// Claim: `obj`'s placement = link atom `elemId` (or TOMB_ID to detach),
/// superseding the heads this replica sees.
function placeObjAt(obj, elemId) {
  web.placeAt(obj, elemId);
  placementMemo.delete(obj);
}
function placeNodeAt(nodeOrigin, elemId) {
  placeObjAt(web.createSeq(nodeOrigin), elemId);
}

/// The winning link atom for a node, per the register: chain[0] is the
/// single head's claim, or the last-agreed placement under conflict
/// (freeze — contenders never render). TOMB = deleted. null = legacy.
function winningAtomOf(origin) {
  const pl = placementInfo(origin);
  if (pl.empty) return null;
  const w = pl.chain[0];
  return !w || w === TOMB_ID ? undefined : w; // undefined = placed nowhere
}

/// Child node refs of a container (or of the body), membership-filtered:
/// registered children render only at their claimed atom; legacy children
/// by presence (deduped).
function childNodes2(nodeOrigin) {
  const obj = web.createSeq(nodeOrigin);
  const off = childOffset(nodeOrigin);
  const out = [];
  const seen = new Set();
  const n = web.textLen(obj);
  for (let i = off; i < n; i++) {
    const o = web.payloadAt(obj, i);
    if (!o || o === CONTAINER_MARK || seen.has(o)) continue;
    const pl = placementInfo(o);
    if (pl.empty) {
      seen.add(o);
      out.push({ idx: i, origin: o, legacy: true });
      continue;
    }
    const winner = winningAtomOf(o);
    if (winner && web.seqIdAt(obj, i) === winner) {
      seen.add(o);
      out.push({ idx: i, origin: o, conflicted: pl.conflicted });
    }
    // else: a dead ghost atom — invisible, retained for the fallback.
  }
  return out;
}
function childIndexOf(parentOrigin, origin) {
  return childNodes2(parentOrigin).findIndex((c) => c.origin === origin);
}
function makeLeafNode() {
  const o = randOrigin();
  web.createSeq(o);
  return o;
}
function makeContainerNode(childOrigins) {
  const o = randOrigin();
  const c = web.createSeq(o);
  web.seqInsertRef(c, 0, CONTAINER_MARK);
  childOrigins.forEach((co, i) => {
    const elemId = web.seqInsertRef(c, 1 + i, co);
    placeNodeAt(co, elemId); // the children now live here
  });
  return o;
}
/// DELETE a node: the register records the detachment (no fallback can
/// resurrect it), the visible atom is tombstoned as hygiene. Moves never
/// come through here — a move is insertChildAt (a fresh claim).
function removeNodeFromParent(parentOrigin, nodeOrigin) {
  placeNodeAt(nodeOrigin, TOMB_ID);
  const p = web.createSeq(parentOrigin);
  for (let i = 0; i < web.textLen(p); i++) {
    if (web.payloadAt(p, i) === nodeOrigin) {
      web.textRemove(p, i, 1);
      return;
    }
  }
}
/// Link `nodeOrigin` into `parentOrigin` at child index and CLAIM the new
/// atom — the one primitive behind birth, move, and reparent. Old atoms
/// (if any) go dead by the membership rule; they are never tombstoned.
function insertChildAt(parentOrigin, nodeOrigin, childIdx) {
  const p = web.createSeq(parentOrigin);
  const off = childOffset(parentOrigin);
  const at = Math.min(off + childIdx, web.textLen(p));
  const elemId = web.seqInsertRef(p, at, nodeOrigin);
  placeNodeAt(nodeOrigin, elemId);
  return elemId;
}
/// The new node takes the old one's slot. The old node's own register says
/// where it went (the caller placed it — typically into `newOrigin`); its
/// atom here stays as a dead ghost.
function replaceChild(parentOrigin, oldOrigin, newOrigin) {
  const ci = childIndexOf(parentOrigin, oldOrigin);
  insertChildAt(parentOrigin, newOrigin, ci < 0 ? 1e9 : ci);
}
/// Depth after edits can leave empty containers or pointless single-child
/// wrappers; a full walk from the body removes the former and unwraps the
/// latter (trees are tiny, so a whole-tree normalize is cheap and simpler
/// than incremental parent tracking).
function normalizeTree(parentOrigin, seen = new Set()) {
  const p = web.createSeq(parentOrigin);
  // Legacy heal only: for nodes WITHOUT a register, duplicate atoms are
  // visible — first occurrence in document order wins, later raw atoms
  // are removed. Registered nodes cannot visibly duplicate (membership
  // picks one atom) and their ghosts must be retained for the fallback.
  {
    const legacySeen = new Set();
    let i = childOffset(parentOrigin);
    while (i < web.textLen(p)) {
      const o = web.payloadAt(p, i);
      if (o && o !== CONTAINER_MARK && placementInfo(o).empty) {
        if (legacySeen.has(o)) {
          web.textRemove(p, i, 1);
          continue;
        }
        legacySeen.add(o);
      }
      i++;
    }
  }
  for (const c of childNodes2(parentOrigin)) {
    if (seen.has(c.origin)) continue; // cycle guard
    seen.add(c.origin);
    if (!nodeIsContainer(c.origin)) continue;
    normalizeTree(c.origin, seen);
    const kids = childNodes2(c.origin);
    if (kids.length === 0) {
      removeNodeFromParent(parentOrigin, c.origin);
    } else if (kids.length === 1) {
      // Unwrap: hoist the child to the container's slot (a move — a fresh
      // claim), then delete the container.
      const ci = childIndexOf(parentOrigin, c.origin);
      insertChildAt(parentOrigin, kids[0].origin, ci < 0 ? 1e9 : ci);
      seen.delete(kids[0].origin);
      removeNodeFromParent(parentOrigin, c.origin);
    }
  }
}
/// Every leaf block object across the whole tree (for comments etc).
/// Dedup across the WHOLE tree, not just per-parent: duplicate refs from
/// concurrent edits must never surface twice (render heals visually even
/// before normalizeTree repairs the data).
function allLeaves(parentOrigin, seen = new Set()) {
  const out = [];
  for (const c of childNodes2(parentOrigin)) {
    if (seen.has(c.origin)) continue;
    seen.add(c.origin);
    if (nodeIsContainer(c.origin)) out.push(...allLeaves(c.origin, seen));
    else out.push({ origin: c.origin, obj: web.createSeq(c.origin) });
  }
  return out;
}

// ---- kv read helpers -------------------------------------------------------

function readKey(obj, key) {
  return JSON.parse(web.readKey(obj, key));
}

/// All ref-typed values under a key (1 normally, >1 on MVR conflict).
function refsOf(obj, key) {
  const r = readKey(obj, key);
  if (r.kind === 'absent') return [];
  return r.values.filter((v) => v.type === 'ref').map((v) => v.id);
}

function stringsOf(obj, key) {
  const r = readKey(obj, key);
  if (r.kind === 'absent') return [];
  return r.values.filter((v) => v.type === 'string').map((v) => v.value);
}

/// Every object implicitly owns an ordered children list: a seq whose
/// origin derives deterministically from the object's id (the app-layer
/// twin of object_id derivation — no pointer key, nothing to race on;
/// replicas converge on the same list by construction).
function childrenListOf(parentObj) {
  return web.createSeq(WasmHashWeb.seqId(parentObj));
}

/// Child pages of a parent, membership-filtered exactly like the layout
/// tree (PLACEMENT_SPEC.md): a registered page renders only at its
/// claimed atom; legacy pages by presence (deduped).
function childrenOf(parentObj) {
  const listObj = childrenListOf(parentObj);
  const out = [];
  const seen = new Set();
  const n = web.textLen(listObj);
  for (let i = 0; i < n; i++) {
    const origin = web.payloadAt(listObj, i);
    if (!origin || seen.has(origin)) continue;
    const pl = placementOfObj(web.createKv(origin));
    if (pl.empty) {
      seen.add(origin);
      out.push({ idx: i, origin, listObj, legacy: true });
      continue;
    }
    const w = pl.chain[0];
    if (w && w !== TOMB_ID && web.seqIdAt(listObj, i) === w) {
      seen.add(origin);
      out.push({ idx: i, origin, listObj, conflicted: pl.conflicted });
    }
  }
  return out;
}

// ---- page graph (rebuilt every render; the store is the only state) --------

// pageMeta: pageObj -> { parentObj, key, title, conflict, subpages: [pageObj] }
let pageMeta = new Map();
let rootPages = [];

function openPagesUnder(parentObj, visited) {
  const out = [];
  for (const c of childrenOf(parentObj)) {
    const pageObj = web.createKv(c.origin); // open-on-discovery: app-level birth
    if (visited.has(pageObj)) continue; // duplicate-atom / cycle guard
    visited.add(pageObj);
    const titles = stringsOf(pageObj, 'title');
    pageMeta.set(pageObj, {
      parentObj,
      listObj: c.listObj,
      idx: c.idx,
      origin: c.origin,
      conflicted: c.conflicted,
      title: titles[0] ?? 'Untitled',
      conflict: titles.length > 1 ? titles : null,
      subpages: openPagesUnder(pageObj, visited),
    });
    out.push(pageObj);
  }
  return out;
}

let orphanPages = [];

function rebuildGraph() {
  pageMeta = new Map();
  const visited = new Set();
  rootPages = openPagesUnder(WS, visited);
  // The unplaced strip (the D4 recovery surface, page-tree edition):
  // pages are enumerable — kvs with a title — so any REGISTERED page the
  // walk never reached is surfaced instead of silently vanishing (cycle,
  // agreement-less conflict, or a stale client's registerless delete of
  // its atom). Legacy-unreachable pages stay buried: that is what
  // deletion meant before registers.
  orphanPages = [];
  for (const rec of JSON.parse(web.kvObjects())) {
    if (rec.obj === WS || visited.has(rec.obj)) continue;
    const titles = stringsOf(rec.obj, 'title');
    if (titles.length === 0) continue; // not a page
    const pl = placementOfObj(rec.obj);
    if (pl.empty || pl.chain[0] === TOMB_ID) continue; // legacy-dead or deleted
    visited.add(rec.obj);
    pageMeta.set(rec.obj, {
      parentObj: null,
      listObj: null,
      idx: -1,
      origin: rec.origin,
      title: titles[0] ?? 'Untitled',
      conflict: titles.length > 1 ? titles : null,
      subpages: openPagesUnder(rec.obj, visited),
      orphan: true,
    });
    orphanPages.push(rec.obj);
  }
}

/// The page's body seq object, opening it if needed. null if no body yet.
function bodyOf(pageObj) {
  const refs = refsOf(pageObj, 'body');
  if (refs.length === 0) return null;
  // On conflict (concurrent body creation) render the smallest origin —
  // deterministic on every replica; the other body is not lost, just
  // unrendered (surface later if it matters).
  return web.createSeq(refs.sort()[0]);
}

// ---- persistence + live sync ------------------------------------------------

const channel = new BroadcastChannel('hashweb-kb');
let persistTimer = null;

/// The wire/cache snapshot: OPS ONLY (empty artifact section). Artifact
/// bytes ride 0xAF pushes at upload and lazy content-addressed fetches
/// (GET /artifact/:id, immutable — the browser HTTP cache is the store).
/// This is what killed the 7MB hello and the localStorage quota bug in
/// one move: state scales with ops, images scale with the HTTP cache.
function snapshotBytes() {
  return web.encodeOps();
}

let localCacheOff = false;

// ---- delta sync (APP_NOTES #8/#19/#21 — the wall, fixed) ---------------------
//
// Steady state ships OPS, not snapshots: local edits drain the wasm
// outbox into a 0xDE delta frame (bytes to KB, not MB) sent to the
// server and the tab channel; images travel ONCE at upload as 0xAF
// artifact frames. Full snapshots remain the hello / reconnect-resync
// path only. The heavy full-state encode (localStorage cache + stats)
// is debounced separately so neither typing nor remote deltas pay it
// per keystroke.

const TAG_DELTA = 0xde;
const TAG_ARTIFACT = 0xaf;

/// Full encode + local cache + stats. Heavy — callers debounce.
function persistLocal() {
  const bytes = snapshotBytes();
  // localStorage is a *cache*, not the source of truth — the server and
  // the op DAG are. If the snapshot outgrows the ~5MB quota, drop the
  // cache and keep running; never let a cache write break sync or render.
  if (!localCacheOff) {
    try {
      let bin = '';
      for (let i = 0; i < bytes.length; i += 0x8000) {
        bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
      }
      localStorage.setItem(STORAGE_KEY, btoa(bin));
    } catch (e) {
      localCacheOff = true;
      try {
        localStorage.removeItem(STORAGE_KEY);
      } catch (_) {
        /* ignore */
      }
      console.warn('[kb] local cache disabled (snapshot exceeds quota):', e.name);
    }
  }
  statObjects.textContent = web.objectCount();
  statBytes.textContent = fmtBytes(bytes.length);
  statParked.textContent = web.orphanCount();
  return bytes;
}

let localSaveTimer = null;
function persistLocalSoon() {
  clearTimeout(localSaveTimer);
  localSaveTimer = setTimeout(persistLocal, 1200);
}

/// Drain authored ops onto the wire — cheap and immediate.
function sendDeltas() {
  const delta = web.takeDeltas();
  if (delta.length > 0) {
    try {
      channel.postMessage(delta);
    } catch (_) {
      /* channel closed */
    }
    if (wsReady) ws.send(delta);
  }
}

/// Push one artifact's bytes (image upload) to every peer, once. If the
/// server is unreachable the id is queued and re-pushed on reconnect —
/// ops travel via the snapshot resync, but artifact bytes have no other
/// road to the server.
const pendingArtifactPush = new Set();
function sendArtifact(idHex) {
  try {
    const frame = web.artifactFrame(idHex);
    try {
      channel.postMessage(frame);
    } catch (_) {
      /* channel closed */
    }
    if (wsReady) {
      ws.send(frame);
    } else {
      pendingArtifactPush.add(idHex);
    }
  } catch (_) {
    /* no local bytes — nothing to push */
  }
}

/// Lazy artifact fetch: an unresolvable id might be an artifact the
/// server holds (images arrive as ops long before their bytes now).
/// Content addressing makes the response verifiable AND immutable —
/// fetched once per browser, ever. A 404 means it is not an artifact
/// (a link to an unopened object, or bytes nobody pushed).
/// Fetch state per id: 'pending' (in flight) | 'miss' (definitively not
/// an artifact here — 404, verification failure, or offline). While a
/// value is unresolved-but-not-missed, embeds render a PLACEHOLDER and
/// never fall through to structural probes: an absent value is a
/// rendering state, not a license to reclassify (the #13 debris made
/// pending images probe as tables).
const artifactFetchState = new Map();
function requestArtifact(idHex) {
  if (artifactFetchState.has(idHex)) return;
  if (!location.protocol.startsWith('http')) {
    artifactFetchState.set(idHex, 'miss'); // file:// — structural probes may run
    return;
  }
  artifactFetchState.set(idHex, 'pending');
  fetch('/artifact/' + idHex)
    .then(async (r) => {
      if (!r.ok) {
        artifactFetchState.set(idHex, 'miss');
        repaintEmbedsOf(idHex);
        return;
      }
      const buf = new Uint8Array(await r.arrayBuffer());
      const got = web.provideArtifactBytes(buf);
      if (got !== idHex) {
        console.warn('[kb] artifact id mismatch — discarded', idHex, got);
        artifactFetchState.set(idHex, 'miss');
        return;
      }
      artifactFetchState.delete(idHex); // resolves locally from here on
      persistLocalSoon();
      // Bytes arriving change no span signature, so the incremental
      // renderer would skip every affected block (the KaTeX lesson,
      // third time): force-repaint blocks holding this atom.
      repaintEmbedsOf(idHex);
    })
    .catch(() => {
      // Network trouble: let structural probes run (tables must render
      // offline), but allow a retry on a later render.
      artifactFetchState.set(idHex, 'miss');
      repaintEmbedsOf(idHex);
      setTimeout(() => {
        if (artifactFetchState.get(idHex) === 'miss') artifactFetchState.delete(idHex);
      }, 15000);
    });
}

/// The placeholder for an unresolved value (HASHWEB_SPEC's
/// pending/unavailable state, now a first-class rendering).
function pendingEmbedNode(idHex) {
  const box = document.createElement('span');
  box.className = 'pending-embed';
  box.textContent = `⧗ fetching ⟨${idHex.slice(0, 8)}⟩`;
  box.title = 'content pending — the bytes have not arrived yet';
  return box;
}

/// Force-rerender unfocused blocks that embed `idHex` (sig-equal skips
/// would leave stale pending chips after a lazy artifact lands).
function repaintEmbedsOf(idHex) {
  for (const ed of blocksEl.querySelectorAll('.block-ed')) {
    if (ed === document.activeElement || ed.contains(document.activeElement)) continue;
    const obj = ed.dataset.blockObj;
    const t = web.text(obj);
    for (let i = 0; i < t.length; i++) {
      if (t[i] === ATOM && web.payloadAt(obj, i) === idHex) {
        rerenderBlock(ed, obj, null);
        break;
      }
    }
  }
}

function persistNow(broadcast) {
  if (broadcast) sendDeltas();
  return persistLocal();
}

function persistSoon() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => {
    sendDeltas(); // wire: fast path
    persistLocalSoon(); // cache: debounced heavy path
  }, 200);
}

/// One dispatch for both transports (tab channel + WebSocket).
/// `replySnap` sends our snapshot back on the quiesce path.
function handleSyncMessage(raw, replySnap) {
  const theirs = new Uint8Array(raw);
  if (theirs.length === 0) return;
  const savedEdit = captureEditState();
  if (theirs[0] === TAG_DELTA) {
    try {
      web.applyDelta(theirs);
    } catch (err) {
      console.warn('[kb] bad delta:', err);
    }
    persistLocalSoon();
    render();
    restoreEditState(savedEdit);
    return;
  }
  if (theirs[0] === TAG_ARTIFACT) {
    web.provideArtifactBytes(theirs.subarray(1));
    persistLocalSoon();
    render(); // pending images resolve
    restoreEditState(savedEdit);
    return;
  }
  // A snapshot: hello, reconnect resync, or a legacy peer.
  web.mergeEncoded(theirs);
  const mine = persistLocal();
  // Canonical bytes: equal op sets ⟺ identical snapshots. Re-send only
  // while we know something they don't; equality ends the exchange.
  if (!bytesEqual(mine, theirs)) replySnap(mine);
  render();
  restoreEditState(savedEdit);
}

channel.onmessage = (e) => handleSyncMessage(e.data, (mine) => channel.postMessage(mine));

// ---- server sync (hashweb-sync relay) ----------------------------------------
//
// Same protocol as the tab channel, over a WebSocket: on join the server
// sends its canonical state; local changes send our snapshot; the server
// merges (it is a replica, not a proxy of trust) and broadcasts the
// merged canonical bytes. We reply only while our bytes differ — byte
// equality quiesces the exchange.

let ws = null;
let wsReady = false;
let wsRetry = 1000;

function bytesEqual(a, b) {
  return a.length === b.length && a.every((x, i) => x === b[i]);
}

function setSyncStatus(live) {
  const dot = document.getElementById('sync-dot');
  if (live) {
    dot.style.color = 'var(--green)';
    dot.textContent = '● LIVE — synced via server; other tabs merge locally too';
  } else {
    dot.style.color = 'var(--amber)';
    dot.textContent = '◌ LOCAL — no sync server; tabs on this machine still merge';
  }
}

function connectSync() {
  const proto = location.protocol === 'https:' ? 'wss' : 'ws';
  let sock;
  try {
    sock = new WebSocket(`${proto}://${location.host}/sync`);
  } catch (_) {
    return; // file:// etc.
  }
  sock.binaryType = 'arraybuffer';
  sock.onopen = () => {
    ws = sock;
    wsReady = true;
    wsRetry = 1000;
    setSyncStatus(true);
    sock.send(snapshotBytes()); // offer what we know (ops only)
    for (const id of [...pendingArtifactPush]) {
      pendingArtifactPush.delete(id);
      sendArtifact(id); // bytes authored while offline
    }
  };
  sock.onmessage = (e) => handleSyncMessage(e.data, (mine) => sock.send(mine));
  sock.onclose = () => {
    wsReady = false;
    setSyncStatus(false);
    setTimeout(connectSync, wsRetry);
    wsRetry = Math.min(wsRetry * 2, 15000);
  };
  sock.onerror = () => sock.close();
}

if (location.protocol.startsWith('http')) connectSync();

let toastTimer = null;
function toast(msg, isErr = false) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.classList.toggle('err', isErr);
  el.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.classList.remove('show'), isErr ? 3500 : 1800);
}

function fmtBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(2)} MB`;
}

// ---- DOM -------------------------------------------------------------------

const treeEl = document.getElementById('tree');
const crumbsEl = document.getElementById('crumbs');
const titleEl = document.getElementById('title');
const conflictEl = document.getElementById('conflict-bar');
const blocksEl = document.getElementById('blocks');

const toolsEl = document.getElementById('page-tools');
const noPageEl = document.getElementById('no-page');
const statObjects = document.getElementById('stat-objects');
const statBytes = document.getElementById('stat-bytes');
const statParked = document.getElementById('stat-parked');

let current = null; // pageObj hex
let currentBody = null; // body seq obj id
let currentBodyOrigin = null; // body origin (root node id for the tree)
const viewMode = 'edit'; // WYSIWYG is the only mode now
let renderTargetObj = null; // the seq renderBody is currently rendering
let focusedBlockObj = null; // last-focused block (toolbar target)
let exposedRegion = null; // {blockObj, kind:'math'|'eqblock', ord} — source shown for editing
let treeDrag = null; // { pageObj, listObj, idx } — a sidebar drag in flight

// ---- rendering: marks + light markup ----------------------------------------
//
// Formatting (inline code, inline math, code blocks, equation blocks) is
// MARKS — ops anchored to elements, surviving concurrent edits and moving
// with the text (MARKS.md regional semantics). Structure (tables, headings)
// stays line-level markup over the same text seq. KaTeX loads lazily from a
// CDN; without it, math renders as its source.

const MARK_KINDS = ['code', 'math', 'codeblock', 'eqblock'];
const ATOM = '￼'; // an embedded object ref renders as U+FFFC in text()

let katex = null;
import('https://esm.sh/katex@0.16.11')
  .then((m) => {
    katex = m.default;
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = 'https://esm.sh/katex@0.16.11/dist/katex.min.css';
    document.head.appendChild(link);
    // Force-repaint math: blocks' span signatures didn't change (only
    // KaTeX availability did), so the incremental renderer would skip
    // them. Rerender every block/preview directly — but never under an
    // in-flight edit (a rebuild drops focus and eats keystrokes).
    const hasMath = (obj) =>
      JSON.parse(web.markedSpans(obj)).some((sp) =>
        sp.marks.some((m) => m.kind === 'math' || m.kind === 'eqblock'),
      );
    const repaintMath = () => {
      for (const ed of blocksEl.querySelectorAll('.block-ed')) {
        if (ed === document.activeElement || ed.contains(document.activeElement)) continue;
        if (hasMath(ed.dataset.blockObj)) rerenderBlock(ed, ed.dataset.blockObj, null);
      }

    };
    if (document.activeElement?.closest?.('.block-ed, [contenteditable]')) {
      window.addEventListener('focusout', repaintMath, { once: true });
    } else {
      repaintMath();
    }
  })
  .catch((e) => console.warn('[kb] KaTeX unavailable — math renders as source:', e));

function mathNode(tex, display) {
  const el = document.createElement(display ? 'div' : 'span');
  if (katex) {
    try {
      katex.render(tex, el, { displayMode: display, throwOnError: true });
      return el;
    } catch (_) {
      /* fall through to source */
    }
  }
  el.className = 'math-src';
  el.textContent = display ? `$$${tex}$$` : `$${tex}$`;
  return el;
}

/// markedSpans → per-span flags the renderer understands. Comment marks
/// are per-comment kinds (`comment:<tag>`) so overlapping comments coexist
/// instead of overwriting each other.
function styledSpans(bodyObj) {
  return JSON.parse(web.markedSpans(bodyObj)).map((s) => {
    const f = {
      text: s.text,
      code: false,
      math: false,
      codeblock: null,
      eqblock: false,
      comments: [],
    };
    for (const m of s.marks) {
      if (m.kind === 'code') f.code = true;
      else if (m.kind === 'math') f.math = true;
      else if (m.kind === 'codeblock') f.codeblock = m.values[0] ?? '';
      else if (m.kind === 'eqblock') f.eqblock = true;
      else if (m.kind.startsWith('comment:')) {
        f.comments.push({ kind: m.kind });
      }
    }
    return f;
  });
}

function commentKey(comments) {
  return comments.map((x) => x.kind).join(',');
}

/// Chunk a char-array by (code, math, comments) and emit text / <code> /
/// KaTeX / embed nodes; commented chunks get a highlight wrapper.
function chunkNodes(chars) {
  const out = [];
  let k = 0;
  while (k < chars.length) {
    const first = chars[k];
    if (first.c === ATOM) {
      out.push(renderEmbed(first.idx));
      k++;
      continue;
    }
    const { code, math } = first;
    const ckey = commentKey(first.comments);
    let text = '';
    while (
      k < chars.length &&
      chars[k].c !== ATOM &&
      chars[k].code === code &&
      chars[k].math === math &&
      commentKey(chars[k].comments) === ckey
    ) {
      text += chars[k].c;
      k++;
    }
    if (!text) continue;
    let node;
    if (math) node = mathNode(text, false);
    else if (code) {
      node = document.createElement('code');
      node.textContent = text;
    } else node = document.createTextNode(text);
    if (first.comments.length > 0) {
      const hl = document.createElement('span');
      hl.className = 'comment-hl';
      hl.dataset.tags = ckey;
      hl.title = 'commented — see the panel';
      hl.appendChild(node);
      node = hl;
    }
    out.push(node);
  }
  return out;
}

/// Caret-aware diff: a plain prefix/suffix diff is ambiguous when the
/// typed text borders equal text ("​ \cdot k" before " \cdot 42" shares
/// " \cdot ") and can slide the insertion point across a mark boundary.
/// The caret says where the edit really happened — trust it when it
/// checks out, fall back to the plain diff otherwise.
function applyDiffAt(obj, prev, next, caret) {
  if (caret != null && next.length > prev.length) {
    const n = next.length - prev.length;
    const p = caret - n;
    if (
      p >= 0 &&
      p <= prev.length &&
      prev.slice(0, p) === next.slice(0, p) &&
      prev.slice(p) === next.slice(p + n)
    ) {
      web.textInsert(obj, p, next.slice(p, p + n));
      return { kind: 'insert', p, n };
    }
  } else if (caret != null && next.length < prev.length) {
    const n = prev.length - next.length;
    if (
      caret >= 0 &&
      caret + n <= prev.length &&
      prev.slice(0, caret) === next.slice(0, caret) &&
      prev.slice(caret + n) === next.slice(caret)
    ) {
      web.textRemove(obj, caret, n);
      return { kind: 'remove', p: caret, n };
    }
  }
  if (prev !== next) {
    applyDiff(obj, prev, next);
    return { kind: 'fallback' };
  }
  return null;
}

/// WYSIWYG affinity: the browser decides whether boundary-typed text
/// renders inside or outside a styled span — treat that as the user's
/// intent and reconcile the marks to match, instead of letting the
/// normalize pass "correct" the visual a second later.
function reconcileMarkAffinity(ta, obj, p, n) {
  const sel = window.getSelection();
  if (!sel.rangeCount) return;
  let node = sel.getRangeAt(0).startContainer;
  if (node.nodeType === Node.TEXT_NODE) node = node.parentNode;
  if (!ta.contains(node)) return;
  const f = flagsAt(obj, p); // seq truth for the inserted chars
  if (!f) return;
  const len = ta.dataset.prev.length;
  const spanExtent = (pred) => {
    let s = p;
    while (s > 0) {
      const g = flagsAt(obj, s - 1);
      if (g && pred(g)) s--;
      else break;
    }
    let e = p + n;
    while (e < len) {
      const g = flagsAt(obj, e);
      if (g && pred(g)) e++;
      else break;
    }
    return [s, e];
  };

  // Inline code.
  const inCode = !!node.closest?.('code');
  if (inCode && !f.code) {
    const [a, b] = spanExtent((g) => g.code);
    web.markRangeClosed(obj, a, b, 'code', 'on');
  } else if (!inCode && f.code) {
    web.unmarkRange(obj, p, p + n, 'code');
  }

  // Code block regions (language value preserved from the neighbor).
  const inCb = !!node.closest?.('.cb-region');
  if (inCb && f.codeblock === null) {
    const [a, b] = spanExtent((g) => g.codeblock !== null);
    const lang =
      (a > 0 && flagsAt(obj, a)?.codeblock) ||
      (b < len && flagsAt(obj, b - 1)?.codeblock) ||
      '';
    web.markRangeClosed(obj, a, b, 'codeblock', lang);
  } else if (!inCb && f.codeblock !== null) {
    web.unmarkRange(obj, p, p + n, 'codeblock');
  }

  // Comments: the highlight wrapper names its exact kinds.
  const hl = node.closest?.('.comment-hl');
  const domKinds = new Set((hl?.dataset.tags ?? '').split(',').filter(Boolean));
  const seqKinds = new Set(f.comments.map((c) => c.kind));
  for (const k of seqKinds) {
    if (!domKinds.has(k)) web.unmarkRange(obj, p, p + n, k);
  }
  for (const k of domKinds) {
    if (!seqKinds.has(k)) {
      const [a, b] = spanExtent((g) => g.comments.some((c) => c.kind === k));
      web.markRange(obj, a, b, k, 'on');
    }
  }
}

/// A single-span text diff applied as seq ops.
function applyDiff(obj, prev, next) {
  let start = 0;
  const maxStart = Math.min(prev.length, next.length);
  while (start < maxStart && prev[start] === next[start]) start++;
  let endPrev = prev.length;
  let endNext = next.length;
  while (endPrev > start && endNext > start && prev[endPrev - 1] === next[endNext - 1]) {
    endPrev--;
    endNext--;
  }
  if (endPrev > start) web.textRemove(obj, start, endPrev - start);
  if (endNext > start) web.textInsert(obj, start, next.slice(start, endNext));
}

function sniffImage(b) {
  if (b.length < 12) return null;
  if (b[0] === 0x89 && b[1] === 0x50) return 'image/png';
  if (b[0] === 0xff && b[1] === 0xd8) return 'image/jpeg';
  if (b[0] === 0x47 && b[1] === 0x49) return 'image/gif';
  if (b[8] === 0x57 && b[9] === 0x45 && b[10] === 0x42 && b[11] === 0x50) return 'image/webp';
  return null;
}

// Content-addressing makes this cache sound forever: an artifact's bytes
// can never change under its id.
const imageUrlCache = new Map();

function imageUrlFor(payload) {
  if (imageUrlCache.has(payload)) return imageUrlCache.get(payload);
  const bytes = web.resolveBytes(payload);
  if (!bytes) return null;
  const mime = sniffImage(bytes);
  if (!mime) return null;
  const url = URL.createObjectURL(new Blob([bytes], { type: mime }));
  imageUrlCache.set(payload, url);
  return url;
}

/// An embedded object at position `idx`. The payload is a raw id — the
/// app's conventions decide its face: a known page's OBJECT id is a link
/// (pure name, navigates); an image artifact renders inline; a table's
/// ORIGIN renders the table; an id we can't classify renders as an inert
/// chip (never auto-opened — opening is a write).
function renderEmbed(idx) {
  const payload = web.payloadAt(renderTargetObj, idx);
  if (!payload) return document.createTextNode(ATOM);
  if (pageMeta.has(payload) || web.isKv(payload)) return pageLinkNode(payload);
  const imgUrl = imageUrlFor(payload);
  if (imgUrl) {
    const img = document.createElement('img');
    img.className = 'kb-img';
    img.src = imgUrl;
    img.draggable = false;
    return img;
  }
  if (!web.resolveBytes(payload)) {
    // Unresolved value: PLACEHOLDER until the fetch definitively misses.
    // Structural probes must not run during the pending window — old
    // auto-open debris (#13) makes them lie (images rendered as tables).
    if (artifactFetchState.get(payload) !== 'miss') {
      requestArtifact(payload);
      return pendingEmbedNode(payload);
    }
    // Definitively not an artifact (or offline): structural probes.
    const tableObj = WasmHashWeb.seqId(payload);
    // Non-empty only — empty seqs at a payload's derived id are #13
    // debris, not tables.
    if (web.isSeq(tableObj) && web.textLen(tableObj) > 0) return objTableNode(tableObj);
    const chip = document.createElement('span');
    chip.className = 'page-link broken';
    chip.textContent = `⟨${payload.slice(0, 8)}…⟩`;
    chip.title = 'unknown object reference';
    return chip;
  }
  // Bytes resolved but not a renderable image (e.g. an undecodable
  // format): an inert artifact chip, never a structural probe.
  const chip = document.createElement('span');
  chip.className = 'page-link broken';
  chip.textContent = `⟨${payload.slice(0, 8)}…⟩`;
  chip.title = 'artifact bytes present but not renderable';
  return chip;
}

function pageLinkNode(pageObj) {
  const inTree = pageMeta.has(pageObj);
  const title = inTree
    ? pageMeta.get(pageObj).title
    : (stringsOf(pageObj, 'title')[0] ?? 'untitled');
  const a = document.createElement('span');
  a.className = 'page-link' + (inTree ? '' : ' broken');
  a.textContent = `§ ${title}`;
  if (inTree) {
    a.title = 'go to page';
    a.onclick = () => {
      current = pageObj;
      render();
    };
  } else {
    a.title = 'page object exists but is not reachable from the workspace tree';
  }
  return a;
}

function objTableNode(tableObj) {
  const wrap = document.createElement('div');
  wrap.className = 'obj-table';
  const table = document.createElement('table');
  const tb = table.createTBody();
  const rowCount = web.textLen(tableObj);
  for (let r = 0; r < rowCount; r++) {
    const rowOrigin = web.payloadAt(tableObj, r);
    if (!rowOrigin) continue;
    const rowObj = web.createSeq(rowOrigin);
    const tr = tb.insertRow();
    const cellCount = web.textLen(rowObj);
    for (let c = 0; c < cellCount; c++) {
      const cellOrigin = web.payloadAt(rowObj, c);
      if (!cellOrigin) continue;
      const cellObj = web.createSeq(cellOrigin);
      const td = tr.insertCell();
      td.contentEditable = 'plaintext-only';
      td.textContent = web.text(cellObj);
      td.dataset.prev = td.textContent;
      td.addEventListener('input', (e) => {
        e.stopPropagation(); // the cell's ops are its own, not the block's
        applyDiff(cellObj, td.dataset.prev, td.textContent);
        td.dataset.prev = td.textContent;
        persistSoon();
      });
    }
  }
  wrap.appendChild(table);
  const tools = document.createElement('div');
  tools.className = 'table-tools';
  const addRow = document.createElement('button');
  addRow.textContent = '+ ROW';
  addRow.onclick = () => {
    const cols =
      rowCount > 0 ? Math.max(1, web.textLen(web.createSeq(web.payloadAt(tableObj, 0)))) : 1;
    const rowOrigin = randOrigin();
    web.createSeq(rowOrigin);
    for (let c = 0; c < cols; c++) {
      const cellOrigin = randOrigin();
      web.createSeq(cellOrigin);
      web.seqInsertRef(web.createSeq(rowOrigin), c, cellOrigin);
    }
    web.seqInsertRef(tableObj, rowCount, rowOrigin);
    persistSoon();
    render();
  };
  const addCol = document.createElement('button');
  addCol.textContent = '+ COL';
  addCol.onclick = () => {
    for (let r = 0; r < rowCount; r++) {
      const rowOrigin = web.payloadAt(tableObj, r);
      if (!rowOrigin) continue;
      const rowObj = web.createSeq(rowOrigin);
      const cellOrigin = randOrigin();
      web.createSeq(cellOrigin);
      web.seqInsertRef(rowObj, web.textLen(rowObj), cellOrigin);
    }
    persistSoon();
    render();
  };
  tools.append(addRow, addCol);
  wrap.appendChild(tools);
  return wrap;
}

function lineText(ln) {
  return ln.map((x) => x.c).join('');
}

/// Inline region (no block marks): headings, tables, paragraphs over
/// char-arrays that carry their inline mark flags.
function renderInlineRegion(el, chars) {
  const lines = [[]];
  for (const ch of chars) {
    if (ch.c === '\n') lines.push([]);
    else lines[lines.length - 1].push(ch);
  }
  let para = [];
  const flushPara = () => {
    if (para.length === 0) return;
    const p = document.createElement('p');
    para.forEach((ln, k) => {
      if (k > 0) p.appendChild(document.createElement('br'));
      p.append(...chunkNodes(ln));
    });
    el.appendChild(p);
    para = [];
  };
  let i = 0;
  while (i < lines.length) {
    const ln = lines[i];
    const text = lineText(ln);
    const h = text.match(/^(#{1,3})\s+/);
    if (h) {
      flushPara();
      const hd = document.createElement(`h${h[1].length + 1}`);
      hd.append(...chunkNodes(ln.slice(h[0].length)));
      el.appendChild(hd);
      i++;
      continue;
    }
    if (text.trim() === '') {
      flushPara();
      i++;
      continue;
    }
    para.push(ln);
    i++;
  }
  flushPara();
}

function renderBody(el, bodyObj) {
  el.innerHTML = '';
  if (!bodyObj) return;
  renderTargetObj = bodyObj;
  const spans = styledSpans(bodyObj);
  let i = 0;
  let pos = 0; // absolute visible index (payloadAt addresses atoms by it)
  while (i < spans.length) {
    if (spans[i].codeblock !== null) {
      let lang = '';
      let text = '';
      while (i < spans.length && spans[i].codeblock !== null) {
        lang = lang || spans[i].codeblock;
        text += spans[i].text;
        pos += [...spans[i].text].length;
        i++;
      }
      const wrap = document.createElement('div');
      wrap.className = 'code-block';
      if (lang) {
        const l = document.createElement('div');
        l.className = 'code-lang';
        l.textContent = lang.toUpperCase();
        wrap.appendChild(l);
      }
      const pre = document.createElement('pre');
      pre.textContent = text.replace(/^\n/, '').replace(/\n$/, '');
      wrap.appendChild(pre);
      el.appendChild(wrap);
    } else if (spans[i].eqblock) {
      let tex = '';
      while (i < spans.length && spans[i].eqblock) {
        tex += spans[i].text;
        pos += [...spans[i].text].length;
        i++;
      }
      const d = document.createElement('div');
      d.className = 'eq-block';
      d.appendChild(mathNode(tex.trim(), true));
      el.appendChild(d);
    } else {
      const chars = [];
      while (i < spans.length && spans[i].codeblock === null && !spans[i].eqblock) {
        for (const c of spans[i].text) {
          chars.push({
            c,
            code: spans[i].code,
            math: spans[i].math,
            comments: spans[i].comments,
            idx: pos,
          });
          pos++;
        }
        i++;
      }
      renderInlineRegion(el, chars);
    }
  }
}



// ---- rendering ---------------------------------------------------------------

function render() {
  placementMemo.clear();
  rebuildGraph();
  if (current && !pageMeta.has(current)) current = null;
  if (!current && rootPages.length > 0) current = rootPages[0];

  renderTree();
  renderEditor();
  statObjects.textContent = web.objectCount();
  statParked.textContent = web.orphanCount();
}

function renderTree() {
  treeEl.innerHTML = '';
  if (rootPages.length === 0) {
    const d = document.createElement('div');
    d.className = 'empty-note';
    d.textContent = 'No pages yet. Everything you create here is a content-addressed object.';
    treeEl.appendChild(d);
    return;
  }
  const clearTreeMarks = () => {
    for (const r of treeEl.querySelectorAll('.page-row')) {
      r.classList.remove('drop-above', 'drop-below', 'drop-into');
    }
  };
  // Three drop zones per row: top third = before, bottom third = after,
  // middle = INTO (become a subpage — the target's children list already
  // exists by derivation, even if it has never been touched).
  const dropZone = (row, e) => {
    const rect = row.getBoundingClientRect();
    const y = e.clientY - rect.top;
    if (y < rect.height * 0.3) return 'above';
    if (y > rect.height * 0.7) return 'below';
    return 'into';
  };
  const emit = (pages, depth) => {
    for (const p of pages) {
      const meta = pageMeta.get(p);
      const row = document.createElement('div');
      row.className = 'page-row' + (p === current ? ' active' : '');
      row.style.paddingLeft = `${12 + depth * 16}px`;
      const twirl = document.createElement('span');
      twirl.className = 'twirl';
      twirl.textContent = meta.subpages.length > 0 ? '▸' : '·';
      twirl.title = 'drag to reorder';
      const name = document.createElement('span');
      name.className = 'name';
      name.textContent = meta.title;
      row.appendChild(twirl);
      row.appendChild(name);
      if (meta.conflict) {
        const sub = document.createElement('span');
        sub.className = 'sub';
        sub.textContent = '⚠ title conflict';
        row.appendChild(sub);
      }
      if (meta.conflicted) {
        const sub = document.createElement('span');
        sub.className = 'sub';
        sub.textContent = '⚑ contested';
        sub.title = 'placement contested by a concurrent move — drag to resolve';
        row.appendChild(sub);
      }
      if (meta.orphan) row.classList.add('orphan');
      row.onclick = () => {
        current = p;
        document.body.classList.remove('show-nav'); // overlay mode closes on pick
        render();
      };

      row.draggable = true;
      row.ondragstart = (e) => {
        treeDrag = { pageObj: p, listObj: meta.listObj, idx: meta.idx, origin: meta.origin };
        row.classList.add('dragging');
        e.dataTransfer.effectAllowed = 'move';
        e.dataTransfer.setData('text/plain', meta.title);
      };
      row.ondragend = () => {
        row.classList.remove('dragging');
        clearTreeMarks();
        treeDrag = null;
      };
      row.ondragover = (e) => {
        if (!treeDrag) return;
        e.preventDefault();
        clearTreeMarks();
        const zone = dropZone(row, e);
        row.classList.add(
          zone === 'above' ? 'drop-above' : zone === 'below' ? 'drop-below' : 'drop-into',
        );
      };
      row.ondragleave = () => row.classList.remove('drop-above', 'drop-below', 'drop-into');
      row.ondrop = (e) => {
        if (!treeDrag) return;
        e.preventDefault();
        clearTreeMarks();
        const src = treeDrag;
        treeDrag = null;
        if (src.pageObj === p) return;
        // Dropping into your own subtree would orphan the subtree.
        for (let anc = p; anc && pageMeta.has(anc); anc = pageMeta.get(anc).parentObj) {
          if (anc === src.pageObj) return;
        }
        const zone = dropZone(row, e);
        if (zone === 'into') {
          // Become a subpage. Same list: one seqMove (order only, the
          // register still names the moved atom's stable id). Cross-list:
          // a fresh link + claim — the old atom goes dead by the
          // membership rule, never removed (PLACEMENT_SPEC.md).
          const target = childrenListOf(p);
          if (src.listObj === target) {
            web.seqMove(src.listObj, src.idx, web.textLen(target));
          } else {
            const elemId = web.seqInsertRef(target, web.textLen(target), src.origin);
            placeObjAt(src.pageObj, elemId);
          }
        } else {
          if (meta.orphan) return; // no ordered slot next to an unplaced page
          const slot = meta.idx + (zone === 'above' ? 0 : 1);
          if (src.listObj === meta.listObj) {
            if (slot === src.idx || slot === src.idx + 1) return;
            web.seqMove(src.listObj, src.idx, slot); // same list: ONE Move op
          } else {
            // Reparent = link + claim in the destination; the page's own
            // register decides membership, so nothing is removed and
            // concurrent reparents freeze instead of duplicating.
            const elemId = web.seqInsertRef(meta.listObj, slot, src.origin);
            placeObjAt(src.pageObj, elemId);
          }
        }
        persistSoon();
        render();
      };

      treeEl.appendChild(row);
      emit(meta.subpages, depth + 1);
    }
  };
  emit(rootPages, 0);
  if (orphanPages.length) {
    const head = document.createElement('div');
    head.className = 'orphan-note';
    head.textContent = '⚠ UNPLACED — drag back into the tree';
    treeEl.appendChild(head);
    emit(orphanPages, 0);
  }
}

function crumbPath(pageObj) {
  const path = [];
  let cur = pageObj;
  const guard = new Set();
  while (cur && pageMeta.has(cur) && !guard.has(cur)) {
    guard.add(cur);
    path.unshift(cur);
    cur = pageMeta.get(cur).parentObj;
    if (cur === WS) break;
  }
  return path;
}

function renderEditor() {
  const has = current !== null;
  document.getElementById('page').style.display = has ? '' : 'none';
  toolsEl.style.display = has ? '' : 'none';
  crumbsEl.style.display = has ? '' : 'none';
  noPageEl.style.display = has ? 'none' : '';
  if (!has) {
    renderComments();
    return;
  }

  const meta = pageMeta.get(current);

  // Crumbs
  crumbsEl.innerHTML = '';
  const wsSpan = document.createElement('span');
  wsSpan.textContent = 'WORKSPACE';
  crumbsEl.appendChild(wsSpan);
  for (const p of crumbPath(current)) {
    const sep = document.createElement('span');
    sep.textContent = '/';
    crumbsEl.appendChild(sep);
    const el = document.createElement('span');
    el.textContent = pageMeta.get(p).title;
    if (p !== current) {
      el.className = 'link';
      el.onclick = () => {
        current = p;
        render();
      };
    }
    crumbsEl.appendChild(el);
  }

  // Title (don't clobber while the user is typing in it)
  if (document.activeElement !== titleEl) {
    titleEl.value = meta.title === 'Untitled' ? '' : meta.title;
  }

  // Title conflict banner: every head surfaced, nothing auto-picked.
  if (meta.conflict) {
    conflictEl.style.display = 'block';
    conflictEl.innerHTML = '';
    conflictEl.append(`⚠ CONFLICT — concurrent titles: `);
    meta.conflict.forEach((t, i) => {
      if (i > 0) conflictEl.append(' | ');
      const b = document.createElement('b');
      b.textContent = t;
      b.style.cursor = 'pointer';
      b.title = 'click to resolve to this value';
      b.onclick = () => {
        web.putString(current, 'title', t); // names all heads → dominates
        persistSoon();
        render();
      };
      conflictEl.appendChild(b);
    });
  } else {
    conflictEl.style.display = 'none';
  }

  // Body: a seq of ROW refs; each row a seq of column blocks.
  currentBodyOrigin = refsOf(current, 'body').sort()[0] ?? null;
  currentBody = bodyOf(current);
  if (currentBody) ensureTreeSchema(current, currentBody);
  renderBlocks();
  renderComments();
}

// ---- blocks ------------------------------------------------------------------

function blocksOf(seqObj) {
  const out = [];
  const seen = new Set();
  const n = web.textLen(seqObj);
  for (let i = 0; i < n; i++) {
    const origin = web.payloadAt(seqObj, i);
    if (!origin || seen.has(origin)) continue; // dedup (concurrent migration)
    seen.add(origin);
    out.push({ idx: i, origin, obj: web.createSeq(origin) });
  }
  return out;
}

// ---- WYSIWYG machinery --------------------------------------------------
//
// Each block is a contenteditable div rendered from markedSpans: marks show
// while editing; math/equations/embeds are atomic widgets carrying their
// underlying text in data-text. Edits are extracted from the DOM (widgets
// contribute data-text) and diffed into seq ops — the DOM is a *view* of
// the seq, and the seq stays the source of truth.

const FILLER = '​'; // DOM-only caret landing pads; never content

function extractText(node) {
  let out = '';
  for (const child of node.childNodes) {
    if (child.nodeType === Node.TEXT_NODE) out += child.data.replaceAll(FILLER, '');
    else if (child.nodeType !== Node.ELEMENT_NODE) continue;
    else if (child.dataset && child.dataset.text != null) out += child.dataset.text;
    else if (child.tagName === 'BR') {
      // Sentinel <br>s give a trailing newline a caret-addressable line
      // box; they are presentation, not content.
      if (!child.dataset.sentinel) out += '\n';
    } else out += extractText(child);
  }
  return out;
}

function offsetOfPoint(blockEl, container, offset) {
  const r = document.createRange();
  r.selectNodeContents(blockEl);
  r.setEnd(container, offset);
  const div = document.createElement('div');
  div.appendChild(r.cloneContents());
  return extractText(div).length;
}

function caretOffsetIn(blockEl) {
  const sel = window.getSelection();
  if (!sel.rangeCount) return null;
  const range = sel.getRangeAt(0);
  if (!blockEl.contains(range.startContainer)) return null;
  return offsetOfPoint(blockEl, range.startContainer, range.startOffset);
}

function selectionOffsetsIn(blockEl) {
  const sel = window.getSelection();
  if (!sel.rangeCount) return null;
  const range = sel.getRangeAt(0);
  if (!blockEl.contains(range.startContainer) || !blockEl.contains(range.endContainer)) {
    return null;
  }
  return [
    offsetOfPoint(blockEl, range.startContainer, range.startOffset),
    offsetOfPoint(blockEl, range.endContainer, range.endOffset),
  ];
}

/// Locate text offset `target` as a (node, offset) DOM point. With
/// `preferAfter`, a target at the exact end of a text node resolves past
/// it — placing the caret OUTSIDE any styled wrapper ending there (the
/// affinity decides whether the next keystroke joins the span).
function locateOffset(blockEl, target, preferAfter = false) {
  let remaining = target;
  const visit = (node) => {
    for (const child of node.childNodes) {
      if (child.nodeType === Node.TEXT_NODE) {
        const clean = child.data.replaceAll(FILLER, '').length;
        if (remaining < clean || (!preferAfter && remaining === clean)) {
          // Map the cleaned offset to a raw one, stepping past fillers —
          // landing AFTER a filler keeps the caret off element boundaries
          // (Chrome normalizes boundary inserts into the previous span).
          let raw = 0;
          let seen = 0;
          while (
            raw < child.data.length &&
            (seen < remaining || child.data[raw] === FILLER)
          ) {
            if (child.data[raw] !== FILLER) seen++;
            raw++;
          }
          return { node: child, offset: raw };
        }
        remaining -= clean;
      } else if (child.nodeType === Node.ELEMENT_NODE) {
        if (child.dataset && child.dataset.text != null) {
          const len = child.dataset.text.length;
          if (remaining < len) return { node: child.parentNode, after: child };
          remaining -= len;
        } else if (child.tagName === 'BR') {
          if (child.dataset.sentinel) continue;
          if (remaining <= 0) return { node: child.parentNode, before: child };
          remaining -= 1;
        } else {
          const hit = visit(child);
          if (hit) return hit;
        }
      }
    }
    return null;
  };
  return visit(blockEl);
}

function setSelectionRangeIn(blockEl, a, b, preferAfter = false) {
  const r = document.createRange();
  const place = (target, setter) => {
    const hit = locateOffset(blockEl, target, preferAfter);
    if (!hit) {
      r[setter === 'start' ? 'setStart' : 'setEnd'](blockEl, blockEl.childNodes.length);
    } else if (hit.after) {
      r[setter === 'start' ? 'setStartAfter' : 'setEndAfter'](hit.after);
    } else if (hit.before) {
      r[setter === 'start' ? 'setStartBefore' : 'setEndBefore'](hit.before);
    } else {
      r[setter === 'start' ? 'setStart' : 'setEnd'](hit.node, hit.offset);
    }
  };
  place(a, 'start');
  place(b ?? a, 'end');
  const sel = window.getSelection();
  sel.removeAllRanges();
  sel.addRange(r);
}

function makeWidget(inner, text, cls) {
  const w = document.createElement('span');
  w.className = `inline-widget ${cls}`;
  w.contentEditable = 'false';
  w.dataset.text = text;
  w.appendChild(inner);
  return w;
}

/// Click a math/equation widget (or arrow into it) to expose its source
/// in place: the region's chars render as editable text instead of KaTeX
/// until the caret leaves. No dialogs — the seq is edited directly, and
/// the mark's regional points keep the region alive throughout.
/// Region extent [start, end) of the ord'th region of `kind` in a block.
function regionExtent(blockObj, kind, ord) {
  let pos = 0;
  let n = -1;
  let start = null;
  for (const sp of styledSpans(blockObj)) {
    const inRegion = kind === 'math' ? sp.math && sp.codeblock === null && !sp.eqblock : sp.eqblock;
    if (inRegion && start === null) {
      n++;
      if (n === ord) start = pos;
    }
    if (!inRegion && start !== null && n === ord) return [start, pos];
    if (!inRegion) start = null;
    pos += sp.text.length;
  }
  return start !== null ? [start, pos] : null;
}

function exposeRegion(ed, blockObj, kind, ord, caretOff) {
  // Re-author the mark OPEN-ended while editing: appended source must
  // join the region. (Collapse re-closes it.)
  const ext = regionExtent(blockObj, kind, ord);
  if (ext) web.markRange(blockObj, ext[0], ext[1], kind, 'on');
  exposedRegion = { blockObj, kind, ord };
  rerenderBlock(ed, blockObj, caretOff);
  persistSoon();
}

function collapseExposedRegion() {
  if (!exposedRegion) return;
  const { blockObj, kind, ord } = exposedRegion;
  exposedRegion = null;
  // Re-close the end anchor: typing after the rendered widget stays plain.
  const ext = regionExtent(blockObj, kind, ord);
  if (ext && ext[1] > ext[0]) {
    web.markRangeClosed(blockObj, ext[0], ext[1], kind, 'on');
    persistSoon();
  }
  for (const ed of blocksEl.querySelectorAll('.block-ed')) {
    if (ed.dataset.blockObj === blockObj) {
      const focused = ed.contains(document.activeElement) || document.activeElement === ed;
      rerenderBlock(ed, blockObj, focused ? caretOffsetIn(ed) : null);
      if (!focused) ed.blur?.();
      break;
    }
  }
}

// Collapse the exposed source the moment the caret leaves it.
document.addEventListener('selectionchange', () => {
  const sel = window.getSelection();
  if (!sel.rangeCount) return;
  let node = sel.getRangeAt(0).startContainer;
  if (node.nodeType === Node.TEXT_NODE) node = node.parentNode;
  if (exposedRegion && !node.closest?.('.region-live')) collapseExposedRegion();
  // Caret inside a commented span counts as focusing the comment.
  const hl = node.closest?.('.comment-hl');
  const next = new Set(
    hl && node.closest('.block-ed')
      ? (hl.dataset.tags ?? '').split(',').filter((t) => t.startsWith('comment:'))
      : [],
  );
  if (!setsEqual(next, caretCommentKinds)) {
    caretCommentKinds = next;
    updateCommentHighlights();
  }
});

/// Render a block's content as editable, formatted DOM.
function renderEditableInto(ed, blockObj) {
  ed.innerHTML = '';
  renderTargetObj = blockObj;
  // Live heading styling: a leading '#'/'##'/'###' makes the whole block a
  // heading (the markup marker stays but reads as an affordance).
  const plain = web.text(blockObj);
  const hm = plain.match(/^(#{1,3}) /);
  ed.classList.remove('bl-h1', 'bl-h2', 'bl-h3');
  if (hm) ed.classList.add(`bl-h${hm[1].length}`);
  const spans = styledSpans(blockObj);
  let i = 0;
  let pos = 0;
  const ords = { math: 0, eqblock: 0 };
  const isExposed = (kind, ord) =>
    exposedRegion &&
    exposedRegion.blockObj === blockObj &&
    exposedRegion.kind === kind &&
    exposedRegion.ord === ord;
  while (i < spans.length) {
    if (spans[i].codeblock !== null) {
      let text = '';
      while (i < spans.length && spans[i].codeblock !== null) {
        text += spans[i].text;
        pos += [...spans[i].text].length;
        i++;
      }
      // Editable in place: an inline shaded region (spans whole lines, so
      // it reads as a block) whose chars are the region's chars exactly.
      const region = document.createElement('span');
      region.className = 'cb-region';
      region.appendChild(document.createTextNode(text));
      ed.appendChild(region);
    } else if (spans[i].eqblock) {
      const start = pos;
      let tex = '';
      while (i < spans.length && spans[i].eqblock) {
        tex += spans[i].text;
        pos += [...spans[i].text].length;
        i++;
      }
      const ord = ords.eqblock++;
      if (isExposed('eqblock', ord)) {
        const live = document.createElement('span');
        live.className = 'region-live eq-live';
        live.appendChild(document.createTextNode(tex));
        ed.appendChild(live);
      } else {
        const w = makeWidget(mathNode(tex.trim(), true), tex, 'w-eq');
        w.title = 'click to edit the equation source';
        w.dataset.start = start;
        w.dataset.kind = 'eqblock';
        w.dataset.ord = ord;
        w.onclick = () => exposeRegion(ed, blockObj, 'eqblock', ord, start + tex.length);
        ed.appendChild(w);
      }
    } else {
      const chars = [];
      while (i < spans.length && spans[i].codeblock === null && !spans[i].eqblock) {
        for (const c of spans[i].text) {
          chars.push({
            c,
            code: spans[i].code,
            math: spans[i].math,
            comments: spans[i].comments,
            idx: pos,
          });
          pos++;
        }
        i++;
      }
      emitEditableChunks(ed, chars, blockObj, ords, isExposed);
    }
  }
  // A trailing newline needs a line box for the caret to land on.
  if (extractText(ed).endsWith('\n')) {
    const br = document.createElement('br');
    br.dataset.sentinel = '1';
    ed.appendChild(br);
  }
  // Filler landing pads (ZWSP, stripped from extraction) after every
  // styled element and before a leading one: they keep caret positions
  // off element boundaries, where Chrome would normalize the insertion
  // into the adjacent span.
  for (const child of [...ed.childNodes]) {
    if (
      child.nodeType === Node.ELEMENT_NODE &&
      child.tagName !== 'BR' &&
      !(child.nextSibling?.nodeType === Node.TEXT_NODE &&
        child.nextSibling.data.startsWith(FILLER))
    ) {
      child.after(document.createTextNode(FILLER));
    }
  }
  if (ed.firstChild?.nodeType === Node.ELEMENT_NODE && ed.firstChild.tagName !== 'BR') {
    ed.insertBefore(document.createTextNode(FILLER), ed.firstChild);
  }
}

function emitEditableChunks(ed, chars, blockObj, ords, isExposed) {
  let k = 0;
  while (k < chars.length) {
    const first = chars[k];
    if (first.c === ATOM) {
      const inner = renderEmbed(first.idx);
      const isLink = inner.classList?.contains('page-link');
      const isImg = inner.classList?.contains('kb-img');
      const w = makeWidget(inner, ATOM, isLink ? 'w-link' : isImg ? 'w-img' : 'w-embed');
      ed.appendChild(w);
      k++;
      continue;
    }
    if (first.math) {
      const start = first.idx;
      let src = '';
      while (k < chars.length && chars[k].math && chars[k].c !== ATOM) {
        src += chars[k].c;
        k++;
      }
      const ord = ords.math++;
      if (isExposed('math', ord)) {
        const live = document.createElement('span');
        live.className = 'region-live math-live';
        live.appendChild(document.createTextNode(src));
        ed.appendChild(live);
      } else {
        const w = makeWidget(mathNode(src, false), src, 'w-math');
        w.title = 'click to edit the math source';
        w.dataset.start = start;
        w.dataset.kind = 'math';
        w.dataset.ord = ord;
        w.onclick = () => exposeRegion(ed, blockObj, 'math', ord, start + src.length);
        ed.appendChild(w);
      }
      continue;
    }
    const { code } = first;
    const ckey = commentKey(first.comments);
    let text = '';
    while (
      k < chars.length &&
      chars[k].c !== ATOM &&
      !chars[k].math &&
      chars[k].code === code &&
      commentKey(chars[k].comments) === ckey
    ) {
      text += chars[k].c;
      k++;
    }
    if (!text) continue;
    let node = document.createTextNode(text);
    if (code) {
      const c = document.createElement('code');
      c.appendChild(node);
      node = c;
    }
    if (first.comments.length > 0) {
      const hl = document.createElement('span');
      hl.className = 'comment-hl';
      hl.dataset.tags = ckey;
      hl.title = 'commented — see the panel';
      hl.appendChild(node);
      node = hl;
    }
    ed.appendChild(node);
  }
}

function rerenderBlock(ed, blockObj, caretOff, preferAfter = false) {
  renderEditableInto(ed, blockObj);
  ed.dataset.prev = extractText(ed);
  const row = ed.closest?.('.block-row');
  if (row) row.dataset.sig = web.markedSpans(blockObj);
  if (caretOff != null) {
    ed.focus();
    setSelectionRangeIn(
      ed,
      Math.min(caretOff, ed.dataset.prev.length),
      undefined,
      preferAfter,
    );
  }
}

let normalizeTimer = null;

function clearDropMarks() {
  for (const el of blocksEl.querySelectorAll('.block-col')) {
    el.classList.remove('drop-above', 'drop-below', 'drop-left', 'drop-right');
  }
}

// ---- layout tree: leaves and containers --------------------------------------
//
// The body is the root container (vertical). A child is a LEAF (text block)
// or a CONTAINER (marker + child refs). Orientation alternates by depth, so
// dropping a block perpendicular to its target's parent auto-creates a
// container the other way — arbitrary subdivision from one rule.

/// One-time migration into the tree model. Handles both the flat-block
/// legacy and the intermediate rows model via the page's bodySchema flag.
function ensureTreeSchema(page, body) {
  const schema = stringsOf(page, 'bodySchema');
  if (schema.includes('tree')) return;
  const isRows = schema.includes('rows');
  const atoms = [];
  const n = web.textLen(body);
  for (let i = 0; i < n; i++) {
    const o = web.payloadAt(body, i);
    if (o) atoms.push(o);
  }
  // A flat body IS already a valid tree (a list of leaf refs): flag it
  // and touch nothing. Rewriting atoms here tombstones registered
  // children's claimed atoms (remove-wins = deletion) — found the hard
  // way when a delta-synced fresh page rendered on a peer before its
  // bodySchema flag arrived and 'migrated' itself invisible. Same guard
  // for any body that already has registered children: it cannot be
  // pre-tree data, whatever the flag says.
  if (!isRows || atoms.some((o) => !placementOfObj(web.createSeq(o)).empty)) {
    web.putString(page, 'bodySchema', 'tree');
    persistSoon();
    return;
  }
  web.textRemove(body, 0, web.textLen(body));
  for (const ao of atoms) {
    // rows model (pre-register data, presence rule): ao is a row seq of
    // column blocks.
    const row = web.createSeq(ao);
    const cols = [];
    for (let i = 0; i < web.textLen(row); i++) {
      const c = web.payloadAt(row, i);
      if (c) cols.push(c);
    }
    if (cols.length <= 1) {
      web.seqInsertRef(body, web.textLen(body), cols[0] ?? ao);
    } else {
      web.seqInsertRef(row, 0, CONTAINER_MARK); // reuse the row seq as a container
      web.seqInsertRef(body, web.textLen(body), ao);
    }
  }
  web.putString(page, 'bodySchema', 'tree');
  persistSoon();
}

let dragCol = null; // { origin, parentOrigin }

/// Drop the dragged leaf beside `col` on side `dir`. Parallel to the
/// target's parent axis → sibling insert; perpendicular → wrap target in a
/// new container (which, being one level deeper, arranges the other way).
function dropOnLeaf(col, dir) {
  const src = dragCol;
  dragCol = null;
  if (!src) return;
  const tOrigin = col.dataset.origin;
  if (src.origin === tOrigin) return;
  const tParent = col.dataset.parentOrigin;
  const tDepth = Number(col.dataset.depth);
  // A move is never a delete: the new claim supersedes the old placement,
  // and the old atom stays as a dead ghost.
  const parentAxis = (tDepth - 1) % 2 === 0 ? 'V' : 'H';
  const dirAxis = dir === 'left' || dir === 'right' ? 'H' : 'V';
  const before = dir === 'left' || dir === 'top';
  const ti = childIndexOf(tParent, tOrigin);
  if (ti < 0) {
    normalizeTree(currentBodyOrigin);
    persistSoon();
    render();
    return;
  }
  if (parentAxis === dirAxis) {
    insertChildAt(tParent, src.origin, ti + (before ? 0 : 1));
  } else {
    // Wrap: build the container (which claims both children into it),
    // then claim the container at the target's old slot. Order matters —
    // ti was computed before the target's membership moved.
    const kids = before ? [src.origin, tOrigin] : [tOrigin, src.origin];
    const cont = makeContainerNode(kids);
    insertChildAt(tParent, cont, ti);
  }
  normalizeTree(currentBodyOrigin);
  persistSoon();
  render();
}

function newLeafToNewRow(bodyIdx) {
  const src = dragCol;
  dragCol = null;
  if (!src) return;
  insertChildAt(currentBodyOrigin, src.origin, bodyIdx); // move = re-claim
  normalizeTree(currentBodyOrigin);
  persistSoon();
  render();
}

/// Build a leaf block editor ONCE; renderBlocks keeps its tree context
/// (origin/parentOrigin/depth) updated via dataset. Leaves persist across
/// renders so an untouched block keeps its DOM caret and selection.
// ---- cross-block keyboard flow -------------------------------------------------
// The layout tree should be invisible to the keyboard: arrows glide across
// block edges, and backspace / ctrl-d at an edge merges neighbours, as if
// the blocks were one continuous document.

/// Tree parent of a node (body children live at offset 0, containers at 1).
function parentOfNode(nodeOrigin, parentOrigin = currentBodyOrigin) {
  for (const c of childNodes2(parentOrigin)) {
    if (c.origin === nodeOrigin) return parentOrigin;
    if (nodeIsContainer(c.origin)) {
      const r = parentOfNode(nodeOrigin, c.origin);
      if (r) return r;
    }
  }
  return null;
}

function leafNeighbors(origin) {
  const ls = allLeaves(currentBodyOrigin);
  const i = ls.findIndex((l) => l.origin === origin);
  return {
    prev: i > 0 ? ls[i - 1] : null,
    next: i >= 0 && i < ls.length - 1 ? ls[i + 1] : null,
  };
}

function edOfOrigin(origin) {
  return blocksEl.querySelector(`.block-col[data-origin="${origin}"] .block-ed`);
}

function focusLeaf(origin, offset) {
  const ed = edOfOrigin(origin);
  if (!ed) return;
  ed.focus();
  setSelectionRangeIn(ed, offset === 'end' ? extractText(ed).length : offset);
}

function removeLeaf(origin) {
  const parent = parentOfNode(origin) ?? currentBodyOrigin;
  removeNodeFromParent(parent, origin);
  normalizeTree(currentBodyOrigin);
}

/// The caret's client rect; empty blocks fall back to the editor's box.
function caretClientRect(ta) {
  const sel = window.getSelection();
  if (!sel.rangeCount) return null;
  const range = sel.getRangeAt(0);
  if (!ta.contains(range.startContainer)) return null;
  const rects = range.getClientRects();
  if (rects.length) return rects[0];
  const rb = range.getBoundingClientRect();
  if (rb.top || rb.height || rb.width) return rb;
  return ta.getBoundingClientRect();
}

/// Client rect of a text offset, without touching the live selection.
function rectAtOffset(ed, off) {
  const hit = locateOffset(ed, off);
  const r = document.createRange();
  if (!hit) {
    r.selectNodeContents(ed);
    r.collapse(off === 0);
  } else if (hit.after) {
    r.setStartAfter(hit.after);
    r.collapse(true);
  } else if (hit.before) {
    r.setStartBefore(hit.before);
    r.collapse(true);
  } else {
    r.setStart(hit.node, hit.offset);
    r.collapse(true);
  }
  const rects = r.getClientRects();
  return rects[0] ?? r.getBoundingClientRect();
}

/// Is the caret on the block's first ('up') or last ('down') visual line?
/// Same-line means the caret's line box overlaps the line box of the
/// block's first (resp. last) text position — robust across mixed
/// line-heights (headings, code regions, image lines). caretRangeFromPoint
/// is useless here: it snaps points over padding back to the nearest text.
function caretOnEdgeLine(ta, dir) {
  if (extractText(ta).length === 0) return true;
  const c = caretClientRect(ta);
  if (!c) return false;
  const edge = rectAtOffset(ta, dir === 'up' ? 0 : extractText(ta).length);
  if (!edge || (!edge.height && !edge.top)) return true;
  const tol = Math.max(c.height, edge.height, 8) * 0.6;
  return dir === 'up' ? Math.abs(c.top - edge.top) < tol : Math.abs(c.bottom - edge.bottom) < tol;
}

/// Land the caret in `ed` on its first/last line, as close to client-x
/// as possible — preserves the column when flowing between blocks.
function placeCaretNearX(ed, x, fromTop) {
  const r = ed.getBoundingClientRect();
  const cx = Math.min(Math.max(x, r.left + 2), r.right - 2);
  const y = fromTop ? r.top + 10 : r.bottom - 10;
  const probe = document.caretRangeFromPoint(cx, y);
  if (probe && ed.contains(probe.startContainer)) {
    setSelectionRangeIn(ed, offsetOfPoint(ed, probe.startContainer, probe.startOffset));
  } else {
    setSelectionRangeIn(ed, fromTop ? 0 : extractText(ed).length);
  }
}

/// Append src's content — text, embeds, and marks — onto the end of dst.
/// Returns the offset in dst where src's content begins (the join point).
function mergeBlockInto(dstObj, srcObj) {
  const base = web.textLen(dstObj);
  const txt = web.text(srcObj);
  let i = 0;
  while (i < txt.length) {
    if (txt[i] === ATOM) {
      const p = web.payloadAt(srcObj, i);
      if (p != null) web.seqInsertRef(dstObj, base + i, p);
      else web.textInsert(dstObj, base + i, ' ');
      i++;
    } else {
      let j = i;
      while (j < txt.length && txt[j] !== ATOM) j++;
      web.textInsert(dstObj, base + i, txt.slice(i, j));
      i = j;
    }
  }
  let off = 0;
  for (const sp of JSON.parse(web.markedSpans(srcObj))) {
    const s = off;
    const e = off + sp.text.length;
    off = e;
    for (const m of sp.marks) {
      web.markRangeClosed(dstObj, base + s, base + e, m.kind, m.values[0] ?? '');
    }
  }
  return base;
}

function makeColumn(obj, origin) {
  const col = document.createElement('div');
  col.className = 'block-col';
  col.dataset.obj = obj;
  col.dataset.origin = origin;

  const handle = document.createElement('span');
  handle.className = 'handle';
  handle.textContent = '⠿';
  handle.title = 'drag to move, or drop beside/under another block to split';
  handle.draggable = true;
  handle.ondragstart = (e) => {
    dragCol = { origin: col.dataset.origin, parentOrigin: col.dataset.parentOrigin };
    col.classList.add('dragging');
    e.dataTransfer.effectAllowed = 'move';
    e.dataTransfer.setData('text/plain', 'block');
  };
  handle.ondragend = () => {
    col.classList.remove('dragging');
    clearDropMarks();
    dragCol = null;
  };

  col.ondragover = (e) => {
    if (!dragCol) return;
    e.preventDefault();
    e.stopPropagation();
    const rect = col.getBoundingClientRect();
    const fx = (e.clientX - rect.left) / rect.width;
    const fy = (e.clientY - rect.top) / rect.height;
    clearDropMarks();
    if (Math.min(fx, 1 - fx) < Math.min(fy, 1 - fy)) {
      col.classList.add(fx < 0.5 ? 'drop-left' : 'drop-right');
    } else {
      col.classList.add(fy < 0.5 ? 'drop-above' : 'drop-below');
    }
  };
  col.ondragleave = () => clearDropMarks();
  col.ondrop = (e) => {
    if (!dragCol) return;
    e.preventDefault();
    e.stopPropagation();
    clearDropMarks();
    const rect = col.getBoundingClientRect();
    const fx = (e.clientX - rect.left) / rect.width;
    const fy = (e.clientY - rect.top) / rect.height;
    let dir;
    if (Math.min(fx, 1 - fx) < Math.min(fy, 1 - fy)) dir = fx < 0.5 ? 'left' : 'right';
    else dir = fy < 0.5 ? 'top' : 'bottom';
    dropOnLeaf(col, dir);
  };

  const ta = document.createElement('div');
  ta.className = 'block-ed';
  ta.contentEditable = 'true';
  ta.spellcheck = false;
  ta.dataset.blockObj = obj;

  const refreshSig = () => {
    col.dataset.sig = web.markedSpans(obj);
  };

  const insertNewlineAtCaret = () => {
    const sel = window.getSelection();
    if (!sel.rangeCount) return;
    const r = sel.getRangeAt(0);
    if (!ta.contains(r.startContainer)) return;
    r.deleteContents();
    const tn = document.createTextNode('\n');
    r.insertNode(tn);
    r.setStartAfter(tn);
    r.collapse(true);
    sel.removeAllRanges();
    sel.addRange(r);
    if (extractText(ta).endsWith('\n') && !ta.querySelector('br[data-sentinel]')) {
      const br = document.createElement('br');
      br.dataset.sentinel = '1';
      ta.appendChild(br);
    }
    commitBlockEdit();
  };
  const commitBlockEdit = () => {
    const next = extractText(ta);
    const did = applyDiffAt(obj, ta.dataset.prev, next, caretOffsetIn(ta));
    ta.dataset.prev = next;
    if (did?.kind === 'insert') reconcileMarkAffinity(ta, obj, did.p, did.n);
    refreshSig();
    persistSoon();
    return did;
  };

  ta.addEventListener('beforeinput', (e) => {
    if (e.inputType === 'insertParagraph' || e.inputType === 'insertLineBreak') {
      e.preventDefault();
      insertNewlineAtCaret();
    }
  });
  ta.addEventListener('keydown', (e) => {
    if (slashState && slashState.ta === ta) {
      if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
        e.preventDefault();
        moveSlash(e.key === 'ArrowDown' ? 1 : -1);
        return;
      }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        runSlash(slashState.active);
        return;
      }
      if (e.key === 'Escape') {
        e.preventDefault();
        hideSlash();
        return;
      }
    }
    if (e.key !== 'Enter') return;
    e.preventDefault();
    // Shift+Enter (the common idiom) or cmd/ctrl+Enter: newline within
    // the block. Plain Enter: a new block.
    if (e.shiftKey || e.metaKey || e.ctrlKey) {
      insertNewlineAtCaret();
      return;
    }
    // Enter: a new leaf after this one in its parent. A plain tail moves in.
    const caret = caretOffsetIn(ta) ?? extractText(ta).length;
    const text = extractText(ta);
    const tail = text.slice(caret);
    const tailPlain =
      !tail.includes(ATOM) &&
      (() => {
        for (let q = caret; q < text.length; q++) {
          const f = flagsAt(obj, q);
          if (f && (f.code || f.math || f.codeblock !== null || f.eqblock || f.comments.length)) {
            return false;
          }
        }
        return true;
      })();
    const nb = makeLeafNode();
    const nbObj = web.createSeq(nb);
    if (tail && tailPlain) {
      web.textRemove(obj, caret, text.length - caret);
      web.textInsert(nbObj, 0, tail);
      // No refreshSig() here: the col's sig must stay stale so the render
      // below sees the mismatch and rebuilds this editor without its old
      // tail. Pre-stamping the sig left the DOM (and dataset.prev) showing
      // text the seq no longer has.
    }
    const parent = col.dataset.parentOrigin;
    const ci = childIndexOf(parent, col.dataset.origin);
    insertChildAt(parent, nb, ci + 1);
    persistSoon();
    render();
    for (const ed of blocksEl.querySelectorAll('.block-ed')) {
      if (ed.dataset.blockObj === nbObj) {
        ed.focus();
        setSelectionRangeIn(ed, 0);
        break;
      }
    }
  });
  ta.addEventListener('keydown', (e) => {
    if (e.key !== 'ArrowLeft' && e.key !== 'ArrowRight') return;
    const caret = caretOffsetIn(ta);
    if (caret == null) return;
    for (const w of ta.querySelectorAll('.inline-widget.w-math, .inline-widget.w-eq')) {
      const start = Number(w.dataset.start);
      const len = w.dataset.text.length;
      if (e.key === 'ArrowRight' && caret === start) {
        e.preventDefault();
        exposeRegion(ta, obj, w.dataset.kind, Number(w.dataset.ord), start + 1);
        return;
      }
      if (e.key === 'ArrowLeft' && caret === start + len) {
        e.preventDefault();
        exposeRegion(ta, obj, w.dataset.kind, Number(w.dataset.ord), start + len);
        return;
      }
    }
  });
  // Cross-block flow: arrows glide over block edges; backspace / ctrl-d at
  // an edge merges neighbours. The tree stays out of the keyboard's way.
  ta.addEventListener('keydown', (e) => {
    if (e.defaultPrevented) return;
    if (e.metaKey || e.altKey) return;
    const sel = window.getSelection();
    if (!sel.rangeCount || !sel.getRangeAt(0).collapsed) return;

    if ((e.key === 'ArrowUp' || e.key === 'ArrowDown') && !e.shiftKey && !e.ctrlKey) {
      const dir = e.key === 'ArrowUp' ? 'up' : 'down';
      if (!caretOnEdgeLine(ta, dir)) return;
      const { prev, next } = leafNeighbors(col.dataset.origin);
      if (dir === 'up' && !prev) {
        e.preventDefault();
        titleEl.focus();
        titleEl.setSelectionRange(titleEl.value.length, titleEl.value.length);
        return;
      }
      const target = dir === 'up' ? prev : next;
      if (!target) return;
      const ted = edOfOrigin(target.origin);
      if (!ted) return;
      e.preventDefault();
      const x = caretClientRect(ta)?.left ?? 0;
      ted.focus();
      placeCaretNearX(ted, x, dir === 'down');
      return;
    }
    if (e.key === 'ArrowLeft' && !e.shiftKey && !e.ctrlKey && caretOffsetIn(ta) === 0) {
      const { prev } = leafNeighbors(col.dataset.origin);
      if (!prev) return;
      e.preventDefault();
      focusLeaf(prev.origin, 'end');
      return;
    }
    if (
      e.key === 'ArrowRight' &&
      !e.shiftKey &&
      !e.ctrlKey &&
      caretOffsetIn(ta) === extractText(ta).length
    ) {
      const { next } = leafNeighbors(col.dataset.origin);
      if (!next) return;
      e.preventDefault();
      focusLeaf(next.origin, 0);
      return;
    }

    const isBackspace = e.key === 'Backspace' && !e.ctrlKey;
    const isFwd = (e.key === 'd' && e.ctrlKey) || e.key === 'Delete';
    if (!isBackspace && !isFwd) return;
    const caret = caretOffsetIn(ta);
    if (caret == null) return;
    const len = web.textLen(obj);
    const txt = web.text(obj);

    if (txt === '' || txt === '\n') {
      // Empty block (Chrome represents a cleared block as a lone <br>,
      // which commits as a single newline): the delete deletes the block.
      const { prev, next } = leafNeighbors(col.dataset.origin);
      if (!prev && !next) return; // never delete the only block
      e.preventDefault();
      removeLeaf(col.dataset.origin);
      persistSoon();
      render();
      const t = isBackspace ? (prev ?? next) : (next ?? prev);
      focusLeaf(t.origin, t === prev ? 'end' : 0);
      return;
    }
    if (isBackspace && caret === 0) {
      // Collapse this block into the previous one; caret at the join.
      const { prev } = leafNeighbors(col.dataset.origin);
      if (!prev) return;
      e.preventDefault();
      const at = mergeBlockInto(prev.obj, obj);
      removeLeaf(col.dataset.origin);
      persistSoon();
      render();
      focusLeaf(prev.origin, at);
      return;
    }
    if (isFwd && caret === len) {
      // Pull the next block's content into this one; caret stays put.
      const { next } = leafNeighbors(col.dataset.origin);
      if (!next) return;
      e.preventDefault();
      mergeBlockInto(obj, next.obj);
      removeLeaf(next.origin);
      persistSoon();
      render();
      focusLeaf(col.dataset.origin, caret);
    }
  });
  ta.addEventListener('paste', (e) => {
    e.preventDefault();
    const imgItem = [...(e.clipboardData.items ?? [])].find((it) =>
      it.type.startsWith('image/'),
    );
    if (imgItem) {
      const file = imgItem.getAsFile();
      if (file) {
        const at = caretOffsetIn(ta) ?? extractText(ta).length;
        insertImageFile(file, ta, at);
        return;
      }
    }
    const clean = e.clipboardData.getData('text/plain').replaceAll(FILLER, '');
    document.execCommand('insertText', false, clean);
  });
  ta.addEventListener('input', (e) => {
    if (e.isComposing) return;
    commitBlockEdit();
    if (updateSlash(ta)) return;
    const ruled = maybeInputRule(ta, obj, e.data);
    if (ruled != null) {
      clearTimeout(normalizeTimer);
      rerenderBlock(ta, obj, ruled, true);
    } else {
      clearTimeout(normalizeTimer);
      normalizeTimer = setTimeout(() => {
        if (document.activeElement === ta || ta.contains(document.activeElement)) {
          rerenderBlock(ta, obj, caretOffsetIn(ta));
        }
      }, 900);
    }
  });
  ta.addEventListener('drop', (e) => {
    if (dragCol) e.preventDefault();
  });
  ta.onfocus = () => {
    focusedBlockObj = obj;
  };
  ta.addEventListener('blur', () =>
    setTimeout(() => {
      if (slashState && slashState.ta === ta && !slashMenu.matches(':hover')) hideSlash();
    }, 120),
  );

  const del = document.createElement('span');
  del.className = 'b-del';
  del.textContent = '✕';
  del.title = 'remove block';
  del.onclick = () => {
    removeNodeFromParent(col.dataset.parentOrigin, col.dataset.origin);
    normalizeTree(currentBodyOrigin);
    persistSoon();
    render();
  };

  col.append(handle, ta, del);
  return col;
}

function updateLeafContent(col, obj, ed) {
  const sig = web.markedSpans(obj);
  if (col.dataset.sig === sig) return;
  const focused = ed === document.activeElement || ed.contains(document.activeElement);
  const seqText = JSON.parse(sig)
    .map((sp) => sp.text)
    .join('');
  if (focused && extractText(ed) === seqText) {
    col.dataset.sig = sig;
    clearTimeout(normalizeTimer);
    normalizeTimer = setTimeout(() => {
      rerenderBlock(
        ed,
        obj,
        ed === document.activeElement || ed.contains(document.activeElement)
          ? caretOffsetIn(ed)
          : null,
      );
    }, 400);
  } else {
    renderEditableInto(ed, obj);
    ed.dataset.prev = extractText(ed);
    col.dataset.sig = sig;
  }
}

function renderNodeInto(parentEl, origin, depth, parentOrigin, prevLeaves, single, seen, conflicted) {
  if (seen.has(origin)) return; // duplicate ref — render first occurrence only
  seen.add(origin);
  if (nodeIsContainer(origin)) {
    const cont = document.createElement('div');
    cont.className = 'node-container';
    cont.style.flexDirection = depth % 2 === 0 ? 'column' : 'row';
    for (const k of childNodes2(origin)) {
      renderNodeInto(cont, k.origin, depth + 1, origin, prevLeaves, false, seen, k.conflicted);
    }
    parentEl.appendChild(cont);
    return;
  }
  const obj = web.createSeq(origin);
  let col = prevLeaves.get(origin);
  if (col) prevLeaves.delete(origin);
  else col = makeColumn(obj, origin);
  col.dataset.parentOrigin = parentOrigin;
  col.dataset.depth = depth;
  // Contested placement (two register heads): frozen at last-agreed,
  // badged; the next drag names both heads and resolves.
  col.classList.toggle('pl-conflict', !!conflicted);
  col.title = conflicted ? 'placement contested by a concurrent move — drag to resolve' : '';
  const ed = col.querySelector('.block-ed');
  if (single) ed.dataset.placeholder = 'Type here…';
  else delete ed.dataset.placeholder;
  updateLeafContent(col, obj, ed);
  parentEl.appendChild(col);
}

function ensureAddButton() {
  let add = blocksEl.querySelector('.add-block');
  if (!add) {
    add = document.createElement('button');
    add.className = 'add-block';
    add.textContent = '+ BLOCK';
    add.onclick = () => addBlockAtEnd();
    blocksEl.appendChild(add);
  }
  return add;
}

/// Recursive render. Leaves persist keyed by origin (caret survives); the
/// container DOM is rebuilt each render and reused leaves are re-parented.
function renderBlocks() {
  for (const d of blocksEl.querySelectorAll('.dragging')) d.classList.remove('dragging');
  const prevLeaves = new Map();
  for (const c of blocksEl.querySelectorAll('.block-col')) prevLeaves.set(c.dataset.origin, c);

  const frag = document.createDocumentFragment();
  const topKids = childNodes2(currentBodyOrigin);
  const single = topKids.length === 1 && !nodeIsContainer(topKids[0].origin);
  const seen = new Set();
  for (const k of topKids) {
    renderNodeInto(frag, k.origin, 1, currentBodyOrigin, prevLeaves, single, seen, k.conflicted);
  }

  for (const c of prevLeaves.values()) c.remove();
  blocksEl
    .querySelectorAll(':scope > .node-container, :scope > .block-col')
    .forEach((e) => e.remove());
  const add = ensureAddButton();
  for (const child of [...frag.childNodes]) blocksEl.insertBefore(child, add);
  if (blocksEl.lastChild !== add) blocksEl.appendChild(add);

  blocksEl.ondragover = (e) => {
    if (dragCol) e.preventDefault();
  };
  blocksEl.ondrop = (e) => {
    if (dragCol && (e.target === blocksEl || e.target === add)) {
      newLeafToNewRow(childNodes2(currentBodyOrigin).length);
    }
  };
}

// ---- caret survival across remote merges -------------------------------------
//
// Offsets shift when a peer's insert lands before your caret; element ids
// do not. Capture the id of the character LEFT of each selection endpoint
// BEFORE merging, and re-derive positions from those ids afterwards — the
// selection re-attaches to the same characters wherever they now sit.

function captureEditState() {
  const ed = document.activeElement?.closest?.('.block-ed');
  if (!ed) return null;
  const obj = ed.dataset.blockObj;
  const sel = selectionOffsetsIn(ed);
  if (!sel) return null;
  let anchor = null;
  let focus = null;
  try {
    anchor = sel[0] > 0 ? web.seqIdAt(obj, sel[0] - 1) : null;
    focus = sel[1] > 0 ? web.seqIdAt(obj, sel[1] - 1) : null;
  } catch (_) {
    return null;
  }
  return { obj, anchor, focus, aOff: sel[0], bOff: sel[1] };
}

function restoreEditState(st) {
  if (!st) return;
  for (const ed of blocksEl.querySelectorAll('.block-ed')) {
    if (ed.dataset.blockObj !== st.obj) continue;
    const len = extractText(ed).length;
    const back = (id, off) => {
      if (id == null) return 0;
      const p = web.seqPositionOf(st.obj, id);
      return p == null ? Math.min(off, len) : p + 1; // deleted anchor: best effort
    };
    ed.focus();
    setSelectionRangeIn(ed, back(st.anchor, st.aOff), back(st.focus, st.bOff));
    return;
  }
}

function addBlockAtEnd(focus = true) {
  const body = ensureBody();
  const leaf = makeLeafNode();
  const elemId = web.seqInsertRef(body, web.textLen(body), leaf);
  placeNodeAt(leaf, elemId);
  persistSoon();
  render();
  if (focus) {
    const eds = blocksEl.querySelectorAll('.block-ed');
    eds[eds.length - 1]?.focus();
  }
}



// ---- comments ----------------------------------------------------------------

/// One comment = one identity (its tag — a 32-byte origin) realized as N
/// per-block mark fragments plus a discussion thread: the seq opened AT
/// the tag. Grouping across blocks by kind reassembles the composite.
function collectComments() {
  if (!currentBody) return [];
  const byKind = new Map();
  for (const b of allLeaves(currentBodyOrigin)) {
    let pos = 0;
    for (const s of styledSpans(b.obj)) {
      for (const c of s.text) {
        for (const cm of s.comments) {
          let e = byKind.get(cm.kind);
          if (!e) {
            e = { kind: cm.kind, tag: cm.kind.slice('comment:'.length), fragments: [] };
            byKind.set(cm.kind, e);
          }
          const last = e.fragments[e.fragments.length - 1];
          if (last && last.obj === b.obj && last.end === pos) {
            last.quote += c;
            last.end = pos + 1;
          } else {
            e.fragments.push({ obj: b.obj, start: pos, end: pos + 1, quote: c });
          }
        }
        pos++;
      }
    }
  }
  const out = [];
  for (const e of byKind.values()) {
    if (!/^[0-9a-f]{64}$/.test(e.tag)) continue; // not a thread-tagged comment
    const thread = web.createSeq(e.tag); // the tag IS the thread's origin
    e.thread = thread;
    e.messages = web.text(thread).split('\n').filter(Boolean);
    out.push(e);
  }
  return out;
}

let activeCommentTag = null; // pinned by clicking a card
let hoverCommentKinds = new Set(); // hovering a span or a card
let caretCommentKinds = new Set(); // caret sitting inside a commented span

/// One source of truth: a span (and its card) lights up iff its kind is
/// hovered, caret-focused, or pinned — otherwise it stays dimmed.
function updateCommentHighlights() {
  const lit = new Set([...hoverCommentKinds, ...caretCommentKinds]);
  if (activeCommentTag) lit.add(activeCommentTag);
  for (const el of document.querySelectorAll('.comment-hl')) {
    const tags = (el.dataset.tags ?? '').split(',');
    el.classList.toggle('hl-active', tags.some((t) => lit.has(t)));
  }
  for (const card of document.querySelectorAll('.cc-card')) {
    card.classList.toggle('lit', lit.has(card.dataset.kind) && card.dataset.kind !== activeCommentTag);
    card.classList.toggle('active', card.dataset.kind === activeCommentTag);
  }
}

function setsEqual(a, b) {
  return a.size === b.size && [...a].every((x) => b.has(x));
}

// Hovering a commented span lights it (and its card) up.
document.addEventListener('mouseover', (e) => {
  const hl = e.target.closest?.('.comment-hl');
  const next = new Set(
    hl ? (hl.dataset.tags ?? '').split(',').filter((t) => t.startsWith('comment:')) : [],
  );
  if (!setsEqual(next, hoverCommentKinds)) {
    hoverCommentKinds = next;
    updateCommentHighlights();
  }
});

// Clicking anywhere outside the cards unpins.
document.addEventListener('click', (e) => {
  if (activeCommentTag && !e.target.closest?.('.cc-card')) {
    activeCommentTag = null;
    updateCommentHighlights();
  }
});
let pendingComposeTag = null; // focus this comment's composer after render

/// Replace (or delete, when `next` is empty) message `idx` in a thread —
/// messages are the non-empty newline-delimited lines.
function replaceThreadMessage(thread, idx, next) {
  const text = web.text(thread);
  let off = 0;
  let mi = 0;
  for (const line of text.split('\n')) {
    if (line !== '') {
      if (mi === idx) {
        if (next === line) return;
        if (next) {
          if (line.length) web.textRemove(thread, off, line.length);
          web.textInsert(thread, off, next);
        } else {
          web.textRemove(thread, off, line.length + 1); // line + newline
        }
        return;
      }
      mi++;
    }
    off += line.length + 1;
  }
}

function renderComments() {
  const panel = document.getElementById('comments');
  const comments = current ? collectComments() : [];
  const toggle = document.getElementById('comments-toggle');
  document.getElementById('ct-count').textContent = comments.length;
  toggle.style.visibility = comments.length === 0 ? 'hidden' : '';
  const collapsed =
    !commentsIsOverlay() && document.body.classList.contains('collapse-comments');
  if (comments.length === 0 || collapsed) {
    panel.style.display = 'none';
    if (comments.length === 0) {
      activeCommentTag = null;
      document.body.classList.remove('show-comments');
    }
    return;
  }
  panel.style.display = 'flex';
  panel.innerHTML = '';
  const head = document.createElement('div');
  head.className = 'c-head';
  head.textContent = `COMMENTS (${comments.length})`;
  panel.appendChild(head);

  for (const cm of comments) {
    const card = document.createElement('div');
    card.className = 'cc-card' + (cm.kind === activeCommentTag ? ' active' : '');
    card.dataset.kind = cm.kind;

    const quote = document.createElement('div');
    quote.className = 'cc-quote';
    quote.textContent = cm.fragments.map((f) => f.quote).join(' … ');
    quote.title = cm.fragments.length > 1 ? `${cm.fragments.length} fragments` : '';
    card.appendChild(quote);

    cm.messages.forEach((msg, idx) => {
      const m = document.createElement('div');
      m.className = 'cc-msg';
      m.textContent = msg;
      m.title = 'click to edit';
      m.onclick = (e) => {
        e.stopPropagation();
        if (m.isContentEditable) return;
        m.contentEditable = 'plaintext-only';
        m.classList.add('editing');
        m.focus();
        const r = document.createRange();
        r.selectNodeContents(m);
        r.collapse(false);
        const sel = window.getSelection();
        sel.removeAllRanges();
        sel.addRange(r);
        const commit = () => {
          m.onblur = null;
          replaceThreadMessage(cm.thread, idx, m.textContent.trim());
          persistSoon();
          renderComments();
        };
        m.onblur = commit;
        m.onkeydown = (ev) => {
          if (ev.key === 'Enter' && !ev.shiftKey) {
            ev.preventDefault();
            commit();
          } else if (ev.key === 'Escape') {
            m.onblur = null;
            renderComments();
          }
        };
      };
      card.appendChild(m);
    });

    const reply = document.createElement('input');
    reply.className = 'cc-reply';
    reply.placeholder = cm.messages.length === 0 ? 'write a comment…' : 'reply…';
    reply.onclick = (e) => e.stopPropagation();
    reply.onkeydown = (e) => {
      if (e.key !== 'Enter' || !reply.value.trim()) return;
      web.textInsert(cm.thread, web.textLen(cm.thread), `${reply.value.trim()}\n`);
      persistSoon();
      renderComments();
    };
    card.appendChild(reply);

    const tools = document.createElement('div');
    tools.className = 'cc-tools';
    const resolve = document.createElement('span');
    resolve.className = 'cc-resolve';
    resolve.textContent = 'RESOLVE ✕';
    resolve.title = 'tombstone every fragment of this comment';
    resolve.onclick = (e) => {
      e.stopPropagation();
      for (const f of cm.fragments) {
        web.unmarkRange(f.obj, f.start, f.end, cm.kind);
      }
      if (activeCommentTag === cm.kind) activeCommentTag = null;
      persistSoon();
      render();
    };
    tools.appendChild(resolve);
    card.appendChild(tools);

    // Hover or pin the card → light up every fragment it references.
    card.onmouseenter = () => {
      hoverCommentKinds = new Set([cm.kind]);
      updateCommentHighlights();
    };
    card.onmouseleave = () => {
      hoverCommentKinds = new Set();
      updateCommentHighlights();
    };
    card.onclick = () => {
      activeCommentTag = activeCommentTag === cm.kind ? null : cm.kind;
      updateCommentHighlights();
    };
    panel.appendChild(card);
    if (pendingComposeTag === cm.kind) {
      pendingComposeTag = null;
      setTimeout(() => reply.focus(), 0);
    }
  }
  updateCommentHighlights();
}

// ---- editing -----------------------------------------------------------------

function ensureBody() {
  if (currentBody) return currentBody;
  const origin = randOrigin();
  web.putRef(current, 'body', origin);
  currentBody = web.createSeq(origin);
  return currentBody;
}

// ---- input rules: markup typed inline becomes marks -------------------------
//
// Typing the closing delimiter converts the span to a MARK and deletes the
// delimiters from the seq: `x` → code, $x$ → math, $$x$$ → equation block,
// a closing ``` fence → code block (language from the opening fence). The
// mark is authored BEFORE the delimiters are removed — their elements are
// live anchors at mark time, and the regional points survive on their
// ghosts after the tombstone.

function flagsAt(blockObj, pos) {
  let off = 0;
  for (const sp of styledSpans(blockObj)) {
    off += sp.text.length;
    if (pos < off) return sp;
  }
  return null;
}

function plainAt(blockObj, pos) {
  const f = flagsAt(blockObj, pos);
  return !f || (!f.code && !f.math && f.codeblock === null && !f.eqblock);
}

/// Ran after the typed char is already in the seq. Returns the new caret
/// offset when a rule fired, else null.
function maybeInputRule(ed, blockObj, typed) {
  if (typed !== '`' && typed !== '$') return null;
  const text = ed.dataset.prev;
  const caret = caretOffsetIn(ed);
  if (caret == null || caret === 0) return null;
  const p = caret - 1;
  if (text[p] !== typed) return null;

  if (typed === '`') {
    // Closing fence: the line up to the caret is exactly ``` .
    const lineStart = text.lastIndexOf('\n', p - 1) + 1;
    if (text.slice(lineStart, caret) === '```' && (caret === text.length || text[caret] === '\n')) {
      let at = lineStart - 1;
      while (at > 0) {
        const os = text.lastIndexOf('\n', at - 1) + 1;
        const line = text.slice(os, at);
        if (line.startsWith('```')) {
          const lang = line.slice(3).trim();
          const co = os + line.length + 1; // content start (after fence newline)
          const ce = lineStart - 1; // the newline before the closing fence
          if (ce <= co) return null; // empty fence
          web.markRangeClosed(blockObj, co, ce, 'codeblock', lang);
          web.textRemove(blockObj, ce, caret - ce); // "\n```"
          web.textRemove(blockObj, os, co - os); // "```lang\n"
          return ce - (co - os);
        }
        at = os - 1;
      }
      return null;
    }
    // Inline code: `content`
    const i = text.lastIndexOf('`', p - 1);
    if (i === -1) return null;
    const content = text.slice(i + 1, p);
    if (!content || content.includes('\n') || content.includes(ATOM)) return null;
    if (!plainAt(blockObj, i + 1) || !plainAt(blockObj, i)) return null;
    web.markRangeClosed(blockObj, i + 1, p, 'code', 'on');
    web.textRemove(blockObj, p, 1);
    web.textRemove(blockObj, i, 1);
    return p - 1;
  }

  // '$' rules.
  if (text[p - 1] === '$') {
    // $$content$$ on one line → equation block.
    const k = text.lastIndexOf('$$', p - 2);
    if (k === -1) return null;
    const content = text.slice(k + 2, p - 1);
    if (!content || content.includes('\n') || content.includes('$') || content.includes(ATOM)) {
      return null;
    }
    if (!plainAt(blockObj, k + 2) || !plainAt(blockObj, k)) return null;
    web.markRangeClosed(blockObj, k + 2, p - 1, 'eqblock', 'on');
    web.textRemove(blockObj, p - 1, 2);
    web.textRemove(blockObj, k, 2);
    return p - 3;
  }
  // $content$ → inline math.
  const i = text.lastIndexOf('$', p - 1);
  if (i === -1 || text[i - 1] === '$') return null;
  const content = text.slice(i + 1, p);
  if (!content || content.includes('\n') || content.includes(ATOM)) return null;
  if (!plainAt(blockObj, i + 1) || !plainAt(blockObj, i)) return null;
  web.markRangeClosed(blockObj, i + 1, p, 'math', 'on');
  web.textRemove(blockObj, p, 1);
  web.textRemove(blockObj, i, 1);
  return p - 1;
}

/// Every block the current selection touches, with per-block offsets —
/// the input for composite (cross-block) comment marks.
function selectionFragments() {
  const sel = window.getSelection();
  if (!sel.rangeCount || sel.isCollapsed) return [];
  const range = sel.getRangeAt(0);
  const frags = [];
  for (const ed of blocksEl.querySelectorAll('.block-ed')) {
    if (!range.intersectsNode(ed)) continue;
    const r = range.cloneRange();
    const edRange = document.createRange();
    edRange.selectNodeContents(ed);
    if (r.compareBoundaryPoints(Range.START_TO_START, edRange) < 0) {
      r.setStart(edRange.startContainer, edRange.startOffset);
    }
    if (r.compareBoundaryPoints(Range.END_TO_END, edRange) > 0) {
      r.setEnd(edRange.endContainer, edRange.endOffset);
    }
    const a = offsetOfPoint(ed, r.startContainer, r.startOffset);
    const b = offsetOfPoint(ed, r.endContainer, r.endOffset);
    if (a < b) frags.push({ obj: ed.dataset.blockObj, a, b });
  }
  return frags;
}

/// The toolbar's target block editor. Toolbar buttons preventDefault on
/// mousedown, so focus and selection stay in the block while clicking.
function focusedTA() {
  const el = document.activeElement?.closest?.('.block-ed');
  if (el) return el;
  if (focusedBlockObj) {
    for (const ed of blocksEl.querySelectorAll('.block-ed')) {
      if (ed.dataset.blockObj === focusedBlockObj) return ed;
    }
  }
  return null;
}

let titleTimer = null;
titleEl.addEventListener('input', () => {
  if (!current) return;
  clearTimeout(titleTimer);
  titleTimer = setTimeout(() => {
    web.putString(current, 'title', titleEl.value || 'Untitled');
    persistSoon();
    render();
  }, 350);
});
// The title is line zero of the document: Enter opens a fresh first
// block, ArrowDown flows into the body.
titleEl.addEventListener('keydown', (e) => {
  if (!current || !currentBodyOrigin) return;
  if (e.key === 'Enter') {
    e.preventDefault();
    clearTimeout(titleTimer);
    web.putString(current, 'title', titleEl.value || 'Untitled');
    const nb = makeLeafNode();
    insertChildAt(currentBodyOrigin, nb, 0);
    persistSoon();
    render();
    focusLeaf(nb, 0);
  } else if (e.key === 'ArrowDown') {
    const ls = allLeaves(currentBodyOrigin);
    if (!ls.length) return;
    e.preventDefault();
    focusLeaf(ls[0].origin, 0);
  }
});

function createPage(parentObj) {
  const origin = randOrigin();
  const list = childrenListOf(parentObj);
  const elemId = web.seqInsertRef(list, web.textLen(list), origin);
  const page = web.createKv(origin);
  placeObjAt(page, elemId); // claimed at birth
  web.putString(page, 'title', 'Untitled');
  const bodyOrigin = randOrigin();
  web.putRef(page, 'body', bodyOrigin);
  const body = web.createSeq(bodyOrigin);
  {
    const firstLeaf = makeLeafNode();
    const elemId = web.seqInsertRef(body, 0, firstLeaf);
    placeNodeAt(firstLeaf, elemId);
  }
  web.putString(page, 'bodySchema', 'tree');
  current = page;
  persistSoon();
  render();
  titleEl.focus();
}

document.getElementById('new-page').onclick = () => createPage(WS);
document.getElementById('new-subpage').onclick = () => current && createPage(current);

// Inline confirmation: first click arms the button, second click (within
// 3s) deletes. No dialogs anywhere in the app.
let deleteArmTimer = null;
document.getElementById('delete-page').onclick = (e) => {
  if (!current) return;
  const btn = e.target;
  if (!btn.dataset.armed) {
    btn.dataset.armed = '1';
    btn.textContent = 'REALLY DELETE?';
    clearTimeout(deleteArmTimer);
    deleteArmTimer = setTimeout(() => {
      delete btn.dataset.armed;
      btn.textContent = 'DELETE PAGE';
    }, 3000);
    return;
  }
  clearTimeout(deleteArmTimer);
  delete btn.dataset.armed;
  btn.textContent = 'DELETE PAGE';
  const meta = pageMeta.get(current);
  // Delete = the register records detachment (unresurrectable by any
  // fallback), the atom is tombstoned as hygiene.
  placeObjAt(current, TOMB_ID);
  if (meta?.listObj != null && meta.idx >= 0) {
    web.textRemove(meta.listObj, meta.idx, 1);
  }
  current = null;
  persistSoon();
  render();
};

// ---- format toolbar (marks over the focused block's selection) ---------------

function selectionLines(text, selStart, selEnd) {
  const start = text.lastIndexOf('\n', Math.max(0, selStart - 1)) + 1;
  const nl = text.indexOf('\n', selEnd);
  return [start, nl === -1 ? text.length : nl];
}

function insertTableAt(ta, at) {
  const blockObj = ta.dataset.blockObj;
  // A table is a seq of row refs; a row is a seq of cell refs; a cell is a
  // text seq — a hashseq of hashseqs, embedded as a link atom.
  const tableOrigin = randOrigin();
  const tableObj = web.createSeq(tableOrigin);
  for (let r = 0; r < 2; r++) {
    const rowOrigin = randOrigin();
    const rowObj = web.createSeq(rowOrigin);
    for (let c = 0; c < 2; c++) {
      const cellOrigin = randOrigin();
      web.createSeq(cellOrigin);
      web.seqInsertRef(rowObj, c, cellOrigin);
    }
    web.seqInsertRef(tableObj, r, rowOrigin);
  }
  web.seqInsertRef(blockObj, at, tableOrigin);
  persistSoon();
  rerenderBlock(ta, blockObj, at + 1); // the table appears in place
}

function toBlobAsync(canvas, type, quality) {
  return new Promise((res) => canvas.toBlob(res, type, quality));
}

/// Downscale + recompress before storing (snapshots carry every artifact),
/// but NEVER drop the image: every failure mode falls back to the raw
/// bytes so at worst it stores un-optimized instead of vanishing.
async function processImageFile(file) {
  const raw = new Uint8Array(await file.arrayBuffer());
  let bmp;
  try {
    bmp = await createImageBitmap(file); // throws on HEIC etc.
  } catch (_) {
    return raw; // undecodable here — store as-is, the <img> may still show it
  }
  const MAX = 1400;
  const scale = Math.min(1, MAX / Math.max(bmp.width, bmp.height));
  if (scale === 1 && raw.length < 400_000) return raw;

  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(bmp.width * scale));
  canvas.height = Math.max(1, Math.round(bmp.height * scale));
  canvas.getContext('2d').drawImage(bmp, 0, 0, canvas.width, canvas.height);
  // WebP first (smallest, keeps alpha), then JPEG, then raw — toBlob
  // returns null for unsupported types (older Safari), so guard each.
  for (const [type, q] of [['image/webp', 0.85], ['image/jpeg', 0.85]]) {
    const blob = await toBlobAsync(canvas, type, q);
    if (blob && blob.type === type) {
      const out = new Uint8Array(await blob.arrayBuffer());
      if (out.length < raw.length) return out;
    }
  }
  return raw;
}

const MAX_IMAGE_BYTES = 1_500_000; // whole snapshots sync, so cap hard

async function insertImageFile(file, ta, at) {
  try {
    if (!file.type.startsWith('image/')) {
      toast('Not an image file', true);
      return;
    }
    const bytes = await processImageFile(file);
    if (bytes.length > MAX_IMAGE_BYTES) {
      // Almost always an undecodable format (HEIC) that fell back to raw
      // and couldn't be downscaled — guide the user rather than bloat the
      // shared state.
      toast(
        `Image too large (${fmtBytes(bytes.length)}). ` +
          `If it's a HEIC/iPhone photo, export or screenshot it as JPEG/PNG first.`,
        true,
      );
      return;
    }
    const imgId = web.provideBytes(bytes);
    sendArtifact(imgId); // bytes travel once; ops ride the delta path
    const blockObj = ta.dataset.blockObj;
    web.seqInsertRef(blockObj, at, imgId);
    persistSoon();
    rerenderBlock(ta, blockObj, at + 1);
    toast(`Image added (${fmtBytes(bytes.length)})`);
  } catch (e) {
    console.error('[kb] image insert failed:', e);
    toast('Could not read that image', true);
  }
}

const imageFileEl = document.getElementById('image-file');
imageFileEl.onchange = async () => {
  const file = imageFileEl.files?.[0];
  imageFileEl.value = '';
  if (!file) return;
  // The file dialog steals focus; fall back to the last-focused block, or
  // the last block, or a fresh one — the button must never no-op silently.
  let ta = focusedTA();
  if (!ta) {
    const eds = blocksEl.querySelectorAll('.block-ed');
    ta = eds[eds.length - 1] ?? null;
  }
  if (!ta) {
    addBlockAtEnd(false);
    ta = blocksEl.querySelector('.block-ed');
  }
  if (!ta) {
    toast('Open a page first', true);
    return;
  }
  const at = caretOffsetIn(ta) ?? extractText(ta).length;
  await insertImageFile(file, ta, at);
};

function openLinkPicker(ta, at, anchorRect) {
  const blockObj = ta.dataset.blockObj;
  document.getElementById('link-menu')?.remove();

  const menu = document.createElement('div');
  menu.id = 'link-menu';
  const head = document.createElement('div');
  head.className = 'lm-head';
  head.textContent = 'LINK TO PAGE';
  menu.appendChild(head);
  const emit = (pages, depth) => {
    for (const p of pages) {
      const meta = pageMeta.get(p);
      const item = document.createElement('div');
      item.className = 'lm-item';
      item.style.paddingLeft = `${10 + depth * 14}px`;
      item.textContent = (p === current ? '◦ ' : '') + meta.title;
      item.onclick = () => {
        menu.remove();
        // The link atom's payload is the page's OBJECT id — a pure name.
        web.seqInsertRef(blockObj, at, p);
        persistSoon();
        rerenderBlock(ta, blockObj, at + 1); // the link appears in place
          };
      menu.appendChild(item);
      emit(meta.subpages, depth + 1);
    }
  };
  emit(rootPages, 0);
  if (rootPages.length === 0) {
    const none = document.createElement('div');
    none.className = 'lm-item';
    none.textContent = '(no pages)';
    menu.appendChild(none);
  }
  menu.style.left = `${anchorRect.left}px`;
  menu.style.top = `${anchorRect.bottom + 4}px`;
  document.body.appendChild(menu);
  setTimeout(() => {
    document.addEventListener('click', () => menu.remove(), { once: true });
  }, 0);
}

// ---- floating selection toolbar (inline formatting) --------------------------
//
// Appears just above a text selection inside a block; no fixed chrome. The
// selection is what these ops need, so they live where the selection is.

const selToolbar = document.getElementById('sel-toolbar');
selToolbar.addEventListener('mousedown', (e) => e.preventDefault());

function applyInlineMark(kind) {
  if (!current || viewMode === 'view') return;
  if (kind === 'comment') {
    const frags = selectionFragments();
    if (frags.length === 0) return;
    const tag = randOrigin();
    web.createSeq(tag);
    for (const f of frags) web.markRange(f.obj, f.a, f.b, `comment:${tag}`, 'on');
    activeCommentTag = `comment:${tag}`;
    pendingComposeTag = `comment:${tag}`;
    persistSoon();
    hideSelToolbar();
    render();
    return;
  }
  const ta = focusedTA();
  if (!ta) return;
  const blockObj = ta.dataset.blockObj;
  const sel = selectionOffsetsIn(ta);
  if (!sel) return;
  const [a, b] = sel;
  if (a >= b) return;
  if (kind === 'clear') {
    for (const k of MARK_KINDS) web.unmarkRange(blockObj, a, b, k);
  } else {
    web.markRange(blockObj, a, b, kind, 'on');
  }
  persistSoon();
  rerenderBlock(ta, blockObj, null);
  setSelectionRangeIn(ta, a, b);
  ta.focus();
  hideSelToolbar();
}

for (const btn of selToolbar.querySelectorAll('button')) {
  btn.onclick = () => applyInlineMark(btn.dataset.act);
}

function hideSelToolbar() {
  selToolbar.classList.remove('show');
}

function positionSelToolbar() {
  if (viewMode === 'view') return hideSelToolbar();
  const sel = window.getSelection();
  if (!sel.rangeCount || sel.isCollapsed) return hideSelToolbar();
  const inEd =
    sel.anchorNode?.parentElement?.closest?.('.block-ed') ||
    sel.focusNode?.parentElement?.closest?.('.block-ed');
  if (!inEd) return hideSelToolbar();
  const rect = sel.getRangeAt(0).getBoundingClientRect();
  if (rect.width === 0 && rect.height === 0) return hideSelToolbar();
  selToolbar.classList.add('show');
  const tw = selToolbar.offsetWidth || 140;
  let left = rect.left + rect.width / 2 - tw / 2;
  left = Math.max(8, Math.min(left, window.innerWidth - tw - 8));
  let top = rect.top - selToolbar.offsetHeight - 8;
  if (top < 8) top = rect.bottom + 8;
  selToolbar.style.left = `${left}px`;
  selToolbar.style.top = `${top}px`;
}

document.addEventListener('selectionchange', () => {
  clearTimeout(selToolbar._t);
  selToolbar._t = setTimeout(positionSelToolbar, 10);
});
document.addEventListener('scroll', hideSelToolbar, true);

// ---- slash command menu (block insertions) -----------------------------------
//
// Type "/" at the start of an empty block to insert a block-level thing.

const SLASH_ITEMS = [
  { key: 'H', label: 'Heading', desc: 'section title', run: (ta) => makeHeading(ta) },
  { key: '<>', label: 'Code block', desc: 'monospace', run: (ta) => makeBlockKind(ta, 'codeblock') },
  { key: 'S', label: 'Equation', desc: 'display math', run: (ta) => makeBlockKind(ta, 'eqblock') },
  { key: '#', label: 'Table', desc: '2 by 2 grid', run: (ta) => insertTableAt(ta, caretNow(ta)) },
  { key: 'I', label: 'Image', desc: 'upload or paste', run: () => imageFileEl.click() },
  { key: 'P', label: 'Page link', desc: 'link a page', run: (ta, rect) => openLinkPicker(ta, caretNow(ta), rect) },
];

const slashMenu = document.getElementById('slash-menu');
let slashState = null;

function caretNow(ta) {
  return caretOffsetIn(ta) ?? extractText(ta).length;
}

function makeHeading(ta) {
  const obj = ta.dataset.blockObj;
  if (!extractText(ta).startsWith('# ')) web.textInsert(obj, 0, '# ');
  persistSoon();
  rerenderBlock(ta, obj, extractText(ta).length);
}

function makeBlockKind(ta, kind) {
  const obj = ta.dataset.blockObj;
  if (web.textLen(obj) === 0) web.textInsert(obj, 0, ' ');
  const len = web.textLen(obj);
  web.markRangeClosed(obj, 0, len, kind, '');
  persistSoon();
  rerenderBlock(ta, obj, len);
}

function renderSlash(query) {
  const q = query.toLowerCase();
  const items = SLASH_ITEMS.filter((it) => it.label.toLowerCase().includes(q));
  slashState.filtered = items;
  slashState.active = 0;
  slashMenu.innerHTML = '';
  if (items.length === 0) return hideSlash();
  items.forEach((it, i) => {
    const row = document.createElement('div');
    row.className = 'sl-item' + (i === 0 ? ' sel' : '');
    const k = document.createElement('span');
    k.className = 'sl-key';
    k.textContent = it.key;
    const l = document.createElement('span');
    l.textContent = it.label;
    const d = document.createElement('span');
    d.className = 'sl-desc';
    d.textContent = it.desc;
    row.append(k, l, d);
    row.onmousedown = (e) => {
      e.preventDefault();
      runSlash(i);
    };
    slashMenu.appendChild(row);
  });
  slashMenu.classList.add('show');
}

function positionSlash(ta) {
  const sel = window.getSelection();
  const base = ta.getBoundingClientRect();
  let x = base.left;
  let y = base.top;
  if (sel.rangeCount) {
    const r = sel.getRangeAt(0).getBoundingClientRect();
    if (r.left || r.bottom) {
      x = r.left;
      y = r.bottom;
    }
  }
  slashMenu.style.left = `${Math.min(x, window.innerWidth - 230)}px`;
  slashMenu.style.top = `${y + 6}px`;
}

function hideSlash() {
  slashMenu.classList.remove('show');
  slashState = null;
}

function moveSlash(dir) {
  const st = slashState;
  if (!st) return;
  st.active = (st.active + dir + st.filtered.length) % st.filtered.length;
  [...slashMenu.children].forEach((row, i) => row.classList.toggle('sel', i === st.active));
}

/// Called from a block's input handler: manages the slash menu lifecycle.
/// Returns true iff the menu is active (owns the keystroke).
function updateSlash(ta) {
  const text = extractText(ta);
  const caret = caretNow(ta);
  // Trigger: "/" typed at the very start of an otherwise-empty-ish block.
  if (!slashState) {
    if (text === '/' && caret === 1) {
      slashState = { ta, slashAt: 0, filtered: [], active: 0 };
      positionSlash(ta);
      renderSlash('');
      return true;
    }
    return false;
  }
  if (slashState.ta !== ta) {
    hideSlash();
    return false;
  }
  // Query is everything after the slash up to the caret.
  if (caret < slashState.slashAt + 1 || !text.startsWith('/', slashState.slashAt)) {
    hideSlash();
    return false;
  }
  const query = text.slice(slashState.slashAt + 1, caret);
  if (query.includes(' ') || query.includes('\n')) {
    hideSlash();
    return false;
  }
  positionSlash(ta);
  renderSlash(query);
  return !!slashState;
}

function runSlash(i) {
  const st = slashState;
  if (!st) return;
  const it = st.filtered[i];
  if (!it) return;
  const ta = st.ta;
  const obj = ta.dataset.blockObj;
  const cur = extractText(ta);
  if (st.slashAt != null && cur.length >= st.slashAt) {
    const caret = caretNow(ta);
    if (caret > st.slashAt) {
      web.textRemove(obj, st.slashAt, caret - st.slashAt);
      rerenderBlock(ta, obj, st.slashAt);
    }
  }
  const rect = slashMenu.getBoundingClientRect();
  hideSlash();
  it.run(ta, rect);
}


// ---- responsive panel toggles -------------------------------------------------

// Narrow widths: the buttons open slide-in overlays. Wide widths: they
// collapse the panels in-flow, remembered across sessions.
const navIsOverlay = () => matchMedia('(max-width: 900px)').matches;
const commentsIsOverlay = () => matchMedia('(max-width: 1150px)').matches;

const PANEL_PREFS_KEY = 'hashweb-kb-panels';

function savePanelPrefs() {
  localStorage.setItem(
    PANEL_PREFS_KEY,
    JSON.stringify({
      nav: document.body.classList.contains('collapse-nav'),
      comments: document.body.classList.contains('collapse-comments'),
    }),
  );
}
try {
  const prefs = JSON.parse(localStorage.getItem(PANEL_PREFS_KEY) ?? '{}');
  document.body.classList.toggle('collapse-nav', !!prefs.nav);
  document.body.classList.toggle('collapse-comments', !!prefs.comments);
} catch (_) {
  /* fresh */
}

document.getElementById('nav-toggle').onclick = () => {
  if (navIsOverlay()) {
    document.body.classList.toggle('show-nav');
    document.body.classList.remove('show-comments');
  } else {
    document.body.classList.toggle('collapse-nav');
    savePanelPrefs();
  }
};
document.getElementById('comments-toggle').onclick = () => {
  if (commentsIsOverlay()) {
    document.body.classList.toggle('show-comments');
    document.body.classList.remove('show-nav');
  } else {
    document.body.classList.toggle('collapse-comments');
    savePanelPrefs();
    renderComments();
  }
};
document.getElementById('panel-backdrop').onclick = () => {
  document.body.classList.remove('show-nav', 'show-comments');
};

// ---- visual themes ------------------------------------------------------------

// Themes are CSS-variable sets plus structural overrides on body[data-theme],
// cycled with the header button and remembered per-browser. Device state, not
// document state — deliberately never synced.
const THEMES = ['gallery', 'editorial', 'poster', 'zen', 'terminal', 'nord', 'fable'];
const THEME_KEY = 'hashweb-kb-theme';
const themeToggleEl = document.getElementById('theme-toggle');

function applyTheme(name) {
  document.body.dataset.theme = name;
  themeToggleEl.textContent = '◧ ' + name.toUpperCase();
}

try {
  const saved = localStorage.getItem(THEME_KEY);
  applyTheme(THEMES.includes(saved) ? saved : 'gallery');
} catch (_) {
  applyTheme('gallery');
}

themeToggleEl.onclick = () => {
  const cur = document.body.dataset.theme ?? 'gallery';
  const next = THEMES[(THEMES.indexOf(cur) + 1) % THEMES.length];
  applyTheme(next);
  try {
    localStorage.setItem(THEME_KEY, next);
  } catch (_) {
    /* cache off */
  }
  toast('THEME: ' + next.toUpperCase());
};

// ---- go ----------------------------------------------------------------------

render();
persistNow(false);
console.log('[kb] workspace object:', WS);

// Dev handle: inspect the store from the console.
window.__kb = {
  web,
  WS,
  spans: (obj) => JSON.parse(web.markedSpans(obj)),
  text: (obj) => web.text(obj),
  blocks: () => allLeaves(currentBodyOrigin).map((b) => b.obj),
  persist: () => persistNow(true),
  render,
};
