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

function childrenOf(parentObj) {
  const listObj = childrenListOf(parentObj);
  const out = [];
  const n = web.textLen(listObj);
  for (let i = 0; i < n; i++) {
    const origin = web.payloadAt(listObj, i);
    if (origin) out.push({ idx: i, origin, listObj });
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
      title: titles[0] ?? 'Untitled',
      conflict: titles.length > 1 ? titles : null,
      subpages: openPagesUnder(pageObj, visited),
    });
    out.push(pageObj);
  }
  return out;
}

function rebuildGraph() {
  pageMeta = new Map();
  rootPages = openPagesUnder(WS, new Set());
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

function snapshotBytes() {
  return web.encode();
}

function persistNow(broadcast) {
  const bytes = snapshotBytes();
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  }
  localStorage.setItem(STORAGE_KEY, btoa(bin));
  if (broadcast) {
    channel.postMessage(bytes);
    if (wsReady) ws.send(bytes);
  }
  statObjects.textContent = web.objectCount();
  statBytes.textContent = fmtBytes(bytes.length);
  statParked.textContent = web.orphanCount();
  return bytes;
}

function persistSoon() {
  clearTimeout(persistTimer);
  persistTimer = setTimeout(() => persistNow(true), 200);
}

channel.onmessage = (e) => {
  const theirs = new Uint8Array(e.data);
  web.mergeEncoded(theirs);
  const mine = persistNow(false);
  // Canonical bytes: equal op sets ⟺ identical snapshots. Re-broadcast only
  // while we know something they don't; equality ends the exchange.
  if (!bytesEqual(mine, theirs)) {
    channel.postMessage(mine);
  }
  render();
};

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
    sock.send(snapshotBytes()); // offer what we know
  };
  sock.onmessage = (e) => {
    const theirs = new Uint8Array(e.data);
    web.mergeEncoded(theirs);
    const mine = persistNow(false);
    if (!bytesEqual(mine, theirs)) sock.send(mine);
    render();
  };
  sock.onclose = () => {
    wsReady = false;
    setSyncStatus(false);
    setTimeout(connectSync, wsRetry);
    wsRetry = Math.min(wsRetry * 2, 15000);
  };
  sock.onerror = () => sock.close();
}

if (location.protocol.startsWith('http')) connectSync();

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
const previewEl = document.getElementById('preview');
const tabEditEl = document.getElementById('tab-edit');
const tabSplitEl = document.getElementById('tab-split');
const tabViewEl = document.getElementById('tab-view');
const toolsEl = document.getElementById('page-tools');
const noPageEl = document.getElementById('no-page');
const statObjects = document.getElementById('stat-objects');
const statBytes = document.getElementById('stat-bytes');
const statParked = document.getElementById('stat-parked');

let current = null; // pageObj hex
let currentBody = null; // body seq obj hex (a seq of block refs)
let viewMode = 'edit'; // 'edit' | 'split' | 'view'
let renderTargetObj = null; // the seq renderBody is currently rendering
let focusedBlockObj = null; // last-focused block (toolbar target)
let dragFrom = null; // block index a drag started from
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
    // Gentle arrival: never rebuild rows underneath an in-flight edit —
    // a rebuild mid-interaction drops focus and eats keystrokes.
    if (document.activeElement?.closest?.('.block-ed, [contenteditable]')) {
      window.addEventListener('focusout', () => render(), { once: true });
    } else {
      render();
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
  const tableObj = WasmHashWeb.seqId(payload);
  if (web.isSeq(tableObj)) return objTableNode(tableObj);
  const chip = document.createElement('span');
  chip.className = 'page-link broken';
  chip.textContent = `⟨${payload.slice(0, 8)}…⟩`;
  chip.title = 'unknown object reference';
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

function setViewMode(mode) {
  viewMode = mode;
  tabEditEl.classList.toggle('active', mode === 'edit');
  tabSplitEl.classList.toggle('active', mode === 'split');
  tabViewEl.classList.toggle('active', mode === 'view');
  renderEditor();
}

tabEditEl.onclick = () => setViewMode('edit');
tabSplitEl.onclick = () => setViewMode('split');
tabViewEl.onclick = () => setViewMode('view');

// ---- rendering ---------------------------------------------------------------

function render() {
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
      row.onclick = () => {
        current = p;
        render();
      };

      row.draggable = true;
      row.ondragstart = (e) => {
        treeDrag = { pageObj: p, listObj: meta.listObj, idx: meta.idx };
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
          // Become a subpage: append to the drop target's children list
          // (derived origin — it exists the moment we name it).
          const target = childrenListOf(p);
          const origin = web.payloadAt(src.listObj, src.idx);
          if (src.listObj === target) {
            web.seqMove(src.listObj, src.idx, web.textLen(target)); // already a child: move to end
          } else {
            web.textRemove(src.listObj, src.idx, 1);
            web.seqInsertRef(target, web.textLen(target), origin);
          }
        } else {
          const slot = meta.idx + (zone === 'above' ? 0 : 1);
          if (src.listObj === meta.listObj) {
            if (slot === src.idx || slot === src.idx + 1) return;
            web.seqMove(src.listObj, src.idx, slot); // same list: ONE Move op
          } else {
            // Reparent: a Move cannot cross containers (same-container
            // rule), so this is remove + insert — a new atom, new identity.
            const origin = web.payloadAt(src.listObj, src.idx);
            web.textRemove(src.listObj, src.idx, 1);
            web.seqInsertRef(meta.listObj, slot, origin);
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
  titleEl.style.display = has ? '' : 'none';
  document.getElementById('fmt-row').style.display = has ? '' : 'none';
  blocksEl.style.display = has ? '' : 'none';
  previewEl.style.display = 'none';
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

  // Body: a seq of block refs. Each block is its own text seq.
  currentBody = bodyOf(current);
  const editing = viewMode !== 'view';
  const showPreview = viewMode !== 'edit';
  blocksEl.style.display = editing ? '' : 'none';
  previewEl.style.display = showPreview ? '' : 'none';
  if (editing) renderBlocks();
  if (showPreview) renderPreview();
  renderComments();
}

// ---- blocks ------------------------------------------------------------------

function blocksOf(bodyObj) {
  const out = [];
  const n = web.textLen(bodyObj);
  for (let i = 0; i < n; i++) {
    const origin = web.payloadAt(bodyObj, i);
    if (origin) out.push({ idx: i, obj: web.createSeq(origin) });
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
  if (viewMode === 'split') renderPreview();
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
  if (!exposedRegion) return;
  const sel = window.getSelection();
  if (!sel.rangeCount) return;
  let node = sel.getRangeAt(0).startContainer;
  if (node.nodeType === Node.TEXT_NODE) node = node.parentNode;
  if (!node.closest?.('.region-live')) collapseExposedRegion();
});

/// Render a block's content as editable, formatted DOM.
function renderEditableInto(ed, blockObj) {
  ed.innerHTML = '';
  renderTargetObj = blockObj;
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
  for (const r of blocksEl.querySelectorAll('.block-row')) {
    r.classList.remove('drop-above', 'drop-below');
  }
}

function renderBlocks() {
  const blocks = blocksOf(currentBody);
  const activeEd = document.activeElement?.closest?.('.block-ed') ?? null;
  const keepObj = activeEd?.dataset?.blockObj ?? null;
  const keepCaret = activeEd ? caretOffsetIn(activeEd) : null;
  blocksEl.innerHTML = '';

  blocks.forEach((b, i) => {
    const row = document.createElement('div');
    row.className = 'block-row';

    const handle = document.createElement('span');
    handle.className = 'handle';
    handle.textContent = '⠿';
    handle.title = 'drag to reorder (a Move op)';
    handle.draggable = true;
    handle.ondragstart = (e) => {
      dragFrom = i;
      row.classList.add('dragging');
      e.dataTransfer.effectAllowed = 'move';
      e.dataTransfer.setData('text/plain', String(i));
    };
    handle.ondragend = () => {
      row.classList.remove('dragging');
      clearDropMarks();
    };

    row.ondragover = (e) => {
      e.preventDefault();
      const rect = row.getBoundingClientRect();
      const above = e.clientY < rect.top + rect.height / 2;
      clearDropMarks();
      row.classList.add(above ? 'drop-above' : 'drop-below');
    };
    row.ondragleave = () => row.classList.remove('drop-above', 'drop-below');
    row.ondrop = (e) => {
      e.preventDefault();
      clearDropMarks();
      if (dragFrom === null) return;
      const rect = row.getBoundingClientRect();
      const above = e.clientY < rect.top + rect.height / 2;
      const slot = above ? i : i + 1;
      const from = dragFrom;
      dragFrom = null;
      if (slot === from || slot === from + 1) return; // no movement
      web.seqMove(currentBody, from, slot); // drag-reorder IS a Move op
      persistSoon();
      render();
    };

    const ta = document.createElement('div');
    ta.className = 'block-ed';
    ta.contentEditable = 'true';
    ta.spellcheck = false;
    if (i === 0 && blocks.length === 1) ta.dataset.placeholder = 'Type here…';
    renderEditableInto(ta, b.obj);
    ta.dataset.prev = extractText(ta);
    ta.dataset.blockObj = b.obj;
    // Insert a literal newline text node at the caret (Chrome's own
    // insertParagraph/insertText('\n') wraps <div>s, which the extractor
    // would miscount), then commit the edit pipeline manually — no input
    // event fires for programmatic DOM changes.
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
      const did = applyDiffAt(b.obj, ta.dataset.prev, next, caretOffsetIn(ta));
      ta.dataset.prev = next;
      if (did?.kind === 'insert') reconcileMarkAffinity(ta, b.obj, did.p, did.n);
      if (viewMode === 'split') {
        renderPreview();
        renderComments();
      }
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
      if (e.key !== 'Enter') return;
      e.preventDefault();
      if (e.metaKey || e.ctrlKey) {
        // cmd+Enter: a newline within this block.
        insertNewlineAtCaret();
        return;
      }
      // Enter: a new block after this one. A plain tail after the caret
      // moves into the new block (a true split); tails carrying marks or
      // embeds stay put — their anchors live in this block's elements.
      const caret = caretOffsetIn(ta) ?? extractText(ta).length;
      const text = extractText(ta);
      const tail = text.slice(caret);
      const tailPlain =
        !tail.includes(ATOM) &&
        (() => {
          for (let q = caret; q < text.length; q++) {
            const f = flagsAt(b.obj, q);
            if (f && (f.code || f.math || f.codeblock !== null || f.eqblock || f.comments.length)) {
              return false;
            }
          }
          return true;
        })();
      const origin = randOrigin();
      const nb = web.createSeq(origin);
      if (tail && tailPlain) {
        web.textRemove(b.obj, caret, text.length - caret);
        web.textInsert(nb, 0, tail);
      }
      web.seqInsertRef(currentBody, i + 1, origin);
      persistSoon();
      render();
      for (const ed of blocksEl.querySelectorAll('.block-ed')) {
        if (ed.dataset.blockObj === nb) {
          ed.focus();
          setSelectionRangeIn(ed, 0);
          break;
        }
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
      // Table cells inside embed widgets stopPropagation; anything else
      // reaching here edits the block's own text.
      commitBlockEdit();
      const ruled = maybeInputRule(ta, b.obj, e.data);
      if (ruled != null) {
        clearTimeout(normalizeTimer);
        rerenderBlock(ta, b.obj, ruled, true); // caret outside the new span
      }
      // Reconcile styling drift (typing at mark edges) once typing pauses.
      clearTimeout(normalizeTimer);
      normalizeTimer = setTimeout(() => {
        if (document.activeElement === ta || ta.contains(document.activeElement)) {
          rerenderBlock(ta, b.obj, caretOffsetIn(ta));
        }
      }, 900);
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
          exposeRegion(ta, b.obj, w.dataset.kind, Number(w.dataset.ord), start + 1);
          return;
        }
        if (e.key === 'ArrowLeft' && caret === start + len) {
          e.preventDefault();
          exposeRegion(ta, b.obj, w.dataset.kind, Number(w.dataset.ord), start + len);
          return;
        }
      }
    });
    ta.addEventListener('drop', (e) => {
      // Text drops would splice DOM we don't control; block reorders are
      // handled by the row.
      if (dragFrom === null) e.preventDefault();
    });
    ta.onfocus = () => {
      focusedBlockObj = b.obj;
    };

    const del = document.createElement('span');
    del.className = 'b-del';
    del.textContent = '✕';
    del.title = 'remove block (tombstones the ref; the block object remains)';
    del.onclick = () => {
      web.textRemove(currentBody, i, 1);
      persistSoon();
      render();
    };

    row.append(handle, ta, del);
    blocksEl.appendChild(row);
  });

  const add = document.createElement('button');
  add.className = 'add-block';
  add.textContent = '+ BLOCK';
  add.onclick = () => addBlockAtEnd();
  blocksEl.appendChild(add);

  blocksEl.ondragover = (e) => e.preventDefault();
  blocksEl.ondrop = (e) => {
    if (dragFrom === null) return;
    // Dropping on the container (below the rows) moves to the end.
    if (e.target === blocksEl || e.target === add) {
      const from = dragFrom;
      dragFrom = null;
      web.seqMove(currentBody, from, blocksOf(currentBody).length);
      persistSoon();
      render();
    }
  };

  if (keepObj) {
    for (const ed of blocksEl.querySelectorAll('.block-ed')) {
      if (ed.dataset.blockObj === keepObj) {
        ed.focus();
        if (keepCaret != null) {
          setSelectionRangeIn(ed, Math.min(keepCaret, extractText(ed).length));
        }
      }
    }
  }
}

function addBlockAtEnd(focus = true) {
  const body = ensureBody();
  const origin = randOrigin();
  web.createSeq(origin);
  web.seqInsertRef(body, web.textLen(body), origin);
  persistSoon();
  render();
  if (focus) {
    const eds = blocksEl.querySelectorAll('.block-ed');
    eds[eds.length - 1]?.focus();
  }
}

function renderPreview() {
  previewEl.innerHTML = '';
  if (!currentBody) return;
  for (const b of blocksOf(currentBody)) {
    const div = document.createElement('div');
    div.className = 'block-view';
    renderBody(div, b.obj);
    previewEl.appendChild(div);
  }
}

// ---- comments ----------------------------------------------------------------

/// One comment = one identity (its tag — a 32-byte origin) realized as N
/// per-block mark fragments plus a discussion thread: the seq opened AT
/// the tag. Grouping across blocks by kind reassembles the composite.
function collectComments() {
  if (!currentBody) return [];
  const byKind = new Map();
  for (const b of blocksOf(currentBody)) {
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

let activeCommentTag = null;
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

function setCommentHighlight(kind, on) {
  for (const el of document.querySelectorAll('.comment-hl')) {
    const tags = (el.dataset.tags ?? '').split(',');
    if (tags.includes(kind)) el.classList.toggle('hl-active', on);
  }
}

function renderComments() {
  const panel = document.getElementById('comments');
  const comments = current ? collectComments() : [];
  if (comments.length === 0) {
    panel.style.display = 'none';
    activeCommentTag = null;
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

    // Hover or activate the card → light up every fragment it references.
    card.onmouseenter = () => setCommentHighlight(cm.kind, true);
    card.onmouseleave = () => {
      if (activeCommentTag !== cm.kind) setCommentHighlight(cm.kind, false);
    };
    card.onclick = () => {
      const was = activeCommentTag;
      activeCommentTag = was === cm.kind ? null : cm.kind;
      if (was) setCommentHighlight(was, false);
      if (activeCommentTag) setCommentHighlight(activeCommentTag, true);
      for (const c of panel.querySelectorAll('.cc-card')) c.classList.remove('active');
      if (activeCommentTag) card.classList.add('active');
    };
    panel.appendChild(card);
    if (pendingComposeTag === cm.kind) {
      pendingComposeTag = null;
      setTimeout(() => reply.focus(), 0);
    }
  }
  if (activeCommentTag) setCommentHighlight(activeCommentTag, true);
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

// Keep editor focus/selection through toolbar clicks.
document.getElementById('fmt-tools').addEventListener('mousedown', (e) => {
  if (e.target.tagName === 'BUTTON') e.preventDefault();
});

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

function createPage(parentObj) {
  const origin = randOrigin();
  const list = childrenListOf(parentObj);
  web.seqInsertRef(list, web.textLen(list), origin);
  const page = web.createKv(origin);
  web.putString(page, 'title', 'Untitled');
  const bodyOrigin = randOrigin();
  web.putRef(page, 'body', bodyOrigin);
  const body = web.createSeq(bodyOrigin);
  const blockOrigin = randOrigin();
  web.createSeq(blockOrigin);
  web.seqInsertRef(body, 0, blockOrigin);
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
  web.textRemove(meta.listObj, meta.idx, 1);
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

document.getElementById('insert-table').onclick = () => {
  if (!current || viewMode === 'view') return;
  const ta = focusedTA();
  if (!ta) return;
  const blockObj = ta.dataset.blockObj;
  const at = caretOffsetIn(ta) ?? extractText(ta).length;
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
  if (viewMode === 'split') renderPreview();
};

/// Downscale + recompress before storing: snapshots carry every artifact,
/// so keep images modest (WebP keeps alpha).
async function processImageFile(file) {
  const bmp = await createImageBitmap(file);
  const MAX = 1400;
  const scale = Math.min(1, MAX / Math.max(bmp.width, bmp.height));
  if (scale === 1 && file.size < 400_000) {
    return new Uint8Array(await file.arrayBuffer());
  }
  const canvas = document.createElement('canvas');
  canvas.width = Math.max(1, Math.round(bmp.width * scale));
  canvas.height = Math.max(1, Math.round(bmp.height * scale));
  canvas.getContext('2d').drawImage(bmp, 0, 0, canvas.width, canvas.height);
  const blob = await new Promise((res) => canvas.toBlob(res, 'image/webp', 0.85));
  return new Uint8Array(await blob.arrayBuffer());
}

async function insertImageFile(file, ta, at) {
  const bytes = await processImageFile(file);
  const imgId = web.provideBytes(bytes);
  const blockObj = ta.dataset.blockObj;
  web.seqInsertRef(blockObj, at, imgId);
  persistSoon();
  rerenderBlock(ta, blockObj, at + 1);
  if (viewMode === 'split') renderPreview();
}

const imageFileEl = document.getElementById('image-file');
document.getElementById('insert-image').onclick = () => {
  if (!current || viewMode === 'view') return;
  if (!focusedTA()) return;
  imageFileEl.click();
};
imageFileEl.onchange = async () => {
  const file = imageFileEl.files?.[0];
  imageFileEl.value = '';
  const ta = focusedTA();
  if (!file || !ta) return;
  const at = caretOffsetIn(ta) ?? extractText(ta).length;
  await insertImageFile(file, ta, at);
};

document.getElementById('insert-link').onclick = (e) => {
  if (!current || viewMode === 'view') return;
  const ta = focusedTA();
  if (!ta) return;
  const blockObj = ta.dataset.blockObj;
  const at = caretOffsetIn(ta) ?? extractText(ta).length;
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
        if (viewMode === 'split') renderPreview();
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
  const r = e.target.getBoundingClientRect();
  menu.style.left = `${r.left}px`;
  menu.style.top = `${r.bottom + 4}px`;
  document.body.appendChild(menu);
  setTimeout(() => {
    document.addEventListener('click', () => menu.remove(), { once: true });
  }, 0);
};

for (const btn of document.querySelectorAll('#fmt-tools button')) {
  if (!btn.dataset.mark) continue;
  btn.onclick = () => {
    if (!current || viewMode === 'view') return;
    if (btn.dataset.mark === 'comment') {
      // Composite: the selection may span blocks. One identity (a fresh
      // 32-byte tag), one mark fragment per touched block, and the
      // discussion thread is the seq opened AT the tag — no pointer.
      const frags = selectionFragments();
      if (frags.length === 0) return;
      const tag = randOrigin();
      web.createSeq(tag); // the thread starts empty — composed in the panel
      for (const f of frags) {
        web.markRange(f.obj, f.a, f.b, `comment:${tag}`, 'on');
      }
      activeCommentTag = `comment:${tag}`;
      pendingComposeTag = `comment:${tag}`;
      persistSoon();
      render();
      return;
    }
    const ta = focusedTA();
    if (!ta) return;
    const blockObj = ta.dataset.blockObj;
    const sel = selectionOffsetsIn(ta);
    if (!sel) return;
    let [a, b] = sel;
    const kind = btn.dataset.mark;
    if (kind === 'codeblock' || kind === 'eqblock') {
      [a, b] = selectionLines(extractText(ta), a, b);
    }
    if (a >= b) return; // formatting needs a selection
    if (kind === 'clear') {
      for (const k of MARK_KINDS) web.unmarkRange(blockObj, a, b, k);
    } else if (kind === 'codeblock') {
      web.markRange(blockObj, a, b, 'codeblock', '');
    } else {
      web.markRange(blockObj, a, b, kind, 'on');
    }
    persistSoon();
    rerenderBlock(ta, blockObj, null); // formatting appears in place
    setSelectionRangeIn(ta, a, b);
    ta.focus();
    if (viewMode === 'split') {
      renderPreview();
      renderComments();
    }
  };
}

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
  blocks: () => blocksOf(currentBody).map((b) => b.obj),
  persist: () => persistNow(true),
  render,
};
