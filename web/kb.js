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
  if (broadcast) channel.postMessage(bytes);
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
  if (mine.length !== theirs.length || !mine.every((b, i) => b === theirs[i])) {
    channel.postMessage(mine);
  }
  render();
};

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
    if (viewMode === 'view') renderEditor();
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

/// An embedded object at position `idx`. The payload is a raw id — the
/// app's conventions decide its face: a known page's OBJECT id is a link
/// (pure name, navigates); a table's ORIGIN renders the table inline; an
/// id we can't classify renders as an inert chip (never auto-opened —
/// opening is a write).
function renderEmbed(idx) {
  const payload = web.payloadAt(renderTargetObj, idx);
  if (!payload) return document.createTextNode(ATOM);
  if (pageMeta.has(payload) || web.isKv(payload)) return pageLinkNode(payload);
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

function extractText(node) {
  let out = '';
  for (const child of node.childNodes) {
    if (child.nodeType === Node.TEXT_NODE) out += child.data;
    else if (child.nodeType !== Node.ELEMENT_NODE) continue;
    else if (child.dataset && child.dataset.text != null) out += child.dataset.text;
    else if (child.tagName === 'BR') out += '\n';
    else out += extractText(child);
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

/// Locate text offset `target` as a (node, offset) DOM point.
function locateOffset(blockEl, target) {
  let remaining = target;
  const visit = (node) => {
    for (const child of node.childNodes) {
      if (child.nodeType === Node.TEXT_NODE) {
        if (remaining <= child.data.length) return { node: child, offset: remaining };
        remaining -= child.data.length;
      } else if (child.nodeType === Node.ELEMENT_NODE) {
        if (child.dataset && child.dataset.text != null) {
          const len = child.dataset.text.length;
          if (remaining < len) return { node: child.parentNode, after: child };
          remaining -= len;
        } else if (child.tagName === 'BR') {
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

function setSelectionRangeIn(blockEl, a, b) {
  const r = document.createRange();
  const place = (target, setter) => {
    const hit = locateOffset(blockEl, target);
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

/// Double-click a math/equation widget to edit its source: replace the
/// region's chars in place — the mark's regional points survive, so the
/// new source stays inside the math region.
function widgetSourceEditor(w, blockObj, ed, label) {
  w.ondblclick = () => {
    const src = prompt(`${label}:`, w.dataset.text);
    if (src == null || src === w.dataset.text) return;
    const off = offsetOfPoint(ed, ...rangePointBefore(w));
    web.textRemove(blockObj, off, [...w.dataset.text].length);
    if (src.length > 0) web.textInsert(blockObj, off, src);
    persistSoon();
    rerenderBlock(ed, blockObj, off + [...src].length);
    if (viewMode === 'split') renderPreview();
  };
}

function rangePointBefore(node) {
  return [node.parentNode, [...node.parentNode.childNodes].indexOf(node)];
}

/// Render a block's content as editable, formatted DOM.
function renderEditableInto(ed, blockObj) {
  ed.innerHTML = '';
  renderTargetObj = blockObj;
  const spans = styledSpans(blockObj);
  let i = 0;
  let pos = 0;
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
      let tex = '';
      while (i < spans.length && spans[i].eqblock) {
        tex += spans[i].text;
        pos += [...spans[i].text].length;
        i++;
      }
      const w = makeWidget(mathNode(tex.trim(), true), tex, 'w-eq');
      w.title = 'double-click to edit the equation source';
      widgetSourceEditor(w, blockObj, ed, 'Equation (TeX)');
      ed.appendChild(w);
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
      emitEditableChunks(ed, chars, blockObj);
    }
  }
}

function emitEditableChunks(ed, chars, blockObj) {
  let k = 0;
  while (k < chars.length) {
    const first = chars[k];
    if (first.c === ATOM) {
      const inner = renderEmbed(first.idx);
      const isLink = inner.classList?.contains('page-link');
      const w = makeWidget(inner, ATOM, isLink ? 'w-link' : 'w-embed');
      ed.appendChild(w);
      k++;
      continue;
    }
    if (first.math) {
      let src = '';
      while (k < chars.length && chars[k].math && chars[k].c !== ATOM) {
        src += chars[k].c;
        k++;
      }
      const w = makeWidget(mathNode(src, false), src, 'w-math');
      w.title = 'double-click to edit the math source';
      widgetSourceEditor(w, blockObj, ed, 'Math (TeX)');
      ed.appendChild(w);
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

function rerenderBlock(ed, blockObj, caretOff) {
  renderEditableInto(ed, blockObj);
  ed.dataset.prev = extractText(ed);
  if (caretOff != null) {
    ed.focus();
    setSelectionRangeIn(ed, Math.min(caretOff, ed.dataset.prev.length));
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
    ta.addEventListener('beforeinput', (e) => {
      if (e.inputType === 'insertParagraph' || e.inputType === 'insertLineBreak') {
        // Keep the DOM plain: newlines are text, never <div>/<br> soup.
        e.preventDefault();
        document.execCommand('insertText', false, '\n');
      }
    });
    ta.addEventListener('paste', (e) => {
      e.preventDefault();
      document.execCommand('insertText', false, e.clipboardData.getData('text/plain'));
    });
    ta.addEventListener('input', (e) => {
      if (e.isComposing) return;
      // Table cells inside embed widgets stopPropagation; anything else
      // reaching here edits the block's own text.
      const next = extractText(ta);
      applyDiff(b.obj, ta.dataset.prev, next);
      ta.dataset.prev = next;
      if (viewMode === 'split') {
        renderPreview();
        renderComments();
      }
      persistSoon();
      // Reconcile styling drift (typing at mark edges) once typing pauses.
      clearTimeout(normalizeTimer);
      normalizeTimer = setTimeout(() => {
        if (document.activeElement === ta || ta.contains(document.activeElement)) {
          rerenderBlock(ta, b.obj, caretOffsetIn(ta));
        }
      }, 900);
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

document.getElementById('delete-page').onclick = () => {
  if (!current) return;
  const meta = pageMeta.get(current);
  if (!confirm(`Delete "${meta.title}"? (Tombstones its tree entry; ops remain in the DAG.)`)) return;
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
      const lang = prompt('language label (optional):', '') ?? '';
      web.markRange(blockObj, a, b, 'codeblock', lang);
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
