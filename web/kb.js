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

const STORAGE_KEY = 'hashweb-kb-snapshot';

function hex(bytes) {
  return [...bytes].map((b) => b.toString(16).padStart(2, '0')).join('');
}

function randOrigin() {
  const b = new Uint8Array(32);
  crypto.getRandomValues(b);
  return hex(b);
}

function randTag() {
  const b = new Uint8Array(4);
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

function pageKeysOf(obj) {
  return JSON.parse(web.keys(obj))
    .filter((k) => k.startsWith('page:'))
    .sort();
}

// ---- page graph (rebuilt every render; the store is the only state) --------

// pageMeta: pageObj -> { parentObj, key, title, conflict, subpages: [pageObj] }
let pageMeta = new Map();
let rootPages = [];

function openPagesUnder(parentObj, visited) {
  const out = [];
  for (const key of pageKeysOf(parentObj)) {
    for (const origin of refsOf(parentObj, key)) {
      const pageObj = web.createKv(origin); // open-on-discovery: app-level birth
      if (visited.has(pageObj)) continue; // transclusion/cycle guard
      visited.add(pageObj);
      const titles = stringsOf(pageObj, 'title');
      pageMeta.set(pageObj, {
        parentObj,
        key,
        title: titles[0] ?? 'Untitled',
        conflict: titles.length > 1 ? titles : null,
        subpages: openPagesUnder(pageObj, visited),
      });
      out.push(pageObj);
    }
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
const bodyEl = document.getElementById('body');
const previewEl = document.getElementById('preview');
const tabEditEl = document.getElementById('tab-edit');
const tabViewEl = document.getElementById('tab-view');
const toolsEl = document.getElementById('page-tools');
const noPageEl = document.getElementById('no-page');
const statObjects = document.getElementById('stat-objects');
const statBytes = document.getElementById('stat-bytes');
const statParked = document.getElementById('stat-parked');

let current = null; // pageObj hex
let currentBody = null; // body seq obj hex
let prevBodyText = ''; // shadow of the textarea for diffing
let viewMode = 'edit'; // 'edit' | 'view'

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
        f.comments.push({ kind: m.kind, text: m.values[0] ?? '' });
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
      hl.title = first.comments.map((x) => x.text).join('\n');
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

/// An embedded object at body position `idx` — by app convention a table:
/// a seq of row refs, each row a seq of cell refs, each cell a text seq.
function renderEmbed(idx) {
  const origin = web.payloadAt(currentBody, idx);
  if (!origin) return document.createTextNode(ATOM);
  return objTableNode(web.createSeq(origin));
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
      td.addEventListener('input', () => {
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
  tabViewEl.classList.toggle('active', mode === 'view');
  renderEditor();
}

tabEditEl.onclick = () => setViewMode('edit');
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
  const emit = (pages, depth) => {
    for (const p of pages) {
      const meta = pageMeta.get(p);
      const row = document.createElement('div');
      row.className = 'page-row' + (p === current ? ' active' : '');
      row.style.paddingLeft = `${12 + depth * 16}px`;
      const twirl = document.createElement('span');
      twirl.className = 'twirl';
      twirl.textContent = meta.subpages.length > 0 ? '▸' : '·';
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
  bodyEl.style.display = has ? '' : 'none';
  previewEl.style.display = 'none';
  toolsEl.style.display = has ? '' : 'none';
  crumbsEl.style.display = has ? '' : 'none';
  noPageEl.style.display = has ? 'none' : '';
  if (!has) return;

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
    const s = document.createElement('span');
    s.textContent = pageMeta.get(p).title;
    if (p !== current) {
      s.className = 'link';
      s.onclick = () => {
        current = p;
        render();
      };
    }
    crumbsEl.appendChild(s);
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

  // Body
  currentBody = bodyOf(current);
  const text = currentBody ? web.text(currentBody) : '';
  const editing = viewMode === 'edit';
  bodyEl.style.display = editing ? '' : 'none';
  previewEl.style.display = editing ? 'none' : '';
  if (editing) {
    if (bodyEl.value !== text) {
      const focused = document.activeElement === bodyEl;
      const selStart = bodyEl.selectionStart;
      const selEnd = bodyEl.selectionEnd;
      bodyEl.value = text;
      if (focused) {
        // Best-effort caret preservation under remote edits.
        bodyEl.selectionStart = Math.min(selStart, text.length);
        bodyEl.selectionEnd = Math.min(selEnd, text.length);
      }
    }
  } else {
    renderBody(previewEl, currentBody);
  }
  renderComments();
  prevBodyText = text;
}

function collectComments() {
  if (!currentBody) return [];
  const byKind = new Map();
  let pos = 0;
  for (const s of styledSpans(currentBody)) {
    for (const c of s.text) {
      for (const cm of s.comments) {
        let e = byKind.get(cm.kind);
        if (!e) {
          e = { kind: cm.kind, text: cm.text, quote: '', start: pos, end: pos + 1 };
          byKind.set(cm.kind, e);
        }
        e.quote += c;
        e.end = pos + 1;
      }
      pos++;
    }
  }
  return [...byKind.values()];
}

function renderComments() {
  const panel = document.getElementById('comments');
  const comments = viewMode === 'view' && current ? collectComments() : [];
  if (comments.length === 0) {
    panel.style.display = 'none';
    return;
  }
  panel.style.display = 'block';
  panel.innerHTML = '';
  const head = document.createElement('div');
  head.className = 'c-head';
  head.textContent = `COMMENTS (${comments.length})`;
  panel.appendChild(head);
  for (const cm of comments) {
    const row = document.createElement('div');
    row.className = 'c-row';
    const quote = document.createElement('span');
    quote.className = 'c-quote';
    quote.textContent = cm.quote;
    const text = document.createElement('span');
    text.className = 'c-text';
    text.textContent = cm.text;
    const del = document.createElement('span');
    del.className = 'c-del';
    del.textContent = '✕';
    del.title = 'resolve (tombstone the comment mark)';
    del.onclick = () => {
      web.unmarkRange(currentBody, cm.start, cm.end, cm.kind);
      persistSoon();
      render();
    };
    row.append(quote, text, del);
    panel.appendChild(row);
  }
}

// ---- editing ----------------------------------------------------------------

function ensureBody() {
  if (currentBody) return currentBody;
  const origin = randOrigin();
  web.putRef(current, 'body', origin);
  currentBody = web.createSeq(origin);
  return currentBody;
}

bodyEl.addEventListener('input', () => {
  if (!current) return;
  const body = ensureBody();
  const next = bodyEl.value;
  const prev = prevBodyText;
  // Single-span diff: common prefix + common suffix bound the change.
  let start = 0;
  const maxStart = Math.min(prev.length, next.length);
  while (start < maxStart && prev[start] === next[start]) start++;
  let endPrev = prev.length;
  let endNext = next.length;
  while (endPrev > start && endNext > start && prev[endPrev - 1] === next[endNext - 1]) {
    endPrev--;
    endNext--;
  }
  if (endPrev > start) web.textRemove(body, start, endPrev - start);
  if (endNext > start) web.textInsert(body, start, next.slice(start, endNext));
  prevBodyText = next;
  persistSoon();
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
  web.putRef(parentObj, `page:${randTag()}`, origin);
  const page = web.createKv(origin);
  web.putString(page, 'title', 'Untitled');
  const bodyOrigin = randOrigin();
  web.putRef(page, 'body', bodyOrigin);
  web.createSeq(bodyOrigin);
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
  if (!confirm(`Delete "${meta.title}"? (Tombstones the slot; ops remain in the DAG.)`)) return;
  web.del(meta.parentObj, meta.key);
  current = null;
  persistSoon();
  render();
};

// ---- go ----------------------------------------------------------------------

render();
persistNow(false);
console.log('[kb] workspace object:', WS);

// ---- format toolbar (marks over the textarea selection) ----------------------

function selectionLines(text, selStart, selEnd) {
  const start = text.lastIndexOf('\n', Math.max(0, selStart - 1)) + 1;
  const nl = text.indexOf('\n', selEnd);
  return [start, nl === -1 ? text.length : nl];
}

document.getElementById('insert-table').onclick = () => {
  if (!current || viewMode !== 'edit') return;
  const body = ensureBody();
  const at = Math.min(bodyEl.selectionStart ?? bodyEl.value.length, bodyEl.value.length);
  // A table is a seq of row refs; a row is a seq of cell refs; a cell is a
  // text seq — a hashseq of hashseqs, embedded in the body as a link atom.
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
  web.seqInsertRef(body, at, tableOrigin);
  persistSoon();
  setViewMode('view'); // tables live in the rendered face
};

for (const btn of document.querySelectorAll('#fmt-tools button')) {
  if (!btn.dataset.mark) continue;
  btn.onclick = () => {
    if (!current || viewMode !== 'edit') return;
    const body = ensureBody();
    let [a, b] = [bodyEl.selectionStart, bodyEl.selectionEnd];
    const kind = btn.dataset.mark;
    if (kind === 'codeblock' || kind === 'eqblock') {
      [a, b] = selectionLines(bodyEl.value, a, b);
    }
    if (a >= b) return; // formatting needs a selection
    if (kind === 'comment') {
      const text = prompt('Comment:');
      if (!text) return;
      // One kind per comment: overlapping comments coexist (the same-kind
      // overwrite hygiene is a formatting policy, not an annotation one).
      web.markRange(body, a, b, `comment:${randTag()}`, text);
    } else if (kind === 'clear') {
      for (const k of MARK_KINDS) web.unmarkRange(body, a, b, k);
    } else if (kind === 'codeblock') {
      const lang = prompt('language label (optional):', '') ?? '';
      web.markRange(body, a, b, 'codeblock', lang);
    } else {
      web.markRange(body, a, b, kind, 'on');
    }
    persistSoon();
    bodyEl.focus();
    bodyEl.setSelectionRange(a, b);
  };
}