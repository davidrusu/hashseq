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
const toolsEl = document.getElementById('page-tools');
const noPageEl = document.getElementById('no-page');
const statObjects = document.getElementById('stat-objects');
const statBytes = document.getElementById('stat-bytes');
const statParked = document.getElementById('stat-parked');

let current = null; // pageObj hex
let currentBody = null; // body seq obj hex
let prevBodyText = ''; // shadow of the textarea for diffing

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
  bodyEl.style.display = has ? '' : 'none';
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
  prevBodyText = text;
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
