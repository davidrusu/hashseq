import init, { WasmHashSeq, WasmRun } from './pkg/hashseq.js';

await init();

// ───────────────────────── palette ─────────────────────────
const C = {
  paper:'#EFE9DB', paper2:'#F6F1E5', ink:'#1B1816', muted:'#9a907b',
  red:'#B23A2E', green:'#2F5D43', amber:'#C8782D', edge:'rgba(27,24,22,0.28)',
};

// ───────────────────────── state ─────────────────────────
let seq = new WasmHashSeq();
let lastText = '';
const opLog = [];                 // {kind, hex?}
let mode = 'type';

// ───────── op model: every local mutation goes through the cursor/Run/op API ─────────
// (this is the emacs-FFI design mirrored in JS — and it produces the run coalescing
// you see in the tree)
function logOp(kind, bytes) {
  opLog.push({ kind, hex: bytes ? hex(bytes) : null });
  if (opLog.length > 400) opLog.shift();
}
function hex(u8) {
  let s = '';
  for (let i = 0; i < u8.length && i < 24; i++) s += u8[i].toString(16).padStart(2, '0');
  return s + (u8.length > 24 ? '…' : '');
}

function applyDelete(pos, n) {
  if (n <= 0) return;
  const op = seq.removeAndEncodeOp(pos, n);
  if (op) logOp('remove', op);
}

function applyInsert(pos, str) {
  if (!str.length) return;
  const chars = [...str];
  if (seq.isEmpty()) {            // empty seq → InsertRoot path (no single op exposed)
    seq.insert(pos, str);
    logOp('root', null);
    return;
  }
  const cur = seq.cursorAt(pos);  // After{anchor,deps} | Before{anchor,deps}
  if (!cur) { seq.insert(pos, str); logOp('root', null); return; }
  const run = cur.op === 'before'
    ? WasmRun.newBefore(cur.anchor, cur.extraDeps, chars[0])
    : WasmRun.newAfter(cur.anchor, cur.extraDeps, chars[0]);
  for (let i = 1; i < chars.length; i++) run.extend(chars[i]);
  const bytes = run.encodeOp();
  seq.applyRun(run);
  logOp(cur.op, bytes);
  run.free();
  cur.free();
}

// Fallback for programmatic edits / IME: position-based string diff. Ambiguous
// for characters typed adjacent to an identical character (it can't tell which
// side of a duplicate you meant) — interactive typing uses the caret instead.
function applyEdit(oldS, newS) {
  if (oldS === newS) return;
  const oldA = [...oldS], newA = [...newS];
  let p = 0;
  const minl = Math.min(oldA.length, newA.length);
  while (p < minl && oldA[p] === newA[p]) p++;
  let s = 0;
  while (s < minl - p && oldA[oldA.length - 1 - s] === newA[newA.length - 1 - s]) s++;
  const delLen = oldA.length - p - s;
  const ins = newA.slice(p, newA.length - s).join('');
  applyDelete(p, delLen);
  applyInsert(p, ins);
}

// code-point length of a UTF-16 substring offset (HashSeq positions are in
// unicode scalar values, textarea offsets are in UTF-16 code units)
const cpLen = (s) => [...s].length;

// Caret-precise edit: given the selection range in the OLD value (captured on
// `beforeinput`) and the NEW value, derive exactly what was replaced and with
// what — so a char typed next to an identical char anchors where you typed it.
function applyCaretEdit(oldV, newV, selStart, selEnd) {
  if (oldV === newV) return;
  const suffixUnits = oldV.length - selEnd;          // tail that stayed put
  const insEnd = newV.length - suffixUnits;
  // The selection model (replace [selStart,selEnd] with `inserted`) only holds
  // when the browser edited *inside* the selection: head and tail unchanged and
  // a non-negative insert span. Backspace / forward-delete / word-delete with a
  // collapsed caret edit *outside* the selection, which fails these checks — so
  // fall back to a plain prefix/suffix diff, which deletes correctly.
  const headOk = newV.slice(0, selStart) === oldV.slice(0, selStart);
  const tailOk = suffixUnits >= 0 && newV.slice(insEnd) === oldV.slice(selEnd);
  if (!headOk || !tailOk || insEnd < selStart) {
    applyEdit(oldV, newV);
    return;
  }
  const inserted = newV.slice(selStart, insEnd);
  const cpStart = cpLen(oldV.slice(0, selStart));    // → code-point positions
  const cpEnd = cpLen(oldV.slice(0, selEnd));
  applyDelete(cpStart, cpEnd - cpStart);             // remove the selected range
  applyInsert(cpStart, inserted);                    // insert exactly where the caret was
}

// ───────────────────────── DOM ─────────────────────────
const editor = document.getElementById('editor');
const canvas = document.getElementById('tree');
const ctx = canvas.getContext('2d');
const glCanvas = document.getElementById('tree-gl');
const $ = (id) => document.getElementById(id);

let proj = 'tess';                          // 'tess' (canvas2d tessellation) | 'shader' (webgl warp)

// ───────────────────────── tree layout ─────────────────────────
// layout.nodes holds drawable SEGMENTS {kind,text,removed,tip,startS,endS,w,h,depth,cx,cy}.
// A CRDT box whose interior elements anchor before-children is drawn as several
// segments with the children's subtrees laid out between them — the split is
// purely visual (the box is one node in the structure).
let layout = { nodes: [], edges: [], depEdges: [], bounds: null };
let showDeps = false;

function buildLayout(struct) {
  const byId = new Map();
  for (const n of struct.nodes) byId.set(n.id, { ...n, before: [], after: [], segs: [] });
  const tipSet = new Set(struct.tips);
  for (const n of byId.values()) n.tip = tipSet.has(n.id);

  const roots = [];
  for (const n of byId.values()) {
    if (n.parent && byId.has(n.parent)) {
      const par = byId.get(n.parent);
      (n.rel === 'before' ? par.before : par.after).push(n);
    } else {
      roots.push(n);
    }
  }
  const byHash = (a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0);
  roots.sort(byHash);
  for (const n of byId.values()) {
    // befores order by (attach offset, hash): interior attach points keep
    // document order; concurrent befores at the same anchor order by hash
    n.before.sort((a, b) => (a.parentOffset ?? 0) - (b.parentOffset ?? 0) || byHash(a, b));
    n.after.sort(byHash);
  }

  const charW = 6.6, H = 20, GAP_X = 10, PAD = 6;

  // in-order walk (lefts · node · rights) emitting segments, cumulative x,
  // depth = y. Each node's agenda interleaves its text segments with its
  // before-children by attach offset, then visits after-children (which always
  // anchor at the node's tail — after-forks still split runs structurally).
  // Recursion-free via an explicit frame stack so deep traces can't blow the
  // call stack.
  const agendaFor = (n, d) => {
    const chars = [...n.text];
    const mask = n.removedMask || '';          // '0'/'1' per char (tombstones)
    const items = [];
    let prev = 0;
    for (const c of n.before) {
      const off = Math.min(Math.max(c.parentOffset ?? 0, 0), chars.length);
      if (off > prev) { items.push({ seg: [prev, off] }); prev = off; }
      items.push({ child: c });
    }
    if (chars.length > prev) items.push({ seg: [prev, chars.length] });
    for (const c of n.after) items.push({ child: c });
    return { n, d, chars, mask, items, i: 0 };
  };

  let cursorX = 0;
  const nodes = [];                 // segments, in strip order
  const stack = [];
  for (let k = roots.length - 1; k >= 0; k--) stack.push(agendaFor(roots[k], 0));
  while (stack.length) {
    const f = stack[stack.length - 1];
    if (f.i >= f.items.length) { stack.pop(); continue; }
    const item = f.items[f.i++];
    if (item.seg) {
      const [a, b] = item.seg;
      const w = (b - a) * charW + 2 * PAD;
      const seg = {
        kind: f.n.kind, removed: f.n.removed, tip: f.n.tip,
        text: f.chars.slice(a, b).join(''), charStart: a,
        removedMask: f.mask.slice(a, b),       // per-char tombstone, aligned to text
        startS: cursorX, endS: cursorX + w, w, h: H, depth: f.d,
      };
      cursorX += w + GAP_X;
      nodes.push(seg);
      f.n.segs.push(seg);
    } else {
      stack.push(agendaFor(item.child, f.d + 1));
    }
  }

  // ─── spiral parameters ───
  //   in-order arc length  s  →  position along an Archimedean spiral r = b·θ
  //   depth                d  →  outward radial offset from that spiral path
  //   pitch P = maxDepth·D + H + gap — an arm's content spans radially from
  //   −H/2 (depth 0, bottom edge) to maxDepth·D + H/2 (deepest child, top edge),
  //   so the pitch must clear that whole band, not just the depth offsets.
  //   θ(s) = √(2s/b + θ₀²) parametrizes by arc length.
  //
  // Stored on layout so the renderer can evaluate spiralMap(s, d) on the fly —
  // that's what lets long runs tessellate into curved ribbons and individual
  // characters land along the tangent.
  let maxDepth = 0;
  for (const n of nodes) if (n.depth > maxDepth) maxDepth = n.depth;
  const D = 10;
  const ARM_GAP = 8;
  const P = maxDepth * D + H + ARM_GAP;
  const b = P / (2 * Math.PI);
  const theta0 = 2 * Math.PI;
  const halfThickDepth = H / (2 * D);                  // rect height in depth units
  const totalS = cursorX;                              // overall arc length of the strip
  // spiral runs inward: s = 0 (start of doc) maps to the outermost arm, s = totalS
  // maps to the center. Equivalent to substituting s → (totalS − s) in the formulas;
  // tangent reverses with the direction so rot becomes (α + π/2).
  const spiral = { D, P, b, theta0, halfThickDepth, totalS };

  const sm = (s, d) => {
    const sEff = totalS - s;
    const theta = Math.sqrt(2 * sEff / b + theta0 * theta0);
    const alpha = -theta;
    const r = b * theta + d * D;
    return { x: r * Math.cos(alpha), y: r * Math.sin(alpha), rot: alpha + Math.PI / 2 };
  };

  // Per-segment center position + readability flip decision (per segment — used
  // both for any single-glyph rendering and for all chars inside it so the text
  // reads in a consistent direction end-to-end).
  for (const n of nodes) {
    const c = sm((n.startS + n.endS) / 2, n.depth);
    n.cx = c.x; n.cy = c.y; n.rot = c.rot;
    const r2pi = ((c.rot % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI);
    n.flipText = (r2pi > Math.PI / 2 && r2pi < 3 * Math.PI / 2);
    // linear-projection coords: native (s, depth·D) with no rotation
    n.linCx = (n.startS + n.endS) / 2;
    n.linCy = n.depth * D;
  }

  // Edge endpoints are pseudo-points carrying both projections' coords.
  const point = (s, d) => {
    const p = sm(s, d);
    return { cx: p.x, cy: p.y, linCx: s, linCy: d * D };
  };
  // a node's visual midpoint spans all its segments
  const nodeMid = (n) => {
    const a = n.segs[0], z = n.segs[n.segs.length - 1];
    return point((a.startS + z.endS) / 2, a.depth);
  };
  // point on the parent strip where a child attaches: before → left edge of the
  // anchor char, after → its right edge
  const attachPoint = (par, n) => {
    const k = Math.max(0, Math.min(n.parentOffset ?? 0, [...par.text].length - 1));
    const sg = par.segs.find((s) => k < s.charStart + [...s.text].length)
      ?? par.segs[par.segs.length - 1];
    const s = sg.startS + PAD + (k - sg.charStart + (n.rel === 'after' ? 1 : 0)) * charW;
    return point(s, sg.depth);
  };
  // center of element `off` within a box — used by extra-dep edges that point
  // at a specific char (e.g. the run tip a mid-run insert depends on, which is
  // a different element of the same box than the insert's anchor).
  const charMid = (box, off) => {
    const k = Math.max(0, Math.min(off ?? 0, [...box.text].length - 1));
    const sg = box.segs.find((s) => k < s.charStart + [...s.text].length)
      ?? box.segs[box.segs.length - 1];
    const s = sg.startS + PAD + (k - sg.charStart + 0.5) * charW;
    return point(s, sg.depth);
  };

  const edges = [], depEdges = [], runEdges = [];
  for (const n of byId.values()) {
    if (!n.segs.length) continue;
    // run continuity: a run interrupted by an interior before-child is drawn as
    // several segments of the SAME node. Connect consecutive segments so the run
    // reads as continuous across the gap (e.g. "world" still follows "hello ").
    for (let i = 0; i + 1 < n.segs.length; i++) {
      const a = n.segs[i], b = n.segs[i + 1];
      runEdges.push({ from: point(a.endS, a.depth), to: point(b.startS, b.depth), kind: n.kind });
    }
    if (n.parent && byId.has(n.parent)) {
      const par = byId.get(n.parent);
      if (par.segs.length) edges.push({ from: attachPoint(par, n), to: nodeMid(n), rel: n.rel });
    }
    // extra_dependencies: causal context beyond the anchor. Each dep is
    // {box, off}, so the edge attaches to that exact character — e.g. a mid-run
    // insert depends on the run's tip, a different element of the anchor's box.
    // Skip only the anchor element itself (its own before/after edge shows it).
    for (const d of n.deps || []) {
      if (d.box === n.id) continue;
      if (d.box === n.parent && d.off === n.parentOffset) continue;
      if (!byId.has(d.box)) continue;
      const dep = byId.get(d.box);
      if (dep.segs.length) depEdges.push({ from: charMid(dep, d.off), to: nodeMid(n) });
    }
  }

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const n of nodes) {
    // sample the curved rect's four corners + midpoints for a safe AABB
    const ss = [n.startS, (n.startS + n.endS) / 2, n.endS];
    const dd = [n.depth - halfThickDepth, n.depth + halfThickDepth];
    for (const s of ss) for (const d of dd) {
      const p = sm(s, d);
      if (p.x < minX) minX = p.x; if (p.x > maxX) maxX = p.x;
      if (p.y < minY) minY = p.y; if (p.y > maxY) maxY = p.y;
    }
  }
  const bounds = nodes.length ? { minX, maxX, minY, maxY } : null;
  const boxH = 2 * halfThickDepth * D;
  const linearBounds = nodes.length ? {
    minX: 0, maxX: totalS,
    minY: -boxH / 2, maxY: maxDepth * D + boxH / 2,
  } : null;
  layout = { nodes, edges, depEdges, runEdges, bounds, linearBounds, spiral };
}

// ───────────────────────── view transform ─────────────────────────
let view = { scale: 1, fitScale: 1, tx: 0, ty: 0 };
let autoFit = true;

function fitView() {
  const b = proj === 'linear' ? layout.linearBounds : layout.bounds;
  const W = canvas.clientWidth, Hc = canvas.clientHeight;
  if (!b || W === 0) { view = { scale: 1, fitScale: 1, tx: W / 2, ty: 40 }; return; }
  const pad = 40;
  const bw = Math.max(1, b.maxX - b.minX), bh = Math.max(1, b.maxY - b.minY);
  const scale = Math.min((W - pad * 2) / bw, (Hc - pad * 2) / bh, 2.2);
  view.scale = scale;
  view.fitScale = scale;     // remember it so free-zoom can start here without snapping
  view.tx = (W - bw * scale) / 2 - b.minX * scale;
  view.ty = pad - b.minY * scale;
}

function resizeCanvas() {
  const dpr = window.devicePixelRatio || 1;
  const W = canvas.clientWidth, Hc = canvas.clientHeight;
  canvas.width = Math.round(W * dpr);
  canvas.height = Math.round(Hc * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  if (glCanvas) {
    glCanvas.width = canvas.width;
    glCanvas.height = canvas.height;
  }
}

// ───────────────────────── WebGL shader warp ─────────────────────────
// Pipeline:
//   1. Render the LINEAR strip (boxes + text, no rotation) into an offscreen 2D
//      canvas — that's the source texture.
//   2. A fullscreen quad runs a fragment shader that, for each screen pixel,
//      inverse-maps through the spiral: pixel → (r,α) → arm θ → (sEff, d) →
//      source UV (s/totalS, d·D/srcH). Pixels outside any arm or outside the
//      strip are discarded (transparent).
//   3. Edges keep being drawn by the 2D context on top, layered under the GL
//      output by virtue of WebGL's transparent discard regions.

const gl = glCanvas.getContext('webgl', { antialias: true, premultipliedAlpha: true })
        || glCanvas.getContext('experimental-webgl');

const srcCanvas = document.createElement('canvas');
const srcCtx = srcCanvas.getContext('2d');
let srcTexture = null;
let warpProg = null;
let warpUniforms = {};
let warpAttribs = {};
let quadBuf = null;
let lastWarpKey = '';                         // invalidation: rebuild source when layout changes

const VS = `
attribute vec2 a_pos;
void main() { gl_Position = vec4(a_pos, 0.0, 1.0); }
`;
const FS = `
precision highp float;
uniform vec2  uRes;
uniform vec2  uViewT;
uniform float uViewS;
uniform float uB;
uniform float uTheta0;
uniform float uTotalS;
uniform float uD;
uniform float uMaxDepth;
uniform float uHalfThickDepth;
uniform vec2  uSrcSize;
uniform float uSrcScale;
uniform sampler2D uSrc;
#define PI 3.14159265358979
void main() {
  vec2 frag = gl_FragCoord.xy;
  float sx = frag.x;
  float sy = uRes.y - frag.y;                 // flip to top-left origin (matches 2D)
  float wx = (sx - uViewT.x) / uViewS;        // pixel → world layout coords
  float wy = (sy - uViewT.y) / uViewS;
  float r = length(vec2(wx, wy));
  float a = atan(wy, wx);                     // (-π, π]
  float kF = (r / uB + a) / (2.0 * PI);
  float k  = floor(kF + 0.5);                 // arm index closest in radius
  float theta = -a + 2.0 * PI * k;
  if (theta < uTheta0) discard;
  float d = (r - uB * theta) / uD;            // continuous depth at this pixel
  if (d < -uHalfThickDepth || d > uMaxDepth + uHalfThickDepth) discard;
  float sEff = 0.5 * uB * (theta * theta - uTheta0 * uTheta0);
  float s = uTotalS - sEff;                   // spiral runs inward — same convention as tess
  if (s < 0.0 || s > uTotalS) discard;
  float srcXpx = s / uSrcScale;
  float srcYpx = (d + uHalfThickDepth) * uD / uSrcScale;
  if (srcYpx < 0.0 || srcYpx > uSrcSize.y) discard;
  vec2 uv = vec2(srcXpx / uSrcSize.x, srcYpx / uSrcSize.y);
  vec4 c = texture2D(uSrc, uv);
  if (c.a < 0.01) discard;
  gl_FragColor = c;
}
`;

function compileShader(type, src) {
  const sh = gl.createShader(type);
  gl.shaderSource(sh, src);
  gl.compileShader(sh);
  if (!gl.getShaderParameter(sh, gl.COMPILE_STATUS)) {
    console.error('shader compile:', gl.getShaderInfoLog(sh));
  }
  return sh;
}

function initGL() {
  if (!gl) return;
  warpProg = gl.createProgram();
  gl.attachShader(warpProg, compileShader(gl.VERTEX_SHADER, VS));
  gl.attachShader(warpProg, compileShader(gl.FRAGMENT_SHADER, FS));
  gl.linkProgram(warpProg);
  if (!gl.getProgramParameter(warpProg, gl.LINK_STATUS)) {
    console.error('link:', gl.getProgramInfoLog(warpProg));
    return;
  }
  warpAttribs.a_pos = gl.getAttribLocation(warpProg, 'a_pos');
  for (const u of ['uRes','uViewT','uViewS','uB','uTheta0','uTotalS','uD','uMaxDepth','uHalfThickDepth','uSrcSize','uSrcScale','uSrc']) {
    warpUniforms[u] = gl.getUniformLocation(warpProg, u);
  }
  quadBuf = gl.createBuffer();
  gl.bindBuffer(gl.ARRAY_BUFFER, quadBuf);
  gl.bufferData(gl.ARRAY_BUFFER, new Float32Array([-1,-1, 1,-1, -1,1, -1,1, 1,-1, 1,1]), gl.STATIC_DRAW);
  srcTexture = gl.createTexture();
  gl.bindTexture(gl.TEXTURE_2D, srcTexture);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_S, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_WRAP_T, gl.CLAMP_TO_EDGE);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.LINEAR);
  gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.LINEAR);
  gl.enable(gl.BLEND);
  gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);
}
initGL();

// Render the unrolled "strip" (boxes + text in linear (s, depth·D) coords) into
// the source canvas, then upload as a texture. Boxes drawn in depth order so
// outer depths over-paint inner where their radial extents overlap (matches
// what tessellation produces).
function buildWarpSource() {
  if (!gl || !layout.spiral || !layout.nodes.length) return false;
  const sp = layout.spiral;
  const boxH = 2 * sp.halfThickDepth * sp.D;             // recover box height from spiral params
  // bounds in layout units
  let maxDepth = 0;
  for (const n of layout.nodes) if (n.depth > maxDepth) maxDepth = n.depth;
  const layoutW = sp.totalS;
  const layoutH = maxDepth * sp.D + boxH;                // top of depth-0 box (y=0) down to bottom of maxDepth box
  const maxTex = gl.getParameter(gl.MAX_TEXTURE_SIZE);
  // one srcScale for both axes so the shader's mapping is uniform
  const srcScale = Math.max(1, layoutW / maxTex, layoutH / maxTex);
  const srcW = Math.max(2, Math.ceil(layoutW / srcScale));
  const srcH = Math.max(2, Math.ceil(layoutH / srcScale));
  srcCanvas.width = srcW;
  srcCanvas.height = srcH;
  layout.warp = { srcW, srcH, srcScale, maxDepth };
  const inv = 1 / srcScale;

  srcCtx.clearRect(0, 0, srcW, srcH);
  const sorted = [...layout.nodes].sort((a, b) => a.depth - b.depth);
  const fontSize = 11 * inv;
  const drawText = fontSize > 4;             // skip text when source is too tiny
  if (drawText) {
    srcCtx.font = `500 ${fontSize}px PM, monospace`;
    srcCtx.textBaseline = 'middle';
    srcCtx.textAlign = 'left';
  }
  for (const n of sorted) {
    const x = n.startS * inv;
    const y = n.depth * sp.D * inv;
    const w = n.w * inv;
    const h = boxH * inv;
    const color = n.kind === 'before' ? C.green : n.kind === 'run' || n.kind === 'after' ? C.red : C.ink;
    const border = n.kind === 'root' ? C.ink : color;
    srcCtx.fillStyle = n.removed ? '#e7e0d0' : C.paper2;
    srcCtx.fillRect(x, y, w, h);
    srcCtx.lineWidth = Math.max(0.5, (n.tip ? 2 : 1.25) * inv);
    srcCtx.strokeStyle = n.tip ? C.amber : border;
    if (n.removed) srcCtx.setLineDash([3 * inv, 2 * inv]);
    srcCtx.strokeRect(x, y, w, h);
    srcCtx.setLineDash([]);
    if (drawText) {
      const baseCol = n.kind === 'root' ? C.ink : border;
      const pad = 6 * inv;
      const chars = [...n.text];
      const mask = n.removedMask || '';
      const cw = chars.length ? (w - 2 * pad) / chars.length : 0;
      const cy = y + h / 2;
      for (let i = 0; i < chars.length; i++) {
        let ch = chars[i];
        if (ch === '\n') ch = '⏎'; else if (ch === '\t') ch = '⇥';
        const gone = mask[i] === '1';
        const gx = x + pad + i * cw;
        srcCtx.fillStyle = gone ? C.muted : baseCol;
        srcCtx.fillText(ch, gx, cy);
        if (gone) {
          srcCtx.strokeStyle = C.muted;
          srcCtx.lineWidth = Math.max(0.5, fontSize * 0.07);
          srcCtx.beginPath();
          srcCtx.moveTo(gx, cy);
          srcCtx.lineTo(gx + cw * 0.8, cy);
          srcCtx.stroke();
        }
      }
    }
  }
  gl.bindTexture(gl.TEXTURE_2D, srcTexture);
  gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, gl.RGBA, gl.UNSIGNED_BYTE, srcCanvas);
  return true;
}

function drawShader() {
  if (!gl || !layout.spiral) return;
  const key = layout.spiral.totalS + ':' + layout.nodes.length;
  if (key !== lastWarpKey) {
    if (!buildWarpSource()) return;
    lastWarpKey = key;
  }
  const dpr = window.devicePixelRatio || 1;
  gl.viewport(0, 0, glCanvas.width, glCanvas.height);
  gl.clearColor(0, 0, 0, 0);
  gl.clear(gl.COLOR_BUFFER_BIT);
  gl.useProgram(warpProg);
  gl.bindBuffer(gl.ARRAY_BUFFER, quadBuf);
  gl.enableVertexAttribArray(warpAttribs.a_pos);
  gl.vertexAttribPointer(warpAttribs.a_pos, 2, gl.FLOAT, false, 0, 0);
  const sp = layout.spiral, w = layout.warp;
  gl.uniform2f(warpUniforms.uRes, glCanvas.width, glCanvas.height);
  gl.uniform2f(warpUniforms.uViewT, view.tx * dpr, view.ty * dpr);
  gl.uniform1f(warpUniforms.uViewS, view.scale * dpr);
  gl.uniform1f(warpUniforms.uB, sp.b);
  gl.uniform1f(warpUniforms.uTheta0, sp.theta0);
  gl.uniform1f(warpUniforms.uTotalS, sp.totalS);
  gl.uniform1f(warpUniforms.uD, sp.D);
  gl.uniform1f(warpUniforms.uMaxDepth, w.maxDepth);
  gl.uniform1f(warpUniforms.uHalfThickDepth, sp.halfThickDepth);
  gl.uniform2f(warpUniforms.uSrcSize, w.srcW, w.srcH);
  gl.uniform1f(warpUniforms.uSrcScale, w.srcScale);
  gl.activeTexture(gl.TEXTURE0);
  gl.bindTexture(gl.TEXTURE_2D, srcTexture);
  gl.uniform1i(warpUniforms.uSrc, 0);
  gl.drawArrays(gl.TRIANGLES, 0, 6);
}

function invalidateWarp() { lastWarpKey = ''; }

// ───────────────────────── render ─────────────────────────
function draw() {
  const W = canvas.clientWidth, Hc = canvas.clientHeight;
  ctx.clearRect(0, 0, W, Hc);
  const { scale, tx, ty } = view;
  const toX = (x) => x * scale + tx, toY = (y) => y * scale + ty;
  const showDetail = scale > 0.55;

  // helper: orientation-agnostic edge — center-to-center with a perpendicular bow
  const cxOf = proj === 'linear' ? (n) => n.linCx : (n) => n.cx;
  const cyOf = proj === 'linear' ? (n) => n.linCy : (n) => n.cy;
  const drawEdge = (e, k = 0.18) => {
    const x1 = toX(cxOf(e.from)), y1 = toY(cyOf(e.from));
    const x2 = toX(cxOf(e.to)),   y2 = toY(cyOf(e.to));
    const dx = x2 - x1, dy = y2 - y1;
    const mx = (x1 + x2) / 2, my = (y1 + y2) / 2;
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.quadraticCurveTo(mx - dy * k, my + dx * k, x2, y2);
    ctx.stroke();
    return { x2, y2 };
  };

  // run-continuity links — segments of one run interrupted by an interior
  // before-child. Drawn first (under the anchor edges) as a soft solid line in
  // the run's colour so the run reads as continuous across the gap.
  if (showDetail && layout.runEdges?.length) {
    ctx.save();
    ctx.lineWidth = 1.5;
    for (const e of layout.runEdges) {
      ctx.strokeStyle = e.kind === 'before'
        ? 'rgba(47,93,67,0.5)'
        : (e.kind === 'run' || e.kind === 'after') ? 'rgba(178,58,46,0.5)' : 'rgba(27,24,22,0.5)';
      drawEdge(e, 0.22);
    }
    ctx.restore();
  }

  // parent/child (anchor) edges — gated to the same zoom threshold as labels
  if (showDetail) {
    ctx.lineWidth = 1;
    for (const e of layout.edges) {
      ctx.strokeStyle = e.rel === 'before' ? 'rgba(47,93,67,0.45)' : 'rgba(178,58,46,0.45)';
      drawEdge(e);
    }
  }

  // extra-dependency edges (causal context beyond the anchor) — dashed blue
  if (showDeps && layout.depEdges.length) {
    ctx.strokeStyle = 'rgba(63,92,134,0.45)';
    ctx.lineWidth = 1;
    ctx.setLineDash([4, 3]);
    for (const e of layout.depEdges) {
      const { x2, y2 } = drawEdge(e, 0.12);
      ctx.fillStyle = 'rgba(63,92,134,0.65)';
      ctx.beginPath();
      ctx.arc(x2, y2, 1.8, 0, Math.PI * 2);
      ctx.fill();
    }
    ctx.setLineDash([]);
  }

  // SHADER mode: the WebGL pass paints the warped boxes/text; the 2D canvas
  // above just holds the edges we already drew.
  if (proj === 'shader') {
    drawShader();
    return;
  }

  // LINEAR mode: native (s, depth·D) layout — no spiral, no rotation. Acts as
  // a sanity-check view of the underlying tree structure.
  if (proj === 'linear') {
    const sp0 = layout.spiral;
    const boxH = 2 * sp0.halfThickDepth * sp0.D;
    ctx.font = `500 ${11 * scale}px PM, monospace`;
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    for (const n of layout.nodes) {
      const color = n.kind === 'before' ? C.green : n.kind === 'run' || n.kind === 'after' ? C.red : C.ink;
      const border = n.kind === 'root' ? C.ink : color;
      const x = toX(n.startS), y = toY(n.depth * sp0.D - boxH / 2);
      const w = n.w * scale, h = boxH * scale;
      ctx.fillStyle = n.removed ? '#e7e0d0' : C.paper2;
      ctx.fillRect(x, y, w, h);
      ctx.lineWidth = n.tip ? 2 : 1.25;
      ctx.strokeStyle = n.tip ? C.amber : border;
      if (n.removed) ctx.setLineDash([3, 2]);
      ctx.strokeRect(x, y, w, h);
      ctx.setLineDash([]);
      if (showDetail) {
        const baseCol = n.kind === 'root' ? C.ink : border;
        const pad = 6 * scale;
        const chars = [...n.text];
        const mask = n.removedMask || '';
        const cw = chars.length ? (w - 2 * pad) / chars.length : 0;
        const fs = 11 * scale, cy = y + h / 2;
        for (let i = 0; i < chars.length; i++) {
          let ch = chars[i];
          if (ch === '\n') ch = '⏎'; else if (ch === '\t') ch = '⇥';
          const gone = mask[i] === '1';
          const gx = x + pad + i * cw;
          ctx.fillStyle = gone ? C.muted : baseCol;
          ctx.fillText(ch, gx, cy);
          if (gone) {
            ctx.strokeStyle = C.muted;
            ctx.lineWidth = Math.max(0.8, fs * 0.07);
            ctx.beginPath();
            ctx.moveTo(gx, cy);
            ctx.lineTo(gx + cw * 0.8, cy);
            ctx.stroke();
          }
        }
      }
    }
    return;
  }

  // TESS mode: tessellated ribbons + per-char text on the spiral
  const sp = layout.spiral;
  const sm = sp
    ? (s, d) => {
        const sEff = sp.totalS - s;
        const theta = Math.sqrt(2 * sEff / sp.b + sp.theta0 * sp.theta0);
        const alpha = -theta;
        const r = sp.b * theta + d * sp.D;
        return { x: r * Math.cos(alpha), y: r * Math.sin(alpha), rot: alpha + Math.PI / 2 };
      }
    : null;

  ctx.font = `500 ${11 * scale}px PM, monospace`;
  ctx.textBaseline = 'middle';
  ctx.textAlign = 'center';
  for (const n of layout.nodes) {
    const color = n.kind === 'before' ? C.green : n.kind === 'run' || n.kind === 'after' ? C.red : C.ink;
    const border = n.kind === 'root' ? C.ink : color;

    // Sample top + bottom edges along the rect's arc-length range. Segment density
    // adapts to on-screen length so we don't tessellate invisibly when zoomed out.
    const onScreenW = n.w * scale;
    const N = Math.max(2, Math.ceil(onScreenW / 8));
    const top = new Float64Array((N + 1) * 2);
    const bot = new Float64Array((N + 1) * 2);
    for (let i = 0; i <= N; i++) {
      const s = n.startS + (i / N) * n.w;
      const t = sm(s, n.depth - sp.halfThickDepth);
      const b = sm(s, n.depth + sp.halfThickDepth);
      top[i * 2] = toX(t.x); top[i * 2 + 1] = toY(t.y);
      bot[i * 2] = toX(b.x); bot[i * 2 + 1] = toY(b.y);
    }

    // Fill (closed ribbon polygon)
    ctx.fillStyle = n.removed ? '#e7e0d0' : C.paper2;
    ctx.beginPath();
    ctx.moveTo(top[0], top[1]);
    for (let i = 1; i <= N; i++) ctx.lineTo(top[i * 2], top[i * 2 + 1]);
    for (let i = N; i >= 0; i--) ctx.lineTo(bot[i * 2], bot[i * 2 + 1]);
    ctx.closePath();
    ctx.fill();

    // Stroke outline (top + right cap + bottom (reversed) + left cap)
    ctx.lineWidth = n.tip ? 2 : 1.25;
    ctx.strokeStyle = n.tip ? C.amber : border;
    if (n.removed) ctx.setLineDash([3, 2]);
    ctx.beginPath();
    ctx.moveTo(top[0], top[1]);
    for (let i = 1; i <= N; i++) ctx.lineTo(top[i * 2], top[i * 2 + 1]);
    ctx.lineTo(bot[N * 2], bot[N * 2 + 1]);
    for (let i = N - 1; i >= 0; i--) ctx.lineTo(bot[i * 2], bot[i * 2 + 1]);
    ctx.closePath();
    ctx.stroke();
    ctx.setLineDash([]);

    // Text — only when zoomed enough to read it. Each glyph is coloured and
    // struck through individually by its tombstone bit (a run can have just
    // some chars deleted), so deletions read at the character level.
    if (showDetail) {
      const baseCol = n.kind === 'root' ? C.ink : border;
      const fs = 11 * scale;
      const strikeHalf = fs * 0.32;
      const drawGlyph = (ch, gone) => {
        ctx.fillStyle = gone ? C.muted : baseCol;
        ctx.fillText(ch, 0, 0.5);
        if (gone) {
          ctx.strokeStyle = C.muted;
          ctx.lineWidth = Math.max(0.8, fs * 0.07);
          ctx.beginPath();
          ctx.moveTo(-strikeHalf, 0);
          ctx.lineTo(strikeHalf, 0);
          ctx.stroke();
        }
      };
      const chars = [...n.text];
      const mask = n.removedMask || '';
      if (chars.length === 1) {
        // single glyph — center it on the segment
        ctx.save();
        ctx.translate(toX(n.cx), toY(n.cy));
        ctx.rotate(n.flipText ? n.rot + Math.PI : n.rot);
        drawGlyph(chars[0], mask[0] === '1');
        ctx.restore();
      } else {
        // run: place each glyph individually along the arc length so the word
        // bends with the spiral. One flip decision per run (from the midpoint)
        // keeps the word reading consistently end-to-end.
        const pad = 6;                                 // matches segment PAD
        const textW = n.w - 2 * pad;
        const cw = textW / chars.length;
        for (let i = 0; i < chars.length; i++) {
          const s = n.startS + pad + (i + 0.5) * cw;
          const p = sm(s, n.depth);
          // when flipped, reverse char-to-position so the word still reads
          // naturally to the viewer (flipping reversed the on-screen reading
          // direction; placing chars in reverse undoes that for the word)
          const idx = n.flipText ? (chars.length - 1 - i) : i;
          let ch = chars[idx];
          if (ch === '\n') ch = '⏎'; else if (ch === '\t') ch = '⇥';
          ctx.save();
          ctx.translate(toX(p.x), toY(p.y));
          ctx.rotate(n.flipText ? p.rot + Math.PI : p.rot);
          drawGlyph(ch, mask[idx] === '1');
          ctx.restore();
        }
      }
    }
  }
}

// ───────────────────────── refresh pipeline ─────────────────────────
let treeDirty = false;
function refreshTree() {
  const struct = JSON.parse(seq.structureJson());
  buildLayout(struct);
  if (autoFit) fitView();
  $('s-nodes').textContent = struct.nodes.length.toLocaleString();
  // all non-root boxes live in HashSeq.runs (After- and Before-anchored alike)
  $('s-runs').textContent = struct.nodes.filter((n) => n.kind !== 'root').length.toLocaleString();
  $('s-tips').textContent = struct.tips.length.toLocaleString();
  draw();
}
function refreshStatsLight() {
  $('s-chars').textContent = seq.len().toLocaleString();
}
function refreshOpLog() {
  const el = $('oplog');
  const rows = opLog.slice(-120).reverse();
  el.innerHTML = rows.map((o) =>
    `<div class="row"><span class="tag ${o.kind}">${o.kind.toUpperCase()}</span>` +
    `<span class="hex">${o.hex ?? '— root insert (state) —'}</span></div>`).join('');
}

let rafQueued = false;
function scheduleRender() {
  if (rafQueued) return;
  rafQueued = true;
  requestAnimationFrame(() => {
    rafQueued = false;
    refreshStatsLight();
    refreshTree();
    refreshOpLog();
  });
}

// ───────────────────────── editor (type mode) ─────────────────────────
// Capture the selection range *before* the edit — this is the range (in the old
// value) that the edit will replace, which is what disambiguates duplicates.
let pendingSel = null;
let composing = false;

editor.addEventListener('beforeinput', () => {
  if (mode !== 'type') return;
  pendingSel = { start: editor.selectionStart, end: editor.selectionEnd, old: editor.value };
});

editor.addEventListener('compositionstart', () => { composing = true; });
editor.addEventListener('compositionend', () => {
  composing = false;
  // reconcile the whole composed result via diff (rare; positionally best-effort)
  const t = editor.value;
  applyEdit(lastText, t);
  lastText = t;
  pendingSel = null;
  scheduleRender();
});

editor.addEventListener('input', (e) => {
  if (mode !== 'type') return;
  if (composing || e.isComposing) return;             // wait for compositionend
  const t = editor.value;
  if (pendingSel && pendingSel.old === lastText) {
    applyCaretEdit(pendingSel.old, t, pendingSel.start, pendingSel.end);
  } else {
    applyEdit(lastText, t);                            // fallback (no beforeinput)
  }
  pendingSel = null;
  lastText = t;
  scheduleRender();
});

// ───────────────────────── trace replay ─────────────────────────
let trace = null;        // flat array of {pos, del, ins}
let cursorIdx = 0;
let playing = false;

async function loadTrace(name) {
  setReplayButtons(false, true);
  $('progress-txt').textContent = 'loading…';
  const res = await fetch(`./traces/${name}.json`);
  const data = await res.json();
  trace = [];
  for (const txn of data.txns)
    for (const [pos, del, ins] of txn.patches) trace.push({ pos, del, ins });
  resetReplay();
  setReplayButtons(true, false);
}

function resetReplay() {
  playing = false; $('play').textContent = '▶ PLAY';
  seq = new WasmHashSeq();
  lastText = '';
  opLog.length = 0;
  cursorIdx = 0;
  autoFit = true;
  editor.value = '';
  updateProgress();
  scheduleRender();
}

function applyPatch(p) {
  if (p.del > 0) applyDelete(p.pos, p.del);
  if (p.ins.length) applyInsert(p.pos, p.ins);
}

function stepN(count) {
  const end = Math.min(trace.length, cursorIdx + count);
  for (; cursorIdx < end; cursorIdx++) applyPatch(trace[cursorIdx]);
  editor.value = seq.text();
  updateProgress();
  scheduleRender();
  if (cursorIdx >= trace.length) { playing = false; $('play').textContent = '▶ PLAY'; }
}

function updateProgress() {
  const total = trace ? trace.length : 0;
  $('progress-bar').style.width = total ? `${(cursorIdx / total) * 100}%` : '0%';
  $('progress-txt').textContent = `${cursorIdx.toLocaleString()} / ${total.toLocaleString()}`;
}

function playLoop() {
  if (!playing) return;
  const perFrame = parseInt($('speed').value, 10);
  stepN(perFrame);
  if (cursorIdx < trace.length && playing) requestAnimationFrame(playLoop);
  else { playing = false; $('play').textContent = '▶ PLAY'; }
}

function setReplayButtons(enabled, loading) {
  $('play').disabled = !enabled; $('step').disabled = !enabled; $('reset').disabled = !enabled;
  if (loading) $('play').disabled = $('step').disabled = $('reset').disabled = true;
}

$('trace-select').addEventListener('change', (e) => {
  if (e.target.value) loadTrace(e.target.value);
});
$('play').addEventListener('click', () => {
  if (!trace) return;
  if (cursorIdx >= trace.length) resetReplay();
  playing = !playing;
  $('play').textContent = playing ? '⏸ PAUSE' : '▶ PLAY';
  if (playing) requestAnimationFrame(playLoop);
});
$('step').addEventListener('click', () => { if (trace) { playing = false; $('play').textContent = '▶ PLAY'; stepN(1); } });
$('reset').addEventListener('click', resetReplay);

// ───────────────────────── mode tabs ─────────────────────────
document.getElementById('tabs').addEventListener('click', (e) => {
  const b = e.target.closest('button'); if (!b) return;
  mode = b.dataset.mode;
  for (const btn of e.currentTarget.children) btn.classList.toggle('active', btn === b);
  $('replay-controls').style.visibility = mode === 'replay' ? 'visible' : 'hidden';
  if (mode === 'type') {
    playing = false; $('play').textContent = '▶ PLAY';
    seq = new WasmHashSeq(); lastText = ''; opLog.length = 0; trace = null;
    editor.readOnly = false; editor.value = lastText; autoFit = true;
    updateProgress(); scheduleRender(); editor.focus();
  } else {
    editor.readOnly = true;
    setReplayButtons(false, false);
  }
});

// ───────────────────────── canvas interaction ─────────────────────────
canvas.addEventListener('wheel', (e) => {
  e.preventDefault();
  if (autoFit) fitView();        // begin free-zoom from the exact displayed fit (no snap)
  autoFit = false;
  const rect = canvas.getBoundingClientRect();
  const mx = e.clientX - rect.left, my = e.clientY - rect.top;
  const wx = (mx - view.tx) / view.scale, wy = (my - view.ty) / view.scale;
  // normalize wheel delta across devices (px / lines / pages), then damp per event
  let dy = e.deltaY;
  if (e.deltaMode === 1) dy *= 16;
  else if (e.deltaMode === 2) dy *= canvas.clientHeight;
  const factor = Math.min(1.5, Math.max(1 / 1.5, Math.exp(-dy * 0.0015)));
  // bounds adapt to the fit scale so the fitted zoom level is always reachable
  const lo = Math.min(view.fitScale, 0.02), hi = Math.max(8, view.fitScale);
  view.scale = Math.max(lo, Math.min(hi, view.scale * factor));
  view.tx = mx - wx * view.scale;
  view.ty = my - wy * view.scale;
  draw();
}, { passive: false });

let dragging = false, lastMx = 0, lastMy = 0;
canvas.addEventListener('mousedown', (e) => { dragging = true; autoFit = false; lastMx = e.clientX; lastMy = e.clientY; canvas.classList.add('panning'); });
window.addEventListener('mousemove', (e) => {
  if (!dragging) return;
  view.tx += e.clientX - lastMx; view.ty += e.clientY - lastMy;
  lastMx = e.clientX; lastMy = e.clientY; draw();
});
window.addEventListener('mouseup', () => { dragging = false; canvas.classList.remove('panning'); });
$('fit').addEventListener('click', () => { autoFit = true; fitView(); draw(); });
$('show-deps').addEventListener('change', (e) => { showDeps = e.target.checked; draw(); });
$('proj').addEventListener('change', (e) => {
  proj = e.target.value;                              // 'tess' | 'shader' | 'linear'
  document.body.dataset.proj = proj;                  // CSS toggles the WebGL canvas visibility
  invalidateWarp();                                   // re-render source on next shader draw
  if (autoFit) fitView();                             // linear bounds differ — refit so the layout is on-screen
  draw();
});
document.body.dataset.proj = proj;                    // initialize

window.addEventListener('resize', () => { resizeCanvas(); if (autoFit) fitView(); draw(); });

// ───────────────────────── boot ─────────────────────────
$('replay-controls').style.visibility = 'hidden';
resizeCanvas();
editor.value = 'hello world';
lastText = '';
applyEdit('', 'hello world');
lastText = 'hello world';
scheduleRender();
editor.focus();

// optional scripted scenarios for testing/demo via ?scenario=…
const params = new URLSearchParams(location.search);
if (params.get('deps') === '1') { showDeps = true; $('show-deps').checked = true; draw(); }
const projParam = params.get('proj');
if (projParam === 'shader' || projParam === 'linear') {
  proj = projParam;
  $('proj').value = projParam;
  document.body.dataset.proj = projParam;
  invalidateWarp();
  if (autoFit) fitView();
  draw();
}
const scenario = params.get('scenario');
if (scenario === 'fork') {
  // a chain of mid-document insertions: splits runs, creates before/after branches
  const states = [
    'hello world',
    'hello brave world',
    'hello brave new world',
    'oh hello brave new world',
    'oh hello brave new world!',
  ];
  let prev = lastText;
  for (const st of states) { applyEdit(prev, st); prev = st; }
  editor.value = prev; lastText = prev; scheduleRender();
} else if (scenario === 'caret') {
  // exact bug-report sequence, driven through the caret-precise path:
  // type 'b' between y and b (caret at 3) — must anchor After(y), not After(b).
  seq = new WasmHashSeq(); lastText = ''; opLog.length = 0;
  const edits = [
    { old: '', val: 'ab', s: 0, e: 0 },
    { old: 'ab', val: 'axb', s: 1, e: 1 },
    { old: 'axb', val: 'axyb', s: 2, e: 2 },
    { old: 'axyb', val: 'axybb', s: 3, e: 3 },
  ];
  for (const ed of edits) { applyCaretEdit(ed.old, ed.val, ed.s, ed.e); lastText = ed.val; }
  editor.value = lastText; autoFit = true; scheduleRender();
} else if (scenario === 'replay') {
  mode = 'replay';
  editor.readOnly = true;
  const name = params.get('trace') || 'clownschool_flat';
  const steps = parseInt(params.get('steps') || '4000', 10);
  await loadTrace(name);
  stepN(steps);
}
