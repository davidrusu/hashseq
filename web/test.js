import init, { WasmHashSeq } from './pkg/hashseq.js';

await init();

const output = document.getElementById('output');
let passed = 0;
let failed = 0;

function log(html) {
  output.insertAdjacentHTML('beforeend', html);
}

function assert(actual, expected, label) {
  if (actual === expected) {
    passed++;
    log(`<pre class="pass">  PASS: ${label}</pre>`);
  } else {
    failed++;
    log(
      `<pre class="fail">  FAIL: ${label}\n        expected: ${JSON.stringify(expected)}\n        actual:   ${JSON.stringify(actual)}</pre>`
    );
  }
}

function section(name) {
  log(`<h2>${name}</h2>`);
}

// ---------------------------------------------------------------------------
// Simulates what index.js does inside the iterChanges callback.
// All (fromA, toA) positions are in the OLD document coordinate space
// (as CodeMirror's iterChanges provides), so we track a cumulative offset
// to map them to current HashSeq positions.
// ---------------------------------------------------------------------------
function applyChanges(seq, changes) {
  let offset = 0;
  for (const [fromA, toA, text] of changes) {
    const adjustedFrom = fromA + offset;
    const removedLen = toA - fromA;
    if (removedLen > 0) {
      seq.remove(adjustedFrom, removedLen);
    }
    if (text.length > 0) {
      seq.insert(adjustedFrom, text);
    }
    offset += text.length - removedLen;
  }
}

// ---------------------------------------------------------------------------
// Fuzz helpers
// ---------------------------------------------------------------------------
const ALPHABET = 'abcdefghijklmnopqrstuvwxyz \n';

function randInt(max) {
  return (Math.random() * max) | 0;
}

function randChar() {
  return ALPHABET[randInt(ALPHABET.length)];
}

function randString(maxLen) {
  const len = 1 + randInt(maxLen);
  let s = '';
  for (let i = 0; i < len; i++) s += randChar();
  return s;
}

// Apply a random edit to both a JS string (model) and a WasmHashSeq.
// Returns the new model string.
function randomEdit(model, seq) {
  const len = model.length;
  const r = Math.random();

  if (len === 0 || r < 0.5) {
    // Insert
    const pos = randInt(len + 1);
    const text = randString(5);
    seq.insert(pos, text);
    return model.slice(0, pos) + text + model.slice(pos);
  } else if (r < 0.8) {
    // Delete
    const from = randInt(len);
    const delLen = 1 + randInt(Math.min(5, len - from));
    seq.remove(from, delLen);
    return model.slice(0, from) + model.slice(from + delLen);
  } else {
    // Replace
    const from = randInt(len);
    const delLen = 1 + randInt(Math.min(5, len - from));
    const text = randString(3);
    seq.remove(from, delLen);
    seq.insert(from, text);
    return model.slice(0, from) + text + model.slice(from + delLen);
  }
}

// Apply a random multi-change transaction (like CodeMirror would produce).
// Generates N non-overlapping changes in old-doc coordinate space,
// applies them to both model and seq via applyChanges.
// Returns the new model string.
function randomMultiChange(model, seq) {
  const len = model.length;
  if (len < 4) {
    // Too short for multi-change, do a single insert
    const text = randString(3);
    seq.insert(0, text);
    return text + model;
  }

  // Generate 2-4 non-overlapping change positions in old-doc space
  const numChanges = 2 + randInt(3);
  const positions = new Set();
  while (positions.size < Math.min(numChanges, len)) {
    positions.add(randInt(len));
  }
  const sorted = [...positions].sort((a, b) => a - b);

  // Build changes: each is [fromA, toA, insertedText]
  const changes = [];
  let newModel = model;
  let modelOffset = 0;

  for (const pos of sorted) {
    const r = Math.random();
    if (r < 0.4 && pos < len - 1) {
      // Delete 1 char
      changes.push([pos, pos + 1, '']);
      const adj = pos + modelOffset;
      newModel = newModel.slice(0, adj) + newModel.slice(adj + 1);
      modelOffset -= 1;
    } else if (r < 0.7) {
      // Insert
      const text = randString(2);
      changes.push([pos, pos, text]);
      const adj = pos + modelOffset;
      newModel = newModel.slice(0, adj) + text + newModel.slice(adj);
      modelOffset += text.length;
    } else if (pos < len - 1) {
      // Replace 1 char
      const text = randChar();
      changes.push([pos, pos + 1, text]);
      const adj = pos + modelOffset;
      newModel = newModel.slice(0, adj) + text + newModel.slice(adj + 1);
      // offset unchanged (removed 1, inserted 1)
    } else {
      // Insert at end
      const text = randChar();
      changes.push([pos, pos, text]);
      const adj = pos + modelOffset;
      newModel = newModel.slice(0, adj) + text + newModel.slice(adj);
      modelOffset += 1;
    }
  }

  applyChanges(seq, changes);
  return newModel;
}

// ===== Single-change: basic editing =====

section('Sequential typing');
{
  const seq = new WasmHashSeq();
  applyChanges(seq, [[0, 0, 'h']]);
  assert(seq.text(), 'h', 'type "h"');
  applyChanges(seq, [[1, 1, 'e']]);
  assert(seq.text(), 'he', 'type "e"');
  applyChanges(seq, [[2, 2, 'l']]);
  assert(seq.text(), 'hel', 'type "l"');
  applyChanges(seq, [[3, 3, 'l']]);
  assert(seq.text(), 'hell', 'type "l"');
  applyChanges(seq, [[4, 4, 'o']]);
  assert(seq.text(), 'hello', 'type "o"');
}

section('Backspace');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[4, 5, '']]);
  assert(seq.text(), 'hell', 'backspace at end');
  applyChanges(seq, [[3, 4, '']]);
  assert(seq.text(), 'hel', 'backspace again');
}

section('Delete forward');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[1, 2, '']]);
  assert(seq.text(), 'hllo', 'delete forward at pos 1');
}

section('Replace selection');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[1, 4, 'a']]);
  assert(seq.text(), 'hao', 'replace "ell" with "a"');
}

section('Insert in middle');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[2, 2, 'X']]);
  assert(seq.text(), 'heXllo', 'insert "X" at pos 2');
}

section('Paste multi-char');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hd');
  applyChanges(seq, [[1, 1, 'ello worl']]);
  assert(seq.text(), 'hello world', 'paste in middle');
}

section('Replace all text');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[0, 5, 'world']]);
  assert(seq.text(), 'world', 'select-all and replace');
}

section('Delete all');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello');
  applyChanges(seq, [[0, 5, '']]);
  assert(seq.text(), '', 'select-all and delete');
  assert(seq.len(), 0, 'length is 0');
}

// ===== Multi-change: transactions with multiple edits =====

section('Multi-change: two deletes');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'abcdef');
  applyChanges(seq, [
    [1, 2, ''],
    [4, 5, ''],
  ]);
  assert(seq.text(), 'acdf', 'two deletes at different positions');
}

section('Multi-change: two inserts');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'abcdef');
  applyChanges(seq, [
    [2, 2, 'X'],
    [4, 4, 'Y'],
  ]);
  assert(seq.text(), 'abXcdYef', 'two inserts at different positions');
}

section('Multi-change: two replacements');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'hello world');
  applyChanges(seq, [
    [1, 5, 'i'],
    [7, 11, 'ow'],
  ]);
  assert(seq.text(), 'hi wow', 'two replacements');
}

section('Multi-change: indent two lines');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'foo\nbar');
  applyChanges(seq, [
    [0, 0, '  '],
    [4, 4, '  '],
  ]);
  assert(seq.text(), '  foo\n  bar', 'indent both lines');
}

section('Multi-change: find-and-replace');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'the cat sat on the mat');
  applyChanges(seq, [
    [0, 3, 'a'],
    [15, 18, 'a'],
  ]);
  assert(seq.text(), 'a cat sat on a mat', 'replace all occurrences');
}

section('Multi-change: three inserts (multi-cursor)');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'aaa');
  applyChanges(seq, [
    [1, 1, 'X'],
    [2, 2, 'X'],
    [3, 3, 'X'],
  ]);
  assert(seq.text(), 'aXaXaX', 'three multi-cursor inserts');
}

section('Multi-change: delete + insert (different positions)');
{
  const seq = new WasmHashSeq();
  seq.insert(0, 'abcdef');
  applyChanges(seq, [
    [0, 1, ''],
    [6, 6, 'Z'],
  ]);
  assert(seq.text(), 'bcdefZ', 'delete at start, insert at end');
}

// ===== Sync / merge tests =====

section('Sync: independent edits merge');
{
  const a = new WasmHashSeq();
  const b = new WasmHashSeq();
  a.insert(0, 'hello');
  b.insert(0, 'world');
  const bytesA = a.encode();
  const bytesB = b.encode();
  a.merge_encoded(bytesB);
  b.merge_encoded(bytesA);
  assert(a.text(), b.text(), 'both peers agree after sync');
  assert(a.text().includes('hello'), true, 'merged contains "hello"');
  assert(a.text().includes('world'), true, 'merged contains "world"');
}

section('Sync: idempotent');
{
  const a = new WasmHashSeq();
  const b = new WasmHashSeq();
  a.insert(0, 'hello');
  b.insert(0, 'world');
  const bytesA = a.encode();
  const bytesB = b.encode();
  a.merge_encoded(bytesB);
  b.merge_encoded(bytesA);
  const text1 = a.text();
  a.merge_encoded(b.encode());
  b.merge_encoded(a.encode());
  assert(a.text(), text1, 'repeated sync is idempotent (peer A)');
  assert(b.text(), text1, 'repeated sync is idempotent (peer B)');
}

section('Sync: edit-then-sync round trip');
{
  const a = new WasmHashSeq();
  const b = new WasmHashSeq();
  a.insert(0, 'hello ');
  b.merge_encoded(a.encode());
  assert(b.text(), 'hello ', 'peer B has "hello " after first sync');
  b.insert(6, 'world');
  a.insert(6, 'there');
  const ba = b.encode();
  const ab = a.encode();
  a.merge_encoded(ba);
  b.merge_encoded(ab);
  assert(a.text(), b.text(), 'peers agree after concurrent edits');
  assert(a.text().includes('hello '), true, 'merged has common prefix');
  assert(a.text().includes('world'), true, 'merged has peer B edit');
  assert(a.text().includes('there'), true, 'merged has peer A edit');
}

// ===== Fuzz: single-peer model check =====

section('Fuzz: single-peer vs JS string model (500 iterations)');
{
  const ITERS = 500;
  const seq = new WasmHashSeq();
  let model = '';
  let fuzzFailed = false;

  for (let i = 0; i < ITERS; i++) {
    model = randomEdit(model, seq);
    const actual = seq.text();
    if (actual !== model) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: model diverged\n        expected: ${JSON.stringify(model.slice(0, 80))}...\n        actual:   ${JSON.stringify(actual.slice(0, 80))}...</pre>`
      );
      fuzzFailed = true;
      break;
    }
  }
  if (!fuzzFailed) {
    passed++;
    log(
      `<pre class="pass">  PASS: ${ITERS} random edits, HashSeq always matched JS model (final len: ${model.length})</pre>`
    );
  }
}

// ===== Fuzz: multi-change transactions vs model =====

section('Fuzz: multi-change transactions vs model (200 iterations)');
{
  const ITERS = 200;
  const seq = new WasmHashSeq();
  let model = 'the quick brown fox jumps over the lazy dog';
  seq.insert(0, model);
  let fuzzFailed = false;

  for (let i = 0; i < ITERS; i++) {
    if (Math.random() < 0.3) {
      // Single change
      model = randomEdit(model, seq);
    } else {
      // Multi-change transaction
      model = randomMultiChange(model, seq);
    }
    const actual = seq.text();
    if (actual !== model) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: model diverged after multi-change\n        expected: ${JSON.stringify(model.slice(0, 80))}...\n        actual:   ${JSON.stringify(actual.slice(0, 80))}...</pre>`
      );
      fuzzFailed = true;
      break;
    }
  }
  if (!fuzzFailed) {
    passed++;
    log(
      `<pre class="pass">  PASS: ${ITERS} random transactions, HashSeq always matched JS model (final len: ${model.length})</pre>`
    );
  }
}

// ===== Fuzz: encode/decode roundtrip =====

section('Fuzz: encode/decode roundtrip (100 iterations)');
{
  const ITERS = 100;
  let fuzzFailed = false;

  for (let i = 0; i < ITERS; i++) {
    // Build a random sequence
    const seq = new WasmHashSeq();
    let model = '';
    const numOps = 10 + randInt(40);
    for (let j = 0; j < numOps; j++) {
      model = randomEdit(model, seq);
    }

    // Encode then decode via merge_encoded into fresh instance
    const bytes = seq.encode();
    const seq2 = new WasmHashSeq();
    try {
      seq2.merge_encoded(bytes);
    } catch (e) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: decode error: ${e}\n        text was: ${JSON.stringify(model.slice(0, 80))}...</pre>`
      );
      fuzzFailed = true;
      break;
    }

    const decoded = seq2.text();
    if (decoded !== model) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: roundtrip mismatch\n        original: ${JSON.stringify(model.slice(0, 80))}...\n        decoded:  ${JSON.stringify(decoded.slice(0, 80))}...</pre>`
      );
      fuzzFailed = true;
      break;
    }
  }
  if (!fuzzFailed) {
    passed++;
    log(
      `<pre class="pass">  PASS: ${ITERS} random sequences encoded/decoded correctly</pre>`
    );
  }
}

// ===== Fuzz: two-peer sync after random edits =====

section('Fuzz: two-peer sync (100 iterations)');
{
  const ITERS = 100;
  let fuzzFailed = false;

  for (let i = 0; i < ITERS; i++) {
    const a = new WasmHashSeq();
    const b = new WasmHashSeq();
    let modelA = '';
    let modelB = '';

    // Each peer does random edits independently
    const numOps = 5 + randInt(20);
    for (let j = 0; j < numOps; j++) {
      modelA = randomEdit(modelA, a);
    }
    for (let j = 0; j < numOps; j++) {
      modelB = randomEdit(modelB, b);
    }

    // Encode before merge
    const bytesA = a.encode();
    const bytesB = b.encode();

    // Cross-merge
    try {
      a.merge_encoded(bytesB);
      b.merge_encoded(bytesA);
    } catch (e) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: merge decode error: ${e}</pre>`
      );
      fuzzFailed = true;
      break;
    }

    const textA = a.text();
    const textB = b.text();

    if (textA !== textB) {
      failed++;
      log(
        `<pre class="fail">  FAIL at iteration ${i}: peers diverged after sync\n        peer A: ${JSON.stringify(textA.slice(0, 80))}...\n        peer B: ${JSON.stringify(textB.slice(0, 80))}...</pre>`
      );
      fuzzFailed = true;
      break;
    }

    // Verify both contain all content from each peer
    // (can't check exact text since merge order depends on IDs)
  }
  if (!fuzzFailed) {
    passed++;
    log(
      `<pre class="pass">  PASS: ${ITERS} random two-peer syncs, peers always agreed</pre>`
    );
  }
}

// ===== Fuzz: edit-sync-edit-sync cycle =====

section('Fuzz: edit-sync-edit-sync cycles (50 iterations)');
{
  const ITERS = 50;
  let fuzzFailed = false;

  for (let i = 0; i < ITERS; i++) {
    const a = new WasmHashSeq();
    const b = new WasmHashSeq();
    let modelA = '';
    let modelB = '';

    // 3 rounds of: both edit, then sync
    for (let round = 0; round < 3; round++) {
      // Each peer edits
      const numOps = 3 + randInt(10);
      for (let j = 0; j < numOps; j++) {
        modelA = randomEdit(modelA, a);
      }
      for (let j = 0; j < numOps; j++) {
        modelB = randomEdit(modelB, b);
      }

      // Sync
      const bytesA = a.encode();
      const bytesB = b.encode();
      try {
        a.merge_encoded(bytesB);
        b.merge_encoded(bytesA);
      } catch (e) {
        failed++;
        log(
          `<pre class="fail">  FAIL at iteration ${i}, round ${round}: decode error: ${e}</pre>`
        );
        fuzzFailed = true;
        break;
      }

      const textA = a.text();
      const textB = b.text();
      if (textA !== textB) {
        failed++;
        log(
          `<pre class="fail">  FAIL at iteration ${i}, round ${round}: peers diverged\n        peer A (${textA.length} chars): ${JSON.stringify(textA.slice(0, 60))}...\n        peer B (${textB.length} chars): ${JSON.stringify(textB.slice(0, 60))}...</pre>`
        );
        fuzzFailed = true;
        break;
      }

      // After sync, update models to match merged state
      modelA = textA;
      modelB = textB;
    }
    if (fuzzFailed) break;
  }
  if (!fuzzFailed) {
    passed++;
    log(
      `<pre class="pass">  PASS: ${ITERS} multi-round edit-sync cycles, peers always agreed</pre>`
    );
  }
}

// ===== Summary =====

log(
  `<div class="summary ${failed > 0 ? 'fail' : 'pass'}">${passed} passed, ${failed} failed</div>`
);
