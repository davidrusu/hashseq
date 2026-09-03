import init, { WasmHashSeq } from './pkg/hashseq.js';

import {
  basicSetup,
  EditorView,
} from 'https://esm.sh/codemirror@6.0.1';
import { oneDark } from 'https://esm.sh/@codemirror/theme-one-dark@6.1.2';

await init();

// Two independent CRDT peers
const peerA = new WasmHashSeq();
const peerB = new WasmHashSeq();

const lenA = document.getElementById('len-a');
const lenB = document.getElementById('len-b');
const statusEl = document.getElementById('status');

function updateLen(peer, el) {
  el.textContent = `${peer.len()} chars`;
}

// Track whether we're programmatically updating editors (to avoid feedback loops)
let updatingA = false;
let updatingB = false;

// CodeMirror positions are UTF-16 code units; the peer indexes chars
// (Unicode scalars — an emoji is one slot, not two).
const cpLen = (s) => [...s].length;

function makeUpdateListener(peer, lenEl, flagGetter, flagSetter) {
  return EditorView.updateListener.of((update) => {
    if (!update.docChanged || flagGetter()) return;
    const before = update.startState.doc; // fromA/toA index this doc
    let offset = 0; // char drift from earlier changes in this set
    update.changes.iterChanges((fromA, toA, fromB, toB, inserted) => {
      const adjustedFrom = cpLen(before.sliceString(0, fromA)) + offset;
      const removedLen = cpLen(before.sliceString(fromA, toA));
      if (removedLen > 0) {
        peer.remove(adjustedFrom, removedLen);
      }
      const text = inserted.toString();
      const insertedLen = cpLen(text);
      if (insertedLen > 0) {
        peer.insert(adjustedFrom, text);
      }
      offset += insertedLen - removedLen;
    });
    updateLen(peer, lenEl);
    statusEl.textContent = '';
    statusEl.className = 'status';
  });
}

const editorA = new EditorView({
  doc: '',
  extensions: [
    basicSetup,
    oneDark,
    makeUpdateListener(
      peerA,
      lenA,
      () => updatingA,
      (v) => (updatingA = v)
    ),
  ],
  parent: document.getElementById('editor-a'),
});

const editorB = new EditorView({
  doc: '',
  extensions: [
    basicSetup,
    oneDark,
    makeUpdateListener(
      peerB,
      lenB,
      () => updatingB,
      (v) => (updatingB = v)
    ),
  ],
  parent: document.getElementById('editor-b'),
});

function setEditorContent(editor, text, flag) {
  const current = editor.state.doc.toString();
  if (current === text) return;
  flag(true);
  editor.dispatch({
    changes: { from: 0, to: current.length, insert: text },
  });
  flag(false);
}

document.getElementById('sync-btn').addEventListener('click', () => {
  // Encode each peer's state
  const bytesA = peerA.encode();
  const bytesB = peerB.encode();

  // Cross-merge
  try {
    peerA.mergeEncoded(bytesB);
  } catch (e) {
    console.error('Peer A merge failed:', e);
    statusEl.textContent = `Merge into A failed: ${e}`;
    statusEl.className = 'status';
    return;
  }
  try {
    peerB.mergeEncoded(bytesA);
  } catch (e) {
    console.error('Peer B merge failed:', e);
    statusEl.textContent = `Merge into B failed: ${e}`;
    statusEl.className = 'status';
    return;
  }

  // Get merged text (should be identical now)
  const textA = peerA.text();
  const textB = peerB.text();

  // Update editors
  setEditorContent(editorA, textA, (v) => (updatingA = v));
  setEditorContent(editorB, textB, (v) => (updatingB = v));

  updateLen(peerA, lenA);
  updateLen(peerB, lenB);

  statusEl.textContent =
    textA === textB
      ? `Synced! Both peers agree on ${cpLen(textA)} characters.`
      : 'Warning: peers diverged (this should not happen with a CRDT).';
  statusEl.className = textA === textB ? 'status synced' : 'status';
});
