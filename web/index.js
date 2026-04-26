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

function makeUpdateListener(peer, lenEl, flagGetter, flagSetter) {
  return EditorView.updateListener.of((update) => {
    if (!update.docChanged || flagGetter()) return;
    let offset = 0;
    update.changes.iterChanges((fromA, toA, fromB, toB, inserted) => {
      const adjustedFrom = fromA + offset;
      const removedLen = toA - fromA;
      if (removedLen > 0) {
        peer.remove(adjustedFrom, removedLen);
      }
      const text = inserted.toString();
      if (text.length > 0) {
        peer.insert(adjustedFrom, text);
      }
      offset += text.length - removedLen;
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
    peerA.merge_encoded(bytesB);
  } catch (e) {
    console.error('Peer A merge failed:', e);
    statusEl.textContent = `Merge into A failed: ${e}`;
    statusEl.className = 'status';
    return;
  }
  try {
    peerB.merge_encoded(bytesA);
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
      ? `Synced! Both peers agree on ${textA.length} characters.`
      : 'Warning: peers diverged (this should not happen with a CRDT).';
  statusEl.className = textA === textB ? 'status synced' : 'status';
});
