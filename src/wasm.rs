use wasm_bindgen::prelude::*;

use crate::encoding::{decode_hashseq, encode_hashseq};
use crate::hashseq::HashSeq;

#[wasm_bindgen]
#[derive(Default)]
pub struct WasmHashSeq {
    inner: HashSeq,
}

#[wasm_bindgen]
impl WasmHashSeq {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, idx: usize, text: &str) {
        self.inner.insert_batch(idx, text.chars());
    }

    pub fn remove(&mut self, idx: usize, len: usize) {
        self.inner.remove_batch(idx, len);
    }

    pub fn text(&self) -> String {
        self.inner.iter().collect()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn encode(&self) -> Vec<u8> {
        encode_hashseq(&self.inner)
    }

    pub fn merge_encoded(&mut self, bytes: &[u8]) -> Result<(), JsValue> {
        let other = decode_hashseq(bytes)
            .map_err(|e| JsValue::from_str(&format!("decode error: {e}")))?;
        self.inner.merge(other);
        Ok(())
    }
}
