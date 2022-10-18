#![feature(map_first_last)]
#![feature(let_chains)]
#![feature(int_log)]

pub mod hashseq;
// pub mod topo_after_and_before;
pub mod topo_sort;
// pub mod topo_sort_strong_weak;
pub mod cursor;
// pub mod tree;
pub mod hash_node;

pub use self::cursor::Cursor;
pub use self::hash_node::{HashNode, Op};
pub use self::hashseq::HashSeq;

// type Id = [u8; 32];
pub type Id = u64;
