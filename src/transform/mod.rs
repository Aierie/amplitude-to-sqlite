pub mod verifier;
pub mod compare;
pub mod model;
pub mod dupe_cleaner;

pub use dupe_cleaner::{clean_duplicates_and_types, OutputMode}; 