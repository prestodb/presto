//! # Substrait Integration Layer
//!
//! This crate provides bidirectional conversion between the [Substrait](https://substrait.io/)
//! cross-language query plan representation and the optimizer's internal plan representation.
//!
//! ## Why Substrait?
//!
//! Substrait is an industry-standard serialization format for query plans. Using it as
//! the interchange format between the Presto Java coordinator and this Rust optimizer
//! provides several benefits:
//!
//! - **Language independence**: The Java coordinator serializes a logical plan as a
//!   Substrait protobuf, sends it over the wire, and the Rust optimizer deserializes it.
//!   No JNI or FFI needed.
//! - **Standard semantics**: Substrait defines precise semantics for relational operators,
//!   reducing the risk of semantic mismatches between the Java and Rust representations.
//! - **Ecosystem compatibility**: Other tools (DataFusion, DuckDB, Velox) also support
//!   Substrait, enabling interoperability.
//!
//! ## Module Overview
//!
//! - **`consumer`**: Converts Substrait Plan -> internal Memo representation (deserialization).
//! - **`producer`**: Converts optimized PlanNode -> Substrait Plan (serialization).
//! - **`extensions`**: Encodes/decodes table statistics as Substrait advanced extensions
//!   (protobuf `Any` messages) so statistics can travel with the plan.

pub mod consumer;
pub mod extensions;
pub mod producer;
