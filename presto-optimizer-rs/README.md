# OptX: Cascades Query Optimizer for Presto

A standalone Rust-based Cascades cost-based query optimizer that communicates via [Substrait](https://substrait.io/) (protobuf). Inspired by the Columbia/Cascades optimizer framework and Greenplum's Orca.

## Architecture

```
optx-core        Core Cascades engine (memo, search, cost model, rules)
optx-rules       Built-in transformation and implementation rules
optx-substrait   Substrait <-> internal representation conversion
optx-server      Axum REST API for remote optimization
```

### How the Cascades Optimizer Works

The optimizer uses a **memo table** to compactly represent the search space of equivalent query plans. Each **group** in the memo contains logically equivalent expressions (e.g., `A JOIN B` and `B JOIN A`). The search proceeds top-down:

1. **Explore**: Apply transformation rules (e.g., join commutativity) to generate all logically equivalent plans within each group.
2. **Implement**: Apply implementation rules (e.g., logical join -> hash join) to generate physical alternatives.
3. **Cost**: Use a pluggable cost model to compare physical alternatives and select the cheapest plan for each group.
4. **Extract**: Walk the memo to extract the overall cheapest plan.

## Prerequisites

### System Dependencies

| Dependency | Version | Install Command |
|-----------|---------|-----------------|
| **Rust** | 1.70+ | `curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \| sh` |
| **protoc** (Protocol Buffers compiler) | 3.x+ | `brew install protobuf` (macOS) or `apt install protobuf-compiler` (Ubuntu) |

After installing Rust, make sure `cargo` is on your PATH:

```bash
source "$HOME/.cargo/env"
```

### Verify Dependencies

```bash
# Check Rust
rustc --version    # Should be 1.70+
cargo --version

# Check protoc (required by the substrait crate)
protoc --version   # Should be 3.x+
```

## Building

```bash
cd presto-optimizer-rs

# Build all crates
cargo build

# Build in release mode (optimized)
cargo build --release
```

## Running Tests

```bash
# Run all tests
cargo test

# Run with output visible
cargo test -- --nocapture

# Run only the TPC-H Q5 integration test
cargo test -p optx-core --test tpch_q5 -- --nocapture

# Run only unit tests for a specific crate
cargo test -p optx-core
cargo test -p optx-rules
cargo test -p optx-substrait
```

## Running the Server

```bash
# Start the optimizer REST server on port 3000
cargo run -p optx-server

# Or in release mode
cargo run -p optx-server --release
```

### REST API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/rules` | GET | List active optimization rules |
| `/optimize` | POST | Optimize a Substrait plan (protobuf binary) |
| `/optimize/json` | POST | Optimize a Substrait plan (JSON) |
| `/rules/configure` | POST | Enable/disable rules |

### Example Usage

```bash
# Health check
curl http://localhost:3000/health

# List rules
curl http://localhost:3000/rules

# Optimize a plan (JSON format)
curl -X POST http://localhost:3000/optimize/json \
  -H "Content-Type: application/json" \
  -d '{ ... substrait plan JSON ... }'
```

## Crate Details

### optx-core

The core optimizer engine:
- **`memo.rs`** - Memo table: groups of equivalent expressions with deduplication
- **`expr.rs`** - Logical operators (Scan, Join, Filter, etc.) and physical operators (HashJoin, SeqScan, etc.)
- **`search.rs`** - Top-down Cascades search algorithm with memoization
- **`cost.rs`** - Pluggable cost model trait + default implementation
- **`stats.rs`** - Statistics structures and derivation (join selectivity, filter selectivity)
- **`rule.rs`** - Rule trait, registry, and plugin architecture
- **`pattern.rs`** - Declarative pattern matching for rules
- **`properties.rs`** - Logical and physical properties (sort order, distribution)
- **`catalog.rs`** - Schema/table metadata interface

### optx-rules

Built-in optimization rules:
- **Transformations**: Join commutativity, join associativity, predicate pushdown, projection pushdown
- **Implementations**: Hash join (with build-side selection), merge join, nested loop join, sequential scan, hash aggregate, stream aggregate, sort

### optx-substrait

Substrait integration:
- **`consumer.rs`** - Converts Substrait Plan protobuf to internal memo representation
- **`producer.rs`** - Converts optimized physical plan back to Substrait Plan
- **`extensions.rs`** - Custom statistics extensions for Substrait (encoded as protobuf `Any`)

### optx-server

Axum-based REST API with CORS support, health checks, and rule configuration.

## Rust Dependencies

All dependencies are managed via `Cargo.toml` and fetched automatically by `cargo build`:

| Crate | Version | Purpose |
|-------|---------|---------|
| `substrait` | 0.46 | Substrait protobuf definitions |
| `prost` | 0.13 | Protobuf serialization |
| `axum` | 0.8 | HTTP framework for REST API |
| `tokio` | 1 | Async runtime |
| `tower-http` | 0.6 | HTTP middleware (CORS, tracing) |
| `serde` / `serde_json` | 1 | JSON serialization |
| `ordered-float` | 4 | Hashable/orderable floats for cost comparison |
| `tracing` | 0.1 | Structured logging |
| `thiserror` | 2 | Error type derivation |

## TPC-H Q5 Demonstration

The `tests/tpch_q5.rs` integration test demonstrates the optimizer's ability to find better join orderings:

- **Suboptimal plan** (left-deep, query text order): Cost ~343M
- **Optimal plan** (smallest tables first): Cost ~188M
- **Improvement**: ~1.83x

The optimizer correctly identifies that joining tiny dimension tables (region: 5 rows, nation: 25 rows) first dramatically reduces intermediate result sizes.
