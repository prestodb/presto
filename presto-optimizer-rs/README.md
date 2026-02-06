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
# Start the optimizer REST server on port 3000 (default)
cargo run -p optx-server

# Start on a custom port
cargo run -p optx-server -- --port 8080
cargo run -p optx-server -- -p 8080

# In release mode (optimized)
cargo run -p optx-server --release
```

### REST API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/rules` | GET | List active optimization rules |
| `/optimize` | POST | Optimize a Substrait plan (protobuf binary) |
| `/optimize/json` | POST | Optimize a Substrait plan (JSON) |
| `/optimize/join-graph` | POST | Optimize join ordering via join-graph JSON protocol |
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

## Presto Integration (E2E Demo)

The optimizer integrates with Presto's Java coordinator via a **join-graph JSON protocol**. This is a simplified protocol for join reordering that communicates just the essential information (tables with statistics and equi-join conditions) rather than requiring full Substrait plan conversion.

### Architecture

```
Presto Java Coordinator
  |
  | 1. Extract join graph from PlanNode tree
  |    (tables with stats, equi-join conditions)
  |
  | 2. POST /optimize/join-graph (JSON)
  v
Rust Optimizer (optx-server:3000)
  |
  | 3. Build Memo from join graph
  | 4. Run Cascades search (explore + implement + cost)
  | 5. Extract best join tree
  |
  | 6. Response: optimized join tree (JSON)
  v
Presto Java Coordinator
  |
  | 7. Rebuild PlanNode join tree following
  |    the optimized order (reuse original
  |    TableScanNodes, ColumnHandles, etc.)
  v
Continue with remaining optimizers (AddExchanges, etc.)
```

### Join-Graph JSON Protocol

**Request: `POST /optimize/join-graph`**

```json
{
  "tables": [
    {
      "id": "t0",
      "schema": "tpch",
      "name": "customer",
      "rowCount": 150000.0,
      "sizeBytes": 15000000.0,
      "columns": [
        {"name": "c_custkey", "ndv": 150000.0, "nullFraction": 0.0, "avgSize": 8.0}
      ]
    }
  ],
  "joins": [
    {
      "leftTableId": "t0",
      "rightTableId": "t1",
      "joinType": "INNER",
      "leftColumn": "c_custkey",
      "rightColumn": "o_custkey"
    }
  ]
}
```

**Response:**

```json
{
  "tree": {
    "joinType": "INNER",
    "leftColumn": "c_custkey",
    "rightColumn": "o_custkey",
    "left": {
      "joinType": "INNER",
      "leftColumn": "n_regionkey",
      "rightColumn": "r_regionkey",
      "left": {"tableId": "t3"},
      "right": {"tableId": "t5"}
    },
    "right": {"tableId": "t0"}
  },
  "cost": 188000000.0,
  "optimized": true
}
```

### Running the E2E Demo

**1. Start the Rust optimizer:**

```bash
cd presto-optimizer-rs
cargo run -p optx-server
# Listening on http://0.0.0.0:3000
```

**2. Build and start Presto** (with TPC-H catalog configured):

```bash
cd ..
./mvnw clean install -DskipTests -DskipUI -pl :presto-main-base,:presto-main,:presto-server -am
# Start Presto coordinator
```

**3. Enable the optimizer and run EXPLAIN:**

```sql
-- Enable the Rust Cascades optimizer for this session
SET SESSION use_rust_cascade_optimizer = true;

-- Run EXPLAIN on a join-heavy TPC-H query (Q5)
EXPLAIN SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) as revenue
FROM customer, orders, lineitem, supplier, nation, region
WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
  AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
  AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
  AND r_name = 'ASIA'
  AND o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'
GROUP BY n_name ORDER BY revenue DESC;
```

**4. Compare with the default plan:**

```sql
SET SESSION use_rust_cascade_optimizer = false;
-- Same query — shows original join order (left-deep, query text order)
```

The optimized plan should show small tables (region=5 rows, nation=25 rows) joined first, with larger tables joined later against smaller intermediates.

### Session Properties

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `use_rust_cascade_optimizer` | boolean | `false` | Enable Rust Cascades optimizer for join reordering |
| `rust_cascade_optimizer_url` | string | `http://localhost:3000` | URL of the Rust optimizer service |

### Java Components

| File | Description |
|------|-------------|
| `RustCascadeOptimizer.java` | `PlanOptimizer` implementation with inner classes for extraction, HTTP client, and plan rebuilding |
| `FeaturesConfig.java` | Config properties (`optimizer.rust-cascade-enabled`, `optimizer.rust-cascade-url`) |
| `SystemSessionProperties.java` | Session-level property accessors |
| `PlanOptimizers.java` | Optimizer registration (runs after PhysicalCteOptimizer, before AddExchanges) |

### Rust Components

| File | Description |
|------|-------------|
| `optx-server/src/join_graph.rs` | JSON types, Memo construction, response builder, handler |
| `optx-server/src/main.rs` | Route registration for `/optimize/join-graph` |

### Running the Java Integration Test

The `TestRustCascadeOptimizer` test automatically spawns the Rust server, runs queries with the optimizer enabled, and verifies correctness against the default Presto optimizer.

**1. Build the Rust server binary:**

```bash
cd presto-optimizer-rs
cargo build -p optx-server
```

**2. Run the test:**

```bash
cd ..
./mvnw test -pl :presto-tests -am -Dtest=TestRustCascadeOptimizer \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dcheckstyle.skip=true -Dair.check.skip-all
```

The test starts the server on a random free port, waits for the `/health` endpoint, runs TPC-H queries (3-table, 4-table, and 6-table joins), and verifies the optimized plans produce identical results. If the binary is not found, the test is skipped.

To use an explicit binary path:

```bash
./mvnw test -pl :presto-tests -am -Dtest=TestRustCascadeOptimizer \
  -Doptx.server.binary=presto-optimizer-rs/target/release/optx-server \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dcheckstyle.skip=true -Dair.check.skip-all
```

### Known Limitations / Future Work

- **Substrait integration**: The join-graph protocol is a demo simplification. Production use should replace it with full Substrait PlanNode conversion, which requires mapping Presto's TableHandle/ColumnHandle/RowExpression types.
- **Equi-joins only**: Only single-column equi-join conditions are supported. Multi-column joins, non-equi predicates, and theta joins are not yet handled.
- **Inner joins only**: Outer join reordering is not yet supported — left/right/full outer joins have ordering constraints that must be respected.
- **Cyclic join graphs**: Join graphs with cycle edges (e.g., TPC-H Q5's `c.nationkey = s.nationkey` where both tables are already connected via other paths) fall back to the original plan. Cycle predicates would be lost when rebuilding the join tree.
- **No filter pushdown**: Table-level filter predicates are not communicated to the Rust optimizer, which limits cardinality estimation accuracy.
- **Graceful fallback**: On any failure (connection refused, timeout, error response, rebuild failure), the optimizer silently falls back to the original plan.

## Crate Details

### optx-core

The core optimizer engine:
- **`memo.rs`** - Memo table: groups of equivalent expressions with deduplication
- **`expr.rs`** - Logical operators (Scan, Join, Filter, etc.) and physical operators (HashJoin, SeqScan, etc.)
- **`search.rs`** - Top-down Cascades search algorithm with memoization
- **`cost.rs`** - Pluggable cost model trait + default implementation
- **`stats.rs`** - Statistics structures and derivation (join selectivity, filter selectivity)
- **`rule.rs`** - Rule trait (with `RuleResult`/`RuleChild` for creating new groups), registry, and plugin architecture
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
