# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PrestoDB is an open-source distributed SQL query engine for fast analytics against data sources of all sizes. It follows a coordinator + worker node architecture. The project includes ~124 Maven modules covering the core engine, storage connectors, a native C++ execution engine (Prestissimo via Velox), a React-based UI, and CLI client.

## Build Commands

**Java 17 and Maven 3.6.3+ are required.** The project uses Maven Wrapper (`./mvnw`).

```bash
# Full build (skip tests for speed)
./mvnw clean install -DskipTests

# Build skipping UI (avoids Node.js/Yarn)
./mvnw clean install -DskipTests -DskipUI

# Build with checks (CI profile)
./mvnw install -B -V -T 1C -DskipTests -Dmaven.javadoc.skip=true -P ci -pl '!presto-test-coverage,!:presto-docs'

# Build a single module
./mvnw clean install -DskipTests -pl :presto-main
```

## Running Tests

```bash
# Run tests for a specific module
./mvnw test -pl :presto-main

# Run a specific test class
./mvnw test -pl :presto-main -Dtest=TestClassName

# Run a specific test method
./mvnw test -pl :presto-main -Dtest=TestClassName#testMethodName

# Run with CI flags (skip checks, fail at end)
./mvnw test -B -Dair.check.skip-all -Dmaven.javadoc.skip=true --no-transfer-progress --fail-at-end -pl :presto-tests

# Common test profiles for presto-tests module
./mvnw test -pl :presto-tests -P presto-tests-general
./mvnw test -pl :presto-tests -P presto-tests-execution-memory
./mvnw test -pl :presto-tests -P ci-only-distributed-queries
./mvnw test -pl :presto-tests -P ci-only-tpch-distributed-queries
```

Test framework: TestNG + AssertJ. Default test timezone: `America/Bahia_Banderas`.

## Native Execution (Prestissimo/C++)

```bash
cd presto-native-execution
make submodules
# OS setup: ./scripts/setup-macos.sh or ./scripts/setup-ubuntu.sh
make              # Release build
make debug        # Debug build
make unittest     # Build and run C++ tests
```

C++ code follows Velox coding standards with clang-format/clang-tidy.

## Code Style

- **Checkstyle** config: `src/checkstyle/presto-checks.xml`
- Spaces only (no tabs), LF line endings, no trailing whitespace, no consecutive blank lines
- Max line width: 180 characters; lines exceeding this must break with one arg per line
- Static imports required for: `Objects.requireNonNull`, `Math.toIntExact`
- Static imports banned for: `of`, `valueOf`, `copyOf`, `all`, `none`
- Only `java.lang.String.format` can be statically imported as `format()`
- IDE code style template: https://github.com/airlift/codestyle

## Commit Message Format (Conventional Commits)

```
<type>[(<scope>)]: <description>
```

- **Types:** feat, fix, docs, refactor, perf, test, build, ci, chore, revert, misc
- **Scopes:** parser, analyzer, planner, spi, scheduler, protocol, connector, resource, security, function, type, expression, operator, client, server, native, testing, docs, build, ci, ui, deps, plugin-*
- Description must start with a capital letter and not end with a period
- All PRs are squash-merged using the PR title as the commit message

## Architecture

### Key Module Groups

- **Core Engine:** `presto-spi` (plugin interface contracts), `presto-main-base`, `presto-main` (coordinator/worker runtime), `presto-server`
- **SQL Processing Pipeline:** `presto-parser` (ANTLR grammar) → `presto-analyzer` → `presto-planner` → execution
- **Connectors:** `presto-hive`, `presto-iceberg`, `presto-delta`, `presto-kafka`, `presto-bigquery`, `presto-clickhouse`, etc. Each connector implements the SPI interfaces from `presto-spi`
- **Native Execution:** `presto-native-execution` — C++ query execution engine (Prestissimo) using the Velox library, replacing Java-based evaluation for performance
- **Client/UI:** `presto-cli` (command-line client), `presto-client` (Java client library), `presto-ui` (React web console)
- **Testing:** `presto-tests` (integration tests with profiles), `presto-product-tests` (Docker-based E2E using Tempto), `presto-native-tests`
- **Spark Integration:** `presto-spark-base`, `presto-spark-launcher`, `presto-spark-classloader-interface`

### SPI (Service Provider Interface)

`presto-spi` defines the contracts that all plugins/connectors implement. Changes here affect every connector. Key interfaces include `ConnectorFactory`, `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorRecordSetProvider`, and `ConnectorPageSourceProvider`.

### Query Lifecycle

SQL text → Parser (ANTLR) → Analyzer (semantic analysis, type checking) → Planner (logical plan, optimizer rules, physical plan) → Scheduler (distributes work to workers) → Execution (local or native/Velox) → Results

## Running Presto Locally

Main class: `com.facebook.presto.server.PrestoServer`
VM options: `-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+UseGCOverheadLimit -XX:+ExitOnOutOfMemoryError -Xmx2G -Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties -Djdk.attach.allowAttachSelf=true`
Working directory: `presto-main`

## Pre-commit Hooks (Native/CI files)

```bash
pip install pre-commit
pre-commit install --allow-missing-config
pre-commit run        # Changed files only
pre-commit run -a     # All files
```

Hooks enforce: clang-format (C++), license headers, trailing whitespace, YAML/shell formatting, GitHub Actions validation.
