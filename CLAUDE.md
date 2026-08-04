# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
# Full build with tests
./mvnw clean install

# Build skipping tests (faster iteration)
./mvnw clean install -DskipTests

# Build skipping the UI (saves time when not touching frontend)
./mvnw clean install -DskipTests -DskipUI

# Build a single module
./mvnw clean install -pl presto-iceberg -DskipTests

# Build a module and its dependencies
./mvnw clean install -pl presto-iceberg -am -DskipTests
```

## Running Tests

Presto uses TestNG (not JUnit). Tests run in parallel (`methods` mode) with a 4GB JVM.

```bash
# Run all tests in a module
./mvnw test -pl presto-iceberg

# Run a specific test class
./mvnw test -pl presto-iceberg -Dtest=TestIcebergMetadataListing

# Run a specific test method
./mvnw test -pl presto-iceberg -Dtest=TestIcebergMetadataListing#testListTables

# Run tests matching a pattern
./mvnw test -pl presto-main -Dtest="TestSql*"
```

**TestNG-specific patterns:**
- Test classes use `@Test` annotations; lifecycle uses `@BeforeMethod`/`@AfterMethod`
- TestNG does NOT create a new object per test method — reinitialize mutable state in `@BeforeMethod` and annotate with `@Test(singleThreaded = true)` if the class uses instance fields
- Avoid `Thread.sleep` and random values in tests — tests must be reproducible

## Running the Server (IntelliJ)

Main class: `com.facebook.presto.server.PrestoServer`
Working directory: `presto-main/`
VM options:
```
-ea -XX:+UseG1GC -XX:G1HeapRegionSize=32M -XX:+ExplicitGCInvokesConcurrent -Xmx2G
-Dconfig=etc/config.properties -Dlog.levels-file=etc/log.properties
-Djdk.attach.allowAttachSelf=true
```
Also add `--add-opens` flags listed in README.md for Java 17 reflective access.

## Architecture Overview

Presto is a distributed SQL query engine with a coordinator + worker model. The Java codebase handles planning, optimization, and coordination; the C++ native execution layer (`presto-native-execution`, using Velox) handles physical evaluation in performance-critical deployments.

**Core query pipeline** (follow code in this order):
1. `presto-parser` — ANTLR 4 grammar, produces AST
2. `presto-analyzer` — semantic analysis, type checking, resolves references
3. `presto-main` — query planning, optimization (cost-based optimizer), scheduling, and coordinator logic; this is the largest module
4. `presto-spi` — plugin/connector interface (start here when adding a new connector)

**Key modules:**
- `presto-spi` — the plugin SPI; defines `Connector`, `ConnectorMetadata`, `ConnectorSplitManager`, `ConnectorPageSource`, `Type`, and `Plugin` interfaces
- `presto-main` — core engine: `QueryManager`, `SqlQueryExecution`, optimizer rules, `LocalExecutionPlanner`
- `presto-common` — shared utilities, `Page`/`Block` data structures (columnar format used at runtime)
- `presto-expressions` — row expression evaluation
- `presto-bytecode` — JVM bytecode generation for compiled expressions
- `presto-orc` / `presto-parquet` — file format readers
- `presto-hive` / `presto-iceberg` / `presto-delta` — table format connectors (built on `presto-hive-metastore`, `presto-hdfs-core`)
- `presto-native-execution` — C++ Prestissimo worker (Velox-based); separate build system

**Plugin model:** Connectors and other extensions are loaded via Java ServiceLoader. Each plugin JAR implements `Plugin` and is listed in `plugin.bundles` in `presto-main/etc/config.properties`. The SPI is the stable interface — never depend on `presto-main` internals from a connector.

**Data flow:** Queries arrive at the coordinator → parsed → analyzed → planned into a distributed plan → split into stages → tasks distributed to workers → workers execute operators over `Page` objects (columnar blocks) → results stream back.

## Code Style

Max line width: **180 characters** (IntelliJ "Reformat code" does not enforce this — adjust manually).

**Java conventions:**
- `requireNonNull(param, "param is null")` in every constructor
- Use `ImmutableList`/`ImmutableMap`/`ImmutableSet` (Guava); collect with `toImmutableList()` not `Collectors.toList()`
- Static-import constants and factory methods (`toImmutableList`, `NANOSECONDS`, `format`, etc.)
- `Optional` for nullable public method parameters — but not in performance-critical tight loops
- Fields before methods; members ordered public → protected → package-private → private; within each level, static final → final → normal
- Javadoc on all interface methods; `//` inline comments only for non-obvious implementation details
- Alphabetize: sections in docs, methods, variables

**Multi-line function declarations** (when > 180 chars): put each parameter on its own line, first parameter NOT on the same line as the method name:
```java
public Foo(
        ParamType param1,
        ParamType param2)
{
    ...
}
```

## Commit and PR Standards

All commits use [Conventional Commits](https://www.conventionalcommits.org/). The PR title becomes the squashed commit message.

**Format:** `<type>[(scope)]: <description>`

**Types:** `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`, `misc`

**Scopes:** `parser`, `analyzer`, `planner`, `spi`, `scheduler`, `connector`, `resource`, `security`, `function`, `type`, `expression`, `operator`, `client`, `server`, `native`, `testing`, `docs`, `build` — or any connector/plugin name prefixed with `plugin-` (e.g., `plugin-iceberg`, `plugin-hive`).

**Description rules:** Start with capital letter, imperative mood ("Add" not "Added"), no trailing period, 50–72 characters preferred.

**Breaking changes:** Append `!` after type/scope and include `BREAKING CHANGE:` footer in the body.

**Examples:**
```
feat(plugin-iceberg): Support ALTER COLUMN SET NOT NULL
fix: Resolve memory leak in query executor
feat!: Remove deprecated configuration options
```

All PRs are squashed to a single commit on merge ("Squash and merge"). Reference issues at the end of the body: `Resolves: #1234`.
