# Contributing to Presto LanceDB Connector

Thank you for your interest in contributing to the Presto LanceDB connector! This document outlines development guidelines, compilation procedures, testing, and formatting requirements.

## Prerequisites

- **Java Development Kit (JDK):** Java 17 (matching the Presto project configuration).
- **Apache Maven:** Maven 3.6.0+ is required.

---

## Build Commands

Compile the `presto-lance` module along with its required dependency projects using the following Maven command:

```bash
mvn clean package -pl presto-lance -am -DskipTests
```

---

## Running Unit Tests

To run the unit tests of the `presto-lance` module:

```bash
mvn test -pl presto-lance
```

*Note: The test suite includes unit tests that instantiate embedded Arrow dataset allocations and local filesystem databases under `target/test-data`. Ensure your environment has sufficient temporary file creation permissions.*

---

## Code Quality & Formatting

Presto maintains strict styling and code-quality rules. Make sure to run these checks before submitting a pull request:

### 1. Checkstyle
Ensure code style complies with Presto standards:
```bash
mvn checkstyle:check -pl presto-lance
```

### 2. License Headers
Verify all source code files have proper license headers:
```bash
mvn license:check -pl presto-lance
```
To automatically apply/format the license headers:
```bash
mvn license:format -pl presto-lance
```

### 3. Code Style Guidelines
- **Imports order:** Imports should be sorted alphabetically, with static imports grouped separately.
- **Null Checks:** Always use `java.util.Objects.requireNonNull` for mandatory fields in constructor injections.
- **Error Codes:** Define new error conditions in `LanceErrorCode.java` mapping to appropriate Presto error categorizations.
