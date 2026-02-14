# Presto UI Testing Guide

This guide explains the testing infrastructure and patterns used in the Presto UI project.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Test Structure](#test-structure)
3. [Test Utilities](#test-utilities)
4. [Setup Helpers](#setup-helpers)
5. [Mocks](#mocks)
6. [Fixtures](#fixtures)
7. [Testing Patterns](#testing-patterns)
8. [Configuration](#configuration)
9. [Best Practices](#best-practices)

## Quick Start

### Running Tests

```bash
# Run all tests
yarn test

# Run tests in watch mode
yarn test:watch

# Run tests with coverage
yarn test:coverage

# Run specific test file
yarn test QueryList.test.jsx
```

### Writing Your First Test

```jsx
import { render, screen } from "../__tests__/utils/testUtils";
import { setupCommonMocks } from "../__tests__/utils/setupHelpers";
import { createMockQuery } from "../__tests__/fixtures";
import MyComponent from "./MyComponent";

describe("MyComponent", () => {
    setupCommonMocks();

    it("renders correctly", () => {
        const query = createMockQuery();
        render(<MyComponent query={query} />);
        expect(screen.getByText(/expected text/i)).toBeInTheDocument();
    });
});
```

## Test Structure

### Directory Layout

```
src/
├── __tests__/
│   ├── fixtures/          # Test data factories
│   │   ├── index.ts       # Barrel export
│   │   ├── queryFixtures.js
│   │   ├── infoFixtures.ts
│   │   ├── stageFixtures.ts
│   │   └── clusterFixtures.ts
│   ├── mocks/             # Mocking utilities
│   │   ├── index.ts       # Barrel export
│   │   ├── apiMocks.ts    # Fetch API mocks
│   │   ├── jqueryMock.ts  # jQuery mocks
│   │   └── browserMocks.ts # Browser API mocks
│   ├── utils/             # Test utilities
│   │   ├── testUtils.tsx  # Common test helpers
│   │   └── setupHelpers.ts # Setup patterns
│   └── README.md          # This file
├── components/
│   └── MyComponent.test.tsx
└── router/
    └── QueryList.test.jsx
```

## Test Utilities

The `testUtils.tsx` file provides common helpers for testing. Import from `__tests__/utils/testUtils`.

### Loading State Helpers

```javascript
import { waitForLoadingToFinish, expectLoading } from "../__tests__/utils/testUtils";

// Wait for loading to finish
render(<Component />);
await waitForLoadingToFinish();

// Assert loading is present
render(<Component />);
expectLoading();
```

### Text Content Matchers

```javascript
import { findByTextContent, getByTextContent, queryByTextContent } from "../__tests__/utils/testUtils";

// Find by exact text content (async)
const element = await findByTextContent("Total: 100");

// Get by exact text content (sync)
const element = getByTextContent("Total: 100");

// Query by exact text content (returns null if not found)
const element = queryByTextContent("Total: 100");
```

### Dropdown Helpers

```javascript
import { clickDropdownOption, selectDropdownByRole } from "../__tests__/utils/testUtils";

// Click dropdown and select option
await clickDropdownOption("Sort By", "Name");

// Select by role
await selectDropdownByRole("combobox", "Option 1");
```

### Form Helpers

```javascript
import {
    typeIntoInput,
    setInputValue,
    clickButton,
    clickButtonSync,
    appendToInput,
} from "../__tests__/utils/testUtils";

// Type into input (clears first, realistic typing)
await typeIntoInput(/search/i, "test query");

// Set input value directly (faster, for non-interactive tests)
setInputValue(/search/i, "test query");

// Click button (realistic interaction)
await clickButton(/submit/i);

// Click button synchronously (faster, for non-interactive tests)
clickButtonSync(/submit/i);

// Append to input (doesn't clear)
await appendToInput(/search/i, " more text");
```

**When to use async vs sync helpers:**

- Use **async helpers** (`clickButton`, `typeIntoInput`) when testing user interactions that need realistic behavior
- Use **sync helpers** (`clickButtonSync`, `setInputValue`) for faster tests that don't need interaction simulation

### Timer Helpers

```javascript
import { advanceTimersAndWait, typeWithDebounce } from "../__tests__/utils/testUtils";

// Advance timers and wait for updates
await typeIntoInput(/search/i, "test");
await advanceTimersAndWait(300); // Wait for debounce

// Type with automatic debounce handling
const input = screen.getByPlaceholderText(/search/i);
await typeWithDebounce(input, "test", 300);
```

### Interaction Helpers

```javascript
import { clickAndWait, clickAndWaitForRemoval } from "../__tests__/utils/testUtils";

// Click and wait for text to appear
await clickAndWait(button, /success/i);

// Click and wait for element to disappear
await clickAndWaitForRemoval(deleteButton);
```

## Setup Helpers

The `setupHelpers.ts` file provides component-specific setup patterns. Import from `__tests__/utils/setupHelpers`.

### QueryList Setup

```javascript
import { setupQueryListTest } from "../__tests__/utils/setupHelpers";
import { createRunningQuery, createFinishedQuery } from "../__tests__/fixtures";

const { mockQueries } = setupQueryListTest([
    createRunningQuery({ queryId: "q1" }),
    createFinishedQuery({ queryId: "q2" }),
]);

render(<QueryList />);
```

### QueryDetail Setup

```javascript
import { setupQueryDetailTest } from "../__tests__/utils/setupHelpers";
import { createMockQuery } from "../__tests__/fixtures";

const { mockQuery } = setupQueryDetailTest(createMockQuery({ queryId: "test_123" }), { includeStages: true });

render(<QueryDetail queryId="test_123" />);
```

### ClusterHUD Setup

```javascript
import { setupClusterHUDTest } from "../__tests__/utils/setupHelpers";

const { clusterData } = setupClusterHUDTest({
    clusterData: { runningQueries: 10 },
    workerData: [{ workerId: "w1", state: "ACTIVE" }],
});

render(<ClusterHUD />);
```

### Common Setup Patterns

```javascript
import { setupCommonMocks, setupFakeTimers, setupIntegrationTest } from "../__tests__/utils/setupHelpers";

// Setup common mocks (use in describe block)
describe("MyComponent", () => {
    setupCommonMocks();

    it("test", () => {
        /* ... */
    });
});

// Setup fake timers (use in describe block)
describe("MyComponent", () => {
    setupFakeTimers();

    it("test with timers", () => {
        /* ... */
    });
});

// Setup for integration tests (combines both)
describe("MyComponent Integration", () => {
    setupIntegrationTest();

    it("integration test", async () => {
        /* ... */
    });
});
```

## Mocks

Mocks simulate external dependencies. Import from `__tests__/mocks`.

### jQuery Mocks

```javascript
import { mockJQueryGet, mockJQueryAjax } from "../__tests__/mocks";

// Mock $.get
mockJQueryGet("/api/query", { queryId: "123", state: "RUNNING" });

// Mock $.ajax
mockJQueryAjax({
    url: "/api/query",
    success: (data) => data,
    error: (xhr, status, error) => error,
});
```

### Fetch API Mocks

```javascript
import { mockFetchByUrl } from "../__tests__/mocks";
import { createMockQuery } from "../__tests__/fixtures";

mockFetchByUrl({
    "/v1/query": [createMockQuery()],
    "/v1/info": { runningQueries: 5 },
});
```

### Browser API Mocks

```javascript
import { setupAllBrowserMocks } from "../__tests__/mocks";

beforeEach(() => {
    setupAllBrowserMocks();
});
```

## Fixtures

Fixtures provide test data with sensible defaults. Import from `__tests__/fixtures`.

### Query Fixtures

```javascript
import { createMockQuery, createRunningQuery, createFinishedQuery, createFailedQuery } from "../__tests__/fixtures";

// Basic query
const query = createMockQuery();

// Running query with overrides
const runningQuery = createRunningQuery({
    queryId: "custom_123",
    query: "SELECT * FROM users",
});

// Finished query
const finishedQuery = createFinishedQuery({
    elapsedTime: "5.2s",
});

// Failed query
const failedQuery = createFailedQuery({
    errorType: "USER_ERROR",
    errorCode: { name: "SYNTAX_ERROR" },
});
```

### Other Fixtures

```javascript
import { createMockInfo, createMockStage, createMockCluster } from "../__tests__/fixtures";

const info = createMockInfo({ runningQueries: 10 });
const stage = createMockStage({ stageId: "0" });
const cluster = createMockCluster({ activeWorkers: 5 });
```

## Testing Patterns

### Unit Testing

```jsx
import { render, screen } from "../__tests__/utils/testUtils";
import { setupCommonMocks } from "../__tests__/utils/setupHelpers";
import { createMockQuery } from "../__tests__/fixtures";
import QueryListItem from "./QueryListItem";

describe("QueryListItem", () => {
    setupCommonMocks();

    it("displays query ID", () => {
        const query = createMockQuery({ queryId: "test_123" });
        render(<QueryListItem query={query} />);
        expect(screen.getByText("test_123")).toBeInTheDocument();
    });
});
```

### Integration Testing

```jsx
import { render, screen } from "../__tests__/utils/testUtils";
import { setupIntegrationTest, setupQueryListTest } from "../__tests__/utils/setupHelpers";
import { clickButton } from "../__tests__/utils/testUtils";
import { createRunningQuery, createFinishedQuery } from "../__tests__/fixtures";
import QueryList from "./QueryList";

describe("QueryList Integration", () => {
    setupIntegrationTest();

    it("filters queries by state", async () => {
        setupQueryListTest([createRunningQuery({ queryId: "q1" }), createFinishedQuery({ queryId: "q2" })]);

        render(<QueryList />);
        await clickButton(/FINISHED/i);

        expect(screen.getByText("q2")).toBeInTheDocument();
        expect(screen.queryByText("q1")).not.toBeInTheDocument();
    });
});
```

### Testing with Timers

```jsx
import { render, screen } from "../__tests__/utils/testUtils";
import { setupFakeTimers } from "../__tests__/utils/setupHelpers";
import { typeIntoInput, advanceTimersAndWait } from "../__tests__/utils/testUtils";

describe("SearchComponent", () => {
    setupFakeTimers();

    it("debounces search input", async () => {
        render(<SearchComponent />);

        await typeIntoInput(/search/i, "test");
        await advanceTimersAndWait(300);

        expect(screen.getByText(/results for "test"/i)).toBeInTheDocument();
    });
});
```

## Configuration

### Warning Suppression

The test setup automatically suppresses harmless Jest fake timer warnings in `setupTests.ts`:

```typescript
// Suppress harmless fake timer warnings
// These occur when async helpers call jest.advanceTimersByTime()
// but fake timers ARE properly set up in beforeEach
const originalWarn = console.warn;
console.warn = (...args: any[]) => {
    const message = args[0]?.toString() || "";

    // Suppress fake timer warnings - they're harmless
    if (message.includes("A function to advance timers was called but the timers APIs are not replaced")) {
        return;
    }

    // Allow all other warnings
    originalWarn.apply(console, args);
};
```

This ensures clean test output without suppressing legitimate warnings.

### Test Environment

- **Jest 29.7.0** with ts-jest and babel-jest
- **React Testing Library 16.1.0** for component testing
- **@testing-library/user-event 14.5.2** for user interaction simulation
- **Fake Timers** enabled by default for timer-dependent tests
- **ESLint + Prettier** for code quality and formatting

## Best Practices

### 1. Use Setup Helpers

✅ **Good**: Use setup helpers

```javascript
describe("QueryList", () => {
    setupCommonMocks();

    it("test", () => {
        setupQueryListTest([createMockQuery()]);
        render(<QueryList />);
    });
});
```

❌ **Bad**: Manual setup

```javascript
it("test", () => {
    jest.clearAllMocks();
    setupAllBrowserMocks();
    mockFetchByUrl({ "/v1/query": [createMockQuery()] });
    render(<QueryList />);
});
```

### 2. Use Test Utilities

✅ **Good**: Use helper functions

```javascript
await clickButton(/submit/i);
await typeIntoInput(/search/i, "test");
```

❌ **Bad**: Manual interactions

```javascript
const button = screen.getByRole("button", { name: /submit/i });
await userEvent.click(button);
const input = screen.getByPlaceholderText(/search/i);
await userEvent.type(input, "test");
```

### 3. Use Fixtures

✅ **Good**: Use fixtures

```javascript
const query = createRunningQuery({ queryId: "test" });
```

❌ **Bad**: Manual objects

```javascript
const query = {
    queryId: "test",
    state: "RUNNING",
    // ... 50 more properties
};
```

### 4. Test User Behavior

✅ **Good**: Test what users see

```javascript
expect(screen.getByText("RUNNING")).toBeInTheDocument();
```

❌ **Bad**: Test implementation

```javascript
expect(component.state.queries[0].state).toBe("RUNNING");
```

### 5. Choose Appropriate Helpers

✅ **Good**: Use sync helpers for speed when interaction realism isn't needed

```javascript
clickButtonSync(/submit/i);
setInputValue(/search/i, "test");
await advanceTimersAndWait(300);
```

❌ **Bad**: Using slow async helpers unnecessarily

```javascript
await clickButton(/submit/i); // Slower, not needed for simple state changes
await typeIntoInput(/search/i, "test"); // Much slower
```

**Rule of thumb**: Use sync helpers (`clickButtonSync`, `setInputValue`) unless you're specifically testing user interaction behavior.

### 6. Descriptive Test Names

✅ **Good**: Clear intent

```javascript
it("displays error message when API call fails", () => {
    // test
});
```

❌ **Bad**: Vague description

```javascript
it("works", () => {
    // test
});
```

## Example: Before vs After

### Before (Without Helpers)

```javascript
it("filters queries by state", async () => {
    const queries = [createRunningQuery({ queryId: "q1" }), createFinishedQuery({ queryId: "q2" })];

    jest.clearAllMocks();
    setupAllBrowserMocks();
    mockFetchByUrl({ "/v1/query": queries });

    render(<QueryList />);

    const finishedButton = screen.getByRole("button", { name: /FINISHED/i });
    await userEvent.click(finishedButton);

    expect(screen.getByText("q2")).toBeInTheDocument();
    expect(screen.queryByText("q1")).not.toBeInTheDocument();
});
```

### After (With Helpers)

```javascript
describe("QueryList", () => {
    setupCommonMocks();

    it("filters queries by state", async () => {
        setupQueryListTest([createRunningQuery({ queryId: "q1" }), createFinishedQuery({ queryId: "q2" })]);

        render(<QueryList />);
        await clickButton(/FINISHED/i);

        expect(screen.getByText("q2")).toBeInTheDocument();
        expect(screen.queryByText("q1")).not.toBeInTheDocument();
    });
});
```

**Improvements:**

- 30% less code
- Clearer intent
- Easier to maintain
- Reusable patterns

## Additional Resources

- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [React Testing Library](https://testing-library.com/docs/react-testing-library/intro/)
- [Testing Library Queries](https://testing-library.com/docs/queries/about)

---

**Made with Bob** 🤖
