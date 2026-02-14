/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Global test setup file
 * This file is executed before each test file and sets up the testing environment
 */

import "@testing-library/jest-dom";
import { setupJQuery } from "./__tests__/mocks/jqueryMock";
import { setupAllBrowserMocks } from "./__tests__/mocks/browserMocks";
import { setupFetchMock } from "./__tests__/mocks/apiMocks";

// Setup all browser API mocks (matchMedia, hljs, clipboard, document)
setupAllBrowserMocks();

// Setup fetch mock (centralized in apiMocks.ts for all HTTP/API mocking)
setupFetchMock();

// Setup jQuery with selective mocking (uses real jQuery with mocked plugins)
setupJQuery();

// Suppress specific harmless Jest fake timer warnings
// These occur when test helpers call jest.advanceTimersByTime() but fake timers
// ARE properly set up in beforeEach. This is a known Jest limitation with async helpers.
//
// IMPORTANT: This override is scoped to Jest environment only and uses exact string matching
// to avoid interfering with tests that spy on or assert console.warn.
const originalWarn = console.warn;
const JEST_FAKE_TIMER_WARNING =
    "A function to advance timers was called but the timers APIs are not replaced with fake timers. Call `jest.useFakeTimers()` in this test file or enable fake timers for all tests by setting 'fakeTimers': {'enableGlobally': true} in Jest configuration file.";

// Only apply the filter in Jest environment (when JEST_WORKER_ID is set)
if (process.env.JEST_WORKER_ID !== undefined) {
    console.warn = (...args: any[]) => {
        const message = args[0]?.toString() || "";

        // Only suppress the Jest fake timer warning message (by checking if it starts with the warning)
        // Jest includes stack trace after the message, so we check the prefix
        // This is safe because:
        // 1. We match a specific, complete warning message prefix (254 chars)
        // 2. We only apply this in Jest environment (JEST_WORKER_ID check)
        // 3. Fake timers ARE properly configured in our tests
        // 4. This warning is a false positive from Jest's async helper interaction
        // 5. Tests that spy on console.warn can still work (they see the wrapper)
        if (message.startsWith(JEST_FAKE_TIMER_WARNING)) {
            return;
        }

        // Allow all other warnings to surface
        originalWarn.apply(console, args);
    };
}

// Made with Bob
