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
 * Component-Specific Setup Helpers
 * Provides reusable setup patterns for common test scenarios
 */

import { setupAllBrowserMocks } from "../mocks";

// ============================================================================
// Common Test Setup Patterns
// ============================================================================

/**
 * Setup fake timers with automatic cleanup
 * Use in describe block to apply to all tests
 *
 * @example
 * describe('MyComponent', () => {
 *     setupFakeTimers();
 *
 *     it('handles debounced input', () => {
 *         // timers are automatically set up and cleaned up
 *     });
 * });
 */
export const setupFakeTimers = () => {
    beforeEach(() => {
        jest.useFakeTimers();
    });

    afterEach(() => {
        jest.useRealTimers();
    });
};

/**
 * Setup common mocks for all tests
 * Clears mocks and sets up browser APIs
 *
 * @example
 * describe('MyComponent', () => {
 *     setupCommonMocks();
 *
 *     it('renders correctly', () => {
 *         // mocks are ready to use
 *     });
 * });
 */
export const setupCommonMocks = () => {
    beforeEach(() => {
        jest.clearAllMocks();
        setupAllBrowserMocks();
    });
};

/**
 * Setup for integration tests
 * Combines fake timers and common mocks
 *
 * @example
 * describe('MyComponent Integration', () => {
 *     setupIntegrationTest();
 *
 *     it('handles user interactions', async () => {
 *         // ready for integration testing
 *     });
 * });
 */
export const setupIntegrationTest = () => {
    beforeEach(() => {
        jest.useFakeTimers();
        jest.clearAllMocks();
    });

    afterEach(() => {
        jest.useRealTimers();
    });
};

// ============================================================================
// Custom Setup Builders
// ============================================================================

// Made with Bob
