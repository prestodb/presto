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
 * Browser API mocks for testing
 * Provides mock implementations of browser APIs used throughout the application
 */

/**
 * Mock window.matchMedia
 * Used for responsive design and media query testing
 */
export const setupMatchMediaMock = () => {
    Object.defineProperty(window, "matchMedia", {
        writable: true,
        value: jest.fn().mockImplementation((query) => ({
            matches: false,
            media: query,
            onchange: null,
            addListener: jest.fn(),
            removeListener: jest.fn(),
            addEventListener: jest.fn(),
            removeEventListener: jest.fn(),
            dispatchEvent: jest.fn(),
        })),
    });
};

/**
 * NOTE: Fetch mocking is handled in apiMocks.ts
 * This keeps all HTTP/API mocking logic in one place.
 * Import setupFetchMock from apiMocks.ts if needed.
 */

/**
 * Mock highlight.js (hljs)
 * Used for syntax highlighting in code blocks
 */
export const setupHljsMock = () => {
    (global as any).hljs = {
        highlightBlock: jest.fn(),
        highlightElement: jest.fn(),
        highlight: jest.fn().mockReturnValue({ value: "" }),
    };
};

/**
 * Mock Clipboard API
 * Used for copy-to-clipboard functionality
 */
export const setupClipboardMock = () => {
    (global as any).Clipboard = jest.fn();
};

/**
 * Mock document.body extensions
 * Some legacy code may use non-standard document.body methods
 */
export const setupDocumentMocks = () => {
    if (!(document.body as any).createTextRange) {
        (document.body as any).createTextRange = jest.fn();
    }
};

/**
 * Setup console error suppression for known React warnings
 * Suppresses specific warnings that are expected in test environment
 */
export const setupConsoleErrorSuppression = () => {
    const originalError = console.error;

    beforeAll(() => {
        console.error = (...args: any[]) => {
            // Suppress specific React warnings that are expected in tests
            if (
                typeof args[0] === "string" &&
                (args[0].includes("Warning: ReactDOM.render") ||
                    args[0].includes("Warning: useLayoutEffect") ||
                    args[0].includes("Not implemented: HTMLFormElement.prototype.submit"))
            ) {
                return;
            }
            originalError.call(console, ...args);
        };
    });

    afterAll(() => {
        console.error = originalError;
    });
};

/**
 * Setup all browser mocks at once
 * Convenience function to initialize all browser API mocks
 *
 * NOTE: Fetch mocking is NOT included here - it's handled separately
 * in apiMocks.ts to keep all HTTP/API mocking logic centralized.
 */
export const setupAllBrowserMocks = () => {
    setupMatchMediaMock();
    setupHljsMock();
    setupClipboardMock();
    setupDocumentMocks();
    setupConsoleErrorSuppression();
};

// Made with Bob
