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
 * Barrel export for all test mocks
 * Import all mocks from a single location for convenience
 *
 * @example
 * import { mockJQueryGet, mockFetchByUrl, setupAllBrowserMocks } from '../__tests__/mocks';
 */

// jQuery mocks
export * from "./jqueryMock";

// API mocks (fetch) - Centralized in apiMocks.ts
export {
    mockFetchByUrl,
    mockFetchErrorByUrl,
    mockFetchSuccess,
    mockFetchError,
    mockFetchResponse,
    setupFetchMock,
    resetFetchMock,
} from "./apiMocks";

// Browser API mocks (matchMedia, hljs, clipboard, document, console)
// Note: Fetch mocking is NOT included here - it's in apiMocks.ts
export {
    setupMatchMediaMock,
    setupHljsMock,
    setupClipboardMock,
    setupDocumentMocks,
    setupConsoleErrorSuppression,
    setupAllBrowserMocks,
} from "./browserMocks";

// Made with Bob
