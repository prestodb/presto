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
 * API Mocking Utilities
 * Provides helpers for mocking fetch API calls in tests
 *
 * NOTE: This file contains only mocking mechanisms (functions).
 * For test data, import from __tests__/fixtures:
 * - queryFixtures.js - Query objects
 * - infoFixtures.ts - Info/cluster data
 * - stageFixtures.ts - Stage data
 * - clusterFixtures.ts - Cluster data
 */

// ============================================================================
// Response Creation Helpers
// ============================================================================

/**
 * Create a successful fetch response object
 */
const createSuccessResponse = (data: any): Partial<Response> => ({
    ok: true,
    status: 200,
    json: async () => data,
    text: async () => JSON.stringify(data),
});

/**
 * Create a failed fetch response object
 */
const createErrorResponse = (status: number, message: string): Partial<Response> => ({
    ok: false,
    status,
    statusText: message,
    json: async () => {
        throw new Error(message);
    },
    text: async () => message,
});

// ============================================================================
// Fetch Mock Setup
// ============================================================================

/**
 * Initialize fetch mock (call in beforeEach)
 */
export const setupFetchMock = () => {
    global.fetch = jest.fn();
};

/**
 * Reset fetch mock (call in afterEach)
 */
export const resetFetchMock = () => {
    (global.fetch as jest.Mock).mockReset();
};

// ============================================================================
// URL-Based Mocking (Recommended)
// ============================================================================

/**
 * Mock fetch responses for specific URLs
 * @param urlMap - Map of URL to response data
 * @example
 * mockFetchByUrl({
 *   "/v1/query": [mockQuery],
 *   "/v1/info": mockInfo,
 * });
 */
export const mockFetchByUrl = (urlMap: Record<string, any>) => {
    (global.fetch as jest.Mock).mockImplementation((url: string) => {
        const data = urlMap[url];
        if (data !== undefined) {
            return Promise.resolve(createSuccessResponse(data));
        }
        // Return empty success response for unmapped URLs
        return Promise.resolve(createSuccessResponse({}));
    });
};

/**
 * Mock fetch error for a specific URL
 * @param url - The URL to mock
 * @param status - HTTP status code (default: 500)
 * @param message - Error message (default: "Internal Server Error")
 */
export const mockFetchErrorByUrl = (url: string, status: number = 500, message: string = "Internal Server Error") => {
    (global.fetch as jest.Mock).mockImplementation((fetchUrl: string) => {
        if (fetchUrl === url) {
            return Promise.resolve(createErrorResponse(status, message));
        }
        // Return success for other URLs
        return Promise.resolve(createSuccessResponse({}));
    });
};

// ============================================================================
// Legacy Single-Call Mocking (For Backward Compatibility)
// ============================================================================

/**
 * Mock a single successful fetch call (legacy)
 * @deprecated Use mockFetchByUrl for better control
 */
export const mockFetchSuccess = (data: any) => {
    (global.fetch as jest.Mock).mockResolvedValueOnce(createSuccessResponse(data));
};

/**
 * Mock a single failed fetch call (legacy)
 * @deprecated Use mockFetchErrorByUrl for better control
 */
export const mockFetchError = (message: string, _status: number = 500) => {
    (global.fetch as jest.Mock).mockRejectedValueOnce(new Error(message));
};

/**
 * Mock fetch with custom response (legacy)
 * @deprecated Use mockFetchByUrl or create custom implementation
 */
export const mockFetchResponse = (response: Partial<Response>) => {
    (global.fetch as jest.Mock).mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({}),
        text: async () => "",
        ...response,
    });
};

// Made with Bob
