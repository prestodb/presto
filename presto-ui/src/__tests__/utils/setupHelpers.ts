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

import { mockFetchByUrl } from "../mocks";
import { createMockQuery, createMockInfo, createMockStage, createMockCluster } from "../fixtures";
import { setupAllBrowserMocks } from "../mocks";

// ============================================================================
// Type Definitions
// ============================================================================

interface QueryListTestOptions {
    includeInfo?: boolean;
    infoData?: Record<string, any>;
    additionalUrls?: Record<string, any>;
}

interface QueryDetailTestOptions {
    includeStages?: boolean;
    stagesData?: any[];
    additionalUrls?: Record<string, any>;
}

interface ClusterHUDTestOptions {
    clusterData?: Record<string, any>;
    workerData?: any[];
    additionalUrls?: Record<string, any>;
}

// ============================================================================
// QueryList Component Setup
// ============================================================================

/**
 * Setup for QueryList component tests
 * Configures fetch mocks for query list and optional info endpoint
 *
 * @param queries - Array of query objects (uses default if empty)
 * @param options - Configuration options
 * @returns Object with mockQueries for assertions
 *
 * @example
 * const { mockQueries } = setupQueryListTest([
 *     createRunningQuery({ queryId: 'q1' }),
 *     createFinishedQuery({ queryId: 'q2' }),
 * ]);
 * render(<QueryList />);
 */
export const setupQueryListTest = (queries: any[] = [], options: QueryListTestOptions = {}) => {
    const { includeInfo = true, infoData = {}, additionalUrls = {} } = options;

    const mockQueries = queries.length > 0 ? queries : [createMockQuery()];

    const urlMap: Record<string, any> = {
        "/v1/query": mockQueries,
        ...additionalUrls,
    };

    if (includeInfo) {
        urlMap["/v1/info"] = createMockInfo(infoData);
    }

    mockFetchByUrl(urlMap);

    return { mockQueries };
};

// ============================================================================
// QueryDetail Component Setup
// ============================================================================

/**
 * Setup for QueryDetail component tests
 * Configures fetch mocks for query detail and optional stages
 *
 * @param query - Query object (uses default if null)
 * @param options - Configuration options
 * @returns Object with mockQuery for assertions
 *
 * @example
 * const { mockQuery } = setupQueryDetailTest(
 *     createRunningQuery({ queryId: 'test_123' }),
 *     { includeStages: true }
 * );
 * render(<QueryDetail queryId="test_123" />);
 */
export const setupQueryDetailTest = (query: any = null, options: QueryDetailTestOptions = {}) => {
    const { includeStages = true, stagesData = [], additionalUrls = {} } = options;

    const mockQuery = query || createMockQuery();

    const urlMap: Record<string, any> = {
        [`/v1/query/${mockQuery.queryId}`]: mockQuery,
        ...additionalUrls,
    };

    if (includeStages) {
        const stages = stagesData.length > 0 ? stagesData : [createMockStage()];
        urlMap[`/v1/query/${mockQuery.queryId}/stages`] = stages;
    }

    mockFetchByUrl(urlMap);

    return { mockQuery };
};

// ============================================================================
// ClusterHUD Component Setup
// ============================================================================

/**
 * Setup for ClusterHUD component tests
 * Configures fetch mocks for cluster and worker data
 *
 * @param options - Configuration options
 * @returns Object with clusterData and workerData for assertions
 *
 * @example
 * const { clusterData } = setupClusterHUDTest({
 *     clusterData: { runningQueries: 10 },
 *     workerData: [{ workerId: 'w1', state: 'ACTIVE' }],
 * });
 * render(<ClusterHUD />);
 */
export const setupClusterHUDTest = (options: ClusterHUDTestOptions = {}) => {
    const { clusterData = {}, workerData = [], additionalUrls = {} } = options;

    const mockClusterData = createMockCluster(clusterData);

    const urlMap: Record<string, any> = {
        "/v1/cluster": mockClusterData,
        "/v1/worker": workerData,
        ...additionalUrls,
    };

    mockFetchByUrl(urlMap);

    return { clusterData: mockClusterData, workerData };
};

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

/**
 * Create a custom setup function for a specific component
 * Useful for components with unique setup requirements
 *
 * @param setupFn - Function that performs setup
 * @returns Setup function that can be called in describe blocks
 *
 * @example
 * const setupMyComponent = createCustomSetup(() => {
 *     mockFetchByUrl({ '/api/data': mockData });
 *     setupAllBrowserMocks();
 * });
 *
 * describe('MyComponent', () => {
 *     setupMyComponent();
 *     // tests...
 * });
 */
export const createCustomSetup = (setupFn: () => void) => {
    return () => {
        beforeEach(() => {
            setupFn();
        });
    };
};

/**
 * Create a setup function with cleanup
 *
 * @param setupFn - Function that performs setup
 * @param cleanupFn - Function that performs cleanup
 * @returns Setup function with automatic cleanup
 *
 * @example
 * const setupWithCleanup = createSetupWithCleanup(
 *     () => { console.log('setup'); },
 *     () => { console.log('cleanup'); }
 * );
 */
export const createSetupWithCleanup = (setupFn: () => void, cleanupFn: () => void) => {
    return () => {
        beforeEach(() => {
            setupFn();
        });

        afterEach(() => {
            cleanupFn();
        });
    };
};

// Made with Bob
