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
 * Shared test fixtures for query-related tests
 * Provides factory functions to create mock query objects with sensible defaults
 */

/**
 * Base mock query object with all required fields
 */
export const baseMockQuery = {
    queryId: "test_query_123",
    state: "RUNNING",
    query: "SELECT * FROM table WHERE id = 1",
    session: {
        user: "testuser",
        source: "presto-ui",
    },
    queryStats: {
        createTime: "2024-01-01T10:00:00.000Z",
        elapsedTime: "5.2s",
        executionTime: "4.8s",
        totalCpuTime: "10.5s",
        totalDrivers: 15,
        queuedDrivers: 0,
        runningDrivers: 5,
        completedDrivers: 10,
        cumulativeUserMemory: 1073741824,
        userMemoryReservation: "1GB",
        totalMemoryReservation: "2GB",
        peakUserMemoryReservation: "1.5GB",
        peakTotalMemoryReservation: "1.5GB",
        rawInputDataSize: "500MB",
        rawInputPositions: 1000000,
    },
    errorType: null,
    errorCode: null,
    coordinatorUri: "",
    scheduled: true,
    fullyBlocked: false,
    blockedReasons: [],
    memoryPool: "general",
};

/**
 * Create a mock query with custom overrides
 * @param {Object} overrides - Properties to override in the base query
 * @returns {Object} Mock query object
 * @example
 * const finishedQuery = createMockQuery({ state: "FINISHED", queryId: "query_456" });
 */
export const createMockQuery = (overrides = {}) => {
    // Deep merge for nested objects like queryStats
    const merged = { ...baseMockQuery, ...overrides };
    if (overrides.queryStats) {
        merged.queryStats = { ...baseMockQuery.queryStats, ...overrides.queryStats };
    }
    if (overrides.session) {
        merged.session = { ...baseMockQuery.session, ...overrides.session };
    }
    return merged;
};

/**
 * Create a mock query in RUNNING state
 */
export const createRunningQuery = (overrides = {}) => createMockQuery({ state: "RUNNING", ...overrides });

/**
 * Create a mock query in FINISHED state
 */
export const createFinishedQuery = (overrides = {}) => createMockQuery({ state: "FINISHED", ...overrides });

/**
 * Create a mock query in FAILED state
 */
export const createFailedQuery = (overrides = {}) =>
    createMockQuery({
        state: "FAILED",
        errorType: "USER_ERROR",
        errorCode: null,
        ...overrides,
    });

/**
 * Create a mock query in QUEUED state
 */
export const createQueuedQuery = (overrides = {}) => createMockQuery({ state: "QUEUED", ...overrides });

/**
 * Create multiple mock queries with sequential IDs
 * @param {number} count - Number of queries to create
 * @param {Object} baseOverrides - Base overrides to apply to all queries
 * @returns {Array} Array of mock query objects
 */
export const createMockQueries = (count, baseOverrides = {}) => {
    return Array.from({ length: count }, (_, i) =>
        createMockQuery({
            queryId: `test_query_${i + 1}`,
            ...baseOverrides,
        })
    );
};

// Made with Bob
