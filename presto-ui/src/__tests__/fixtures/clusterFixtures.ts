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
 * Shared test fixtures for cluster-related tests
 * Provides factory functions to create mock cluster info objects with sensible defaults
 */

/**
 * Base mock cluster info object with all required fields
 */
export const baseMockClusterInfo = {
    runningQueries: 5,
    queuedQueries: 2,
    blockedQueries: 1,
    activeWorkers: 10,
    runningDrivers: 50,
    reservedMemory: 1073741824, // 1GB in bytes
    totalInputRows: 1000000,
    totalInputBytes: 1073741824,
    totalCpuTimeSecs: 100,
};

/**
 * Create a mock cluster info object with custom overrides
 * @param overrides - Properties to override in the base cluster info
 * @returns Mock cluster info object
 * @example
 * const busyCluster = createMockClusterInfo({ runningQueries: 50, activeWorkers: 100 });
 */
export const createMockClusterInfo = (overrides: Partial<typeof baseMockClusterInfo> = {}) => ({
    ...baseMockClusterInfo,
    ...overrides,
});

/**
 * Create a mock cluster info with high load
 */
export const createHighLoadCluster = (overrides: Partial<typeof baseMockClusterInfo> = {}) =>
    createMockClusterInfo({
        runningQueries: 50,
        queuedQueries: 20,
        blockedQueries: 10,
        activeWorkers: 100,
        ...overrides,
    });

/**
 * Create a mock cluster info with low load
 */
export const createLowLoadCluster = (overrides: Partial<typeof baseMockClusterInfo> = {}) =>
    createMockClusterInfo({
        runningQueries: 1,
        queuedQueries: 0,
        blockedQueries: 0,
        activeWorkers: 5,
        ...overrides,
    });

/**
 * Create a mock cluster info with no activity
 */
export const createIdleCluster = (overrides: Partial<typeof baseMockClusterInfo> = {}) =>
    createMockClusterInfo({
        runningQueries: 0,
        queuedQueries: 0,
        blockedQueries: 0,
        activeWorkers: 0,
        runningDrivers: 0,
        ...overrides,
    });

// Made with Bob
