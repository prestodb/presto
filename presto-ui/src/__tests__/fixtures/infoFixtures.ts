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
 * Shared test fixtures for info/cluster API responses
 * Used primarily by PageTitle component tests
 */

import { mockFetchByUrl } from "../mocks/apiMocks";

/**
 * Base mock info response
 */
export const baseMockInfo = {
    nodeVersion: { version: "1.0.0" },
    environment: "test",
    uptime: "1h",
};

/**
 * Base mock cluster response
 */
export const baseMockCluster = {
    clusterTag: "",
};

/**
 * Create a mock info object with custom overrides
 * @param overrides - Properties to override in the base info
 * @returns Mock info object
 */
export const createMockInfo = (overrides: Partial<typeof baseMockInfo> = {}) => ({
    ...baseMockInfo,
    ...overrides,
    nodeVersion: {
        ...baseMockInfo.nodeVersion,
        ...(overrides.nodeVersion || {}),
    },
});

/**
 * Create a mock cluster object with custom overrides
 * @param overrides - Properties to override in the base cluster
 * @returns Mock cluster object
 */
export const createMockCluster = (overrides: Partial<typeof baseMockCluster> = {}) => ({
    ...baseMockCluster,
    ...overrides,
});

/**
 * Setup PageTitle test with standard API mocks
 * This is the most common pattern in PageTitle tests
 * @param infoOverrides - Custom info response data
 * @param clusterOverrides - Custom cluster response data
 */
export const setupPageTitleTest = (
    infoOverrides: Partial<typeof baseMockInfo> = {},
    clusterOverrides: Partial<typeof baseMockCluster> = {}
) => {
    mockFetchByUrl({
        "/v1/info": createMockInfo(infoOverrides),
        "/v1/cluster": createMockCluster(clusterOverrides),
    });
};

// Made with Bob
