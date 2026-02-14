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
 * Shared test fixtures for stage-related tests
 * Provides factory functions to create mock stage objects with sensible defaults
 */

/**
 * Base mock stage object with all required fields
 */
export const baseMockStage = {
    stageId: "0",
    state: "RUNNING",
    stageStats: {
        totalDrivers: 10,
        queuedDrivers: 0,
        runningDrivers: 5,
        completedDrivers: 5,
        fullyBlocked: false,
    },
};

/**
 * Create a mock stage with custom overrides
 * @param overrides - Properties to override in the base stage
 * @returns Mock stage object
 * @example
 * const finishedStage = createMockStage({ state: "FINISHED", stageId: "1" });
 */
export const createMockStage = (overrides: Partial<typeof baseMockStage> = {}) => {
    const merged = { ...baseMockStage, ...overrides };
    if (overrides.stageStats) {
        merged.stageStats = { ...baseMockStage.stageStats, ...overrides.stageStats };
    }
    return merged;
};

/**
 * Create a mock stage in RUNNING state
 */
export const createRunningStage = (overrides: Partial<typeof baseMockStage> = {}) =>
    createMockStage({ state: "RUNNING", ...overrides });

/**
 * Create a mock stage in FINISHED state
 */
export const createFinishedStage = (overrides: Partial<typeof baseMockStage> = {}) =>
    createMockStage({ state: "FINISHED", ...overrides });

/**
 * Create a mock stage in FAILED state
 */
export const createFailedStage = (overrides: Partial<typeof baseMockStage> = {}) =>
    createMockStage({ state: "FAILED", ...overrides });

/**
 * Create multiple mock stages with sequential IDs
 * @param count - Number of stages to create
 * @param baseOverrides - Base overrides to apply to all stages
 * @returns Array of mock stage objects
 */
export const createMockStages = (count: number, baseOverrides: Partial<typeof baseMockStage> = {}) => {
    return Array.from({ length: count }, (_, i) =>
        createMockStage({
            stageId: `${i}`,
            ...baseOverrides,
        })
    );
};

// Made with Bob
