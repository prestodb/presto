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
 * Barrel export for all test fixtures
 * Import all fixtures from a single location for convenience
 *
 * @example
 * import { createMockQuery, createMockStage, createMockClusterInfo } from '../__tests__/fixtures';
 */

// Query fixtures
export * from "./queryFixtures";

// Info/cluster API fixtures
export * from "./infoFixtures";

// Stage fixtures
export * from "./stageFixtures";

// Cluster info fixtures
export * from "./clusterFixtures";

// Made with Bob
