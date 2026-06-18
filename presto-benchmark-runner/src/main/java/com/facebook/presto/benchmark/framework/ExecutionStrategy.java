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
package com.facebook.presto.benchmark.framework;

public enum ExecutionStrategy
{
    /**
     * STREAM execution requires an {@link StreamExecutionPhase}.
     *
     * Each {@link StreamExecutionPhase} defines a list of streams, while
     * each stream defines a list of queries (query names).
     *
     * Each stream are executed in parallel, while queries within a stream
     * are executed sequentially in the order defined.
     */
    STREAM,

    /**
     * CONCURRENT execution requires a {@link ConcurrentExecutionPhase}.
     *
     * Each {@link ConcurrentExecutionPhase} defines a list of queries.
     * Multiples queries can be executed at the same time and the order
     * of execution is arbitrary. Max concurrency can be defined either
     * within {@link ConcurrentExecutionPhase} or via configuration property.
     */
    CONCURRENT
}
