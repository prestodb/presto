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
package com.facebook.presto.sql.planner.optimizations;

/**
 * The user-facing RPC dispatch mode — the value space of the {@code rpc_streaming_mode} session
 * property. A value is either an explicit mechanism (PER_ROW / BATCH) or an optimization objective
 * (LATENCY / THROUGHPUT / COST / AUTOMATIC) that {@link RpcExecutionPolicy} resolves to a concrete
 * {@link com.facebook.presto.sql.planner.plan.RPCNode.StreamingMode} (PER_ROW/BATCH) at plan time.
 * Objectives let the caller state WHAT to optimize for rather than the mechanism. Growable — a new
 * objective maps to a mechanism in the policy, no other code changes.
 */
public enum RpcExecutionMode
{
    /** Explicit mechanism: one RPC per row. */
    PER_ROW,
    /** Explicit mechanism: accumulate rows and dispatch as a batch. */
    BATCH,
    /** Objective: minimize tail / time-to-first-row latency — resolves to per-row. */
    LATENCY,
    /** Objective: maximize rows/sec — resolves to batch. */
    THROUGHPUT,
    /** Objective: minimize $/GPU — resolves to batch (aliases THROUGHPUT until real cost signals). */
    COST,
    /** Objective: let the system decide (cardinality-based where the policy supports it). */
    AUTOMATIC
}
