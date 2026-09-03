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

import com.facebook.presto.sql.planner.plan.RPCNode;

/**
 * Default {@link RpcExecutionPolicy}: carries no cardinality heuristic. Explicit PER_ROW/BATCH
 * resolve to themselves; THROUGHPUT/COST resolve to BATCH; LATENCY and AUTOMATIC resolve to
 * PER_ROW (no stats heuristic here). A deployment that wants stats-driven AUTOMATIC resolution
 * binds an override of {@link RpcExecutionPolicy} via a Guice module.
 */
public class DefaultRpcExecutionPolicy
        implements RpcExecutionPolicy
{
    @Override
    public RpcExecutionProperties translateIntent(RpcExecutionIntent intent)
    {
        RPCNode.StreamingMode resolved;
        switch (intent.getRequestedMode()) {
            case BATCH:
            case THROUGHPUT:
            case COST:
                resolved = RPCNode.StreamingMode.BATCH;
                break;
            case PER_ROW:
            case LATENCY:
            case AUTOMATIC:
                resolved = RPCNode.StreamingMode.PER_ROW;
                break;
            default:
                // A new objective must state its own mapping. Falling through to
                // PER_ROW here would silently discard the objective's meaning.
                throw new IllegalArgumentException("Unhandled RpcExecutionMode: " + intent.getRequestedMode());
        }
        return RpcExecutionProperties.of(resolved);
    }
}
