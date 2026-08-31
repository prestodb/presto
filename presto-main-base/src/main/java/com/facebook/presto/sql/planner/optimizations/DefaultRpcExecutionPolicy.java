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
 * Default {@link RpcExecutionPolicy}: carries no batching heuristic, so it ignores the input
 * stats. Explicit PER_ROW/BATCH pass through unchanged; AUTOMATIC degrades to PER_ROW (a safe
 * fallback). A deployment that wants stats-driven AUTOMATIC resolution binds an override of
 * {@link RpcExecutionPolicy} via a Guice module.
 */
public class DefaultRpcExecutionPolicy
        implements RpcExecutionPolicy
{
    @Override
    public RpcExecutionProperties translateIntent(RpcExecutionIntent intent)
    {
        RPCNode.StreamingMode requested = intent.getRequestedMode();
        RPCNode.StreamingMode resolved = requested == RPCNode.StreamingMode.AUTOMATIC
                ? RPCNode.StreamingMode.PER_ROW
                : requested;
        return RpcExecutionProperties.of(resolved);
    }
}
