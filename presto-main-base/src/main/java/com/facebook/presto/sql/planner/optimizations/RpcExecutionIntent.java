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

import com.facebook.presto.Session;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.sql.planner.plan.RPCNode;

import static java.util.Objects.requireNonNull;

/**
 * The planning intent for a single RPC function call: the requested execution mode (a mechanism
 * or an objective) plus the context a policy may use to resolve it.
 *
 * This is the input to {@link RpcExecutionPolicy#translateIntent}. It is built via {@link
 * #builder} and is intentionally growable — new context fields can be added here (a new builder
 * setter) without changing the policy method signature or breaking existing implementations.
 *
 * Scope note: this models the <em>plan-time execution axis</em> (streaming mode, and in future
 * dispatch parallelism / batch size), which the optimizer fixes on the {@link RPCNode}. Runtime
 * concerns (error policy, deadlines, model-quality/cost) are carried separately via query config
 * read by the worker and are deliberately NOT part of this intent.
 */
public class RpcExecutionIntent
{
    private final RpcExecutionMode requestedMode;
    private final PlanNodeStatsEstimate inputStats;
    private final Session session;
    private final String functionName;

    private RpcExecutionIntent(
            RpcExecutionMode requestedMode,
            PlanNodeStatsEstimate inputStats,
            Session session,
            String functionName)
    {
        this.requestedMode = requireNonNull(requestedMode, "requestedMode is null");
        this.inputStats = requireNonNull(inputStats, "inputStats is null");
        this.session = requireNonNull(session, "session is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
    }

    /**
     * The requested dispatch mode — an explicit mechanism (PER_ROW/BATCH) or an objective
     * (LATENCY/THROUGHPUT/COST/AUTOMATIC) the policy resolves to a concrete streaming mode.
     */
    public RpcExecutionMode getRequestedMode()
    {
        return requestedMode;
    }

    /**
     * Estimated stats of the RPC node's input; an unknown estimate (e.g.
     * {@link PlanNodeStatsEstimate#unknown()}) reports NaN for individual statistics.
     */
    public PlanNodeStatsEstimate getInputStats()
    {
        return inputStats;
    }

    public Session getSession()
    {
        return session;
    }

    /**
     * The RPC function being called (e.g. {@code fb_llm_inference}), for policies that vary by
     * function.
     */
    public String getFunctionName()
    {
        return functionName;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private RpcExecutionMode requestedMode;
        private PlanNodeStatsEstimate inputStats = PlanNodeStatsEstimate.unknown();
        private Session session;
        private String functionName;

        public Builder setRequestedMode(RpcExecutionMode requestedMode)
        {
            this.requestedMode = requestedMode;
            return this;
        }

        public Builder setInputStats(PlanNodeStatsEstimate inputStats)
        {
            this.inputStats = inputStats;
            return this;
        }

        public Builder setSession(Session session)
        {
            this.session = session;
            return this;
        }

        public Builder setFunctionName(String functionName)
        {
            this.functionName = functionName;
            return this;
        }

        public RpcExecutionIntent build()
        {
            return new RpcExecutionIntent(requestedMode, inputStats, session, functionName);
        }
    }
}
