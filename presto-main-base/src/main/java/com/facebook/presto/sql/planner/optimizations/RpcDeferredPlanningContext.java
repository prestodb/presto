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
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class RpcDeferredPlanningContext
{
    private final CallExpression call;
    private final Session session;
    private final RpcArgumentLayout argumentLayout;
    private final int requestedDispatchBatchSize;
    private final RpcExecutionProperties executionProperties;
    private final Function<RowExpression, Optional<ConstantExpression>> constantResolver;

    public RpcDeferredPlanningContext(
            CallExpression call,
            Session session,
            RpcArgumentLayout argumentLayout,
            int requestedDispatchBatchSize,
            RpcExecutionProperties executionProperties,
            Function<RowExpression, Optional<ConstantExpression>> constantResolver)
    {
        this.call = requireNonNull(call, "call is null");
        this.session = requireNonNull(session, "session is null");
        this.argumentLayout = requireNonNull(argumentLayout, "argumentLayout is null");
        this.requestedDispatchBatchSize = requestedDispatchBatchSize;
        this.executionProperties = requireNonNull(executionProperties, "executionProperties is null");
        this.constantResolver = requireNonNull(constantResolver, "constantResolver is null");
    }

    public CallExpression getCall()
    {
        return call;
    }

    public Session getSession()
    {
        return session;
    }

    public RpcArgumentLayout getArgumentLayout()
    {
        return argumentLayout;
    }

    public int getRequestedDispatchBatchSize()
    {
        return requestedDispatchBatchSize;
    }

    public RpcExecutionProperties getExecutionProperties()
    {
        return executionProperties;
    }

    public Optional<ConstantExpression> resolveConstant(RowExpression expression)
    {
        return requireNonNull(constantResolver.apply(expression), "constant resolution is null");
    }
}
