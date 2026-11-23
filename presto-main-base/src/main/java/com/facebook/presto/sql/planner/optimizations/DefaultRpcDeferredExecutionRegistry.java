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

import com.facebook.presto.spi.relation.CallExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class DefaultRpcDeferredExecutionRegistry
        implements RpcDeferredExecutionRegistry
{
    private final Function<CallExpression, RpcArgumentLayout> layoutResolver;
    private final List<DeferredRpcBackendPlanner> providers;

    public DefaultRpcDeferredExecutionRegistry()
    {
        this(call -> call.getArguments().size() >= 3
                ? RpcArgumentLayout.optionsAt(call.getArguments().size() - 1)
                : RpcArgumentLayout.noOptions(), ImmutableList.of());
    }

    public DefaultRpcDeferredExecutionRegistry(
            Function<CallExpression, RpcArgumentLayout> layoutResolver,
            List<DeferredRpcBackendPlanner> providers)
    {
        this.layoutResolver = requireNonNull(layoutResolver, "layoutResolver is null");
        this.providers = ImmutableList.copyOf(requireNonNull(providers, "providers is null"));
    }

    @Override
    public RpcArgumentLayout argumentLayout(CallExpression call)
    {
        RpcArgumentLayout layout = requireNonNull(layoutResolver.apply(call), "argument layout is null");
        checkArgument(!layout.getOptionsArgumentIndex().isPresent() ||
                        layout.getOptionsArgumentIndex().getAsInt() < call.getArguments().size(),
                "RPC options argument is outside the call signature: %s", call.getDisplayName());
        return layout;
    }

    @Override
    public Optional<DeferredRpcExecution> resolve(RpcDeferredPlanningContext context)
    {
        DeferredRpcExecution selected = null;
        for (DeferredRpcBackendPlanner provider : providers) {
            Optional<DeferredRpcExecution> candidate = requireNonNull(provider.resolve(context), "provider result is null");
            if (candidate.isPresent()) {
                checkArgument(selected == null, "Multiple deferred RPC providers support call %s", context.getCall().getDisplayName());
                selected = candidate.get();
            }
        }
        return Optional.ofNullable(selected);
    }
}
