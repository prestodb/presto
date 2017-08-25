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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.MoreCollectors.toOptional;

public interface Lookup
{
    /**
     * Resolves a node by materializing GroupReference nodes
     * representing symbolic references to other nodes. This method
     * is deprecated since is assumes group contains only one node.
     * <p>
     * If the node is not a GroupReference, it returns the
     * argument as is.
     */
    @Deprecated
    default PlanNode resolve(PlanNode node)
    {
        return resolveGroup(node).collect(toOptional()).get();
    }

    /**
     * Resolves nodes by materializing GroupReference nodes
     * representing symbolic references to other nodes.
     * <p>
     * If the node is not a GroupReference, it returns the
     * singleton of the argument node.
     */
    Stream<PlanNode> resolveGroup(PlanNode node);

    /**
     * A Lookup implementation that does not perform lookup. It satisfies contract
     * by rejecting {@link GroupReference}-s.
     */
    static Lookup noLookup()
    {
        return node -> {
            verify(!(node instanceof GroupReference), "Unexpected GroupReference");
            return Stream.of(node);
        };
    }

    static Lookup from(Function<GroupReference, Stream<PlanNode>> resolver)
    {
        return node -> {
            if (node instanceof GroupReference) {
                return resolver.apply((GroupReference) node);
            }

            return Stream.of(node);
        };
    }
}
