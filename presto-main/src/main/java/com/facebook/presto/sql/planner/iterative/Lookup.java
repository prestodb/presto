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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;

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
        if (node instanceof GroupReference) {
            return getOnlyElement(resolveGroup(node));
        }
        return node;
    }

    /**
     * Resolves nodes by materializing GroupReference nodes
     * representing symbolic references to other nodes.
     * <p>
     * If the node is not a GroupReference, it returns the
     * singleton of the argument node.
     */
    List<PlanNode> resolveGroup(PlanNode node);

    GroupTraitSet resolveGroupTraitSet(PlanNode node);

    /**
     * A Lookup implementation that does not perform lookup. It satisfies contract
     * by rejecting {@link GroupReference}-s.
     */
    static Lookup noLookup()
    {
        return new Lookup()
        {
            @Override
            public List<PlanNode> resolveGroup(PlanNode node)
            {
                verify(!(node instanceof GroupReference), "Unexpected GroupReference");
                return ImmutableList.of(node);
            }

            @Override
            public GroupTraitSet resolveGroupTraitSet(PlanNode node)
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
