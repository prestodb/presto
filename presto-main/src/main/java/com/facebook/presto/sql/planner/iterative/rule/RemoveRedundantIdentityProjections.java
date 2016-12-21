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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

/**
 * Removes projection nodes that only perform non-renaming identity projections
 */
public class RemoveRedundantIdentityProjections
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode project = (ProjectNode) node;

        if (!project.isIdentity()) {
            return Optional.empty();
        }

        // only drop this projection if it does not constrain the outputs
        // of its child
        if (!ImmutableSet.copyOf(project.getOutputSymbols()).equals(ImmutableSet.copyOf(project.getSource().getOutputSymbols()))) {
            return Optional.empty();
        }

        return Optional.of(project.getSource());
    }
}
