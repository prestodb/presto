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
package io.prestosql.sql.planner.iterative;

import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Plans
{
    public static PlanNode resolveGroupReferences(PlanNode node, Lookup lookup)
    {
        requireNonNull(node, "node is null");
        return node.accept(new ResolvingVisitor(lookup), null);
    }

    private static class ResolvingVisitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final Lookup lookup;

        public ResolvingVisitor(Lookup lookup)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> children = node.getSources().stream()
                    .map(child -> child.accept(this, context))
                    .collect(Collectors.toList());

            return node.replaceChildren(children);
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, Void context)
        {
            return lookup.resolve(node).accept(this, context);
        }
    }

    private Plans() {}
}
