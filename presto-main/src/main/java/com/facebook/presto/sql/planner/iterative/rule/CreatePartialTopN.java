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

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.TopNNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.TopNNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.TopNNode.Step.SINGLE;

public class CreatePartialTopN
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(TopNNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof TopNNode)) {
            return Optional.empty();
        }

        TopNNode single = (TopNNode) node;

        if (!single.getStep().equals(SINGLE)) {
            return Optional.empty();
        }

        PlanNode source = lookup.resolve(single.getSource());

        TopNNode partial = new TopNNode(
                idAllocator.getNextId(),
                source,
                single.getCount(),
                single.getOrderBy(),
                single.getOrderings(),
                PARTIAL);

        return Optional.of(new TopNNode(
                idAllocator.getNextId(),
                partial,
                single.getCount(),
                single.getOrderBy(),
                single.getOrderings(),
                FINAL));
    }
}
