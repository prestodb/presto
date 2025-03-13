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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.google.common.base.Preconditions.checkState;

public class TestAddPartitionToSortRule
        implements Rule<SortNode>
{
    @Override
    public Pattern<SortNode> getPattern()
    {
        return typeOf(SortNode.class);
    }

    @Override
    public Result apply(SortNode node, Captures captures, Context context)
    {
        if (!node.getPartitionBy().isEmpty()) {
            return Result.empty();
        }
        Optional<VariableReferenceExpression> partition = node.getSource().getOutputVariables().stream().filter(x -> x.getType().equals(BIGINT)).findFirst();
        checkState(partition.isPresent());
        return Result.ofPlanNode(new SortNode(node.getSourceLocation(), context.getIdAllocator().getNextId(), node.getSource(), node.getOrderingScheme(), node.isPartial(), ImmutableList.of(partition.get())));
    }
}
