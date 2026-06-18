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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.google.common.collect.ImmutableList;

import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMost;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFunctionProcessor;

/**
 * Table function can take multiple table arguments. Each argument is either "prune when empty" or "keep when empty".
 * "Prune when empty" means that if this argument has no rows, the function result is empty, so the function can be
 * removed from the plan, and replaced with empty values.
 * "Keep when empty" means that even if the argument has no rows, the function should still be executed, and it can
 * return a non-empty result.
 * All the table arguments are combined into a single source of a TableFunctionProcessorNode. If either argument is
 * "prune when empty", the overall result is "prune when empty". This rule removes a redundant TableFunctionProcessorNode
 * based on the "prune when empty" property.
 */
public class RemoveRedundantTableFunctionProcessor
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        if (node.isPruneWhenEmpty() && node.getSource().isPresent()) {
            if (isAtMost(node.getSource().orElseThrow(NoSuchElementException::new), context.getLookup(), 0)) {
                return Result.ofPlanNode(
                        new ValuesNode(node.getSourceLocation(),
                                node.getId(),
                                node.getOutputVariables(),
                                ImmutableList.of(),
                                Optional.empty()));
            }
        }

        return Result.empty();
    }
}
