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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

/**
 * Transforms:
 * <pre>
 *  Project(x = e1, y = e2)
 *    Exchange()
 *      Source(a, b, c)
 *  </pre>
 * to:
 * <pre>
 *  Exchange()
 *    Project(x = e1, y = e2)
 *      Source(a, b, c)
 *  </pre>
 * Or if Exchange needs symbols from Source for partitioning or as hash symbol to:
 * <pre>
 *  Project(x, y)
 *    Exchange()
 *      Project(x = e1, y = e2, a)
 *        Source(a, b, c)
 *  </pre>
 * To avoid looping this optimizer will not be fired if upper Project contains just symbol references.
 */
public class PushProjectionThroughExchange
        implements Rule<ProjectNode>
{
    private static final Capture<ExchangeNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(project -> !isSymbolToSymbolProjection(project))
            .with(source().matching(exchange().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        ExchangeNode exchange = captures.get(CHILD);
        Set<VariableReferenceExpression> partitioningColumns = exchange.getPartitioningScheme().getPartitioning().getVariableReferences();

        ImmutableList.Builder<PlanNode> newSourceBuilder = ImmutableList.builder();
        ImmutableList.Builder<List<VariableReferenceExpression>> inputsBuilder = ImmutableList.builder();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMap = extractExchangeOutputToInput(exchange, i);

            Assignments.Builder projections = Assignments.builder();
            ImmutableList.Builder<VariableReferenceExpression> inputs = ImmutableList.builder();

            // Need to retain the partition keys for the exchange
            partitioningColumns.stream()
                    .map(outputToInputMap::get)
                    .forEach(variable -> {
                        projections.put(variable, variable);
                        inputs.add(variable);
                    });

            if (exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                // Need to retain the hash symbol for the exchange
                VariableReferenceExpression hashVariable = exchange.getPartitioningScheme().getHashColumn().get();
                projections.put(hashVariable, hashVariable);
                inputs.add(hashVariable);
            }

            if (exchange.getOrderingScheme().isPresent()) {
                // need to retain ordering columns for the exchange
                exchange.getOrderingScheme().get().getOrderByVariables().stream()
                        // do not project the same symbol twice as ExchangeNode verifies that source input symbols match partitioning scheme outputLayout
                        .filter(variable -> !partitioningColumns.contains(variable))
                        .map(outputToInputMap::get)
                        .forEach(variable -> {
                            projections.put(variable, variable);
                            inputs.add(variable);
                        });
            }

            for (Map.Entry<VariableReferenceExpression, RowExpression> projection : project.getAssignments().entrySet()) {
                RowExpression translatedExpression = RowExpressionVariableInliner.inlineVariables(outputToInputMap, projection.getValue());
                VariableReferenceExpression variable = context.getVariableAllocator().newVariable(translatedExpression);
                projections.put(variable, translatedExpression);
                inputs.add(variable);
            }
            newSourceBuilder.add(new ProjectNode(context.getIdAllocator().getNextId(), exchange.getSources().get(i), projections.build(), project.getLocality()));
            inputsBuilder.add(inputs.build());
        }

        // Construct the output symbols in the same order as the sources
        ImmutableList.Builder<VariableReferenceExpression> outputBuilder = ImmutableList.builder();
        partitioningColumns.forEach(outputBuilder::add);
        exchange.getPartitioningScheme().getHashColumn().ifPresent(outputBuilder::add);
        if (exchange.getOrderingScheme().isPresent()) {
            exchange.getOrderingScheme().get().getOrderByVariables().stream()
                    .filter(variable -> !partitioningColumns.contains(variable))
                    .forEach(outputBuilder::add);
        }
        for (Map.Entry<VariableReferenceExpression, RowExpression> projection : project.getAssignments().entrySet()) {
            outputBuilder.add(projection.getKey());
        }

        // outputBuilder contains all partition and hash symbols so simply swap the output layout
        PartitioningScheme partitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                outputBuilder.build(),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        PlanNode result = new ExchangeNode(
                exchange.getId(),
                exchange.getType(),
                exchange.getScope(),
                partitioningScheme,
                newSourceBuilder.build(),
                inputsBuilder.build(),
                exchange.isEnsureSourceOrdering(),
                exchange.getOrderingScheme());

        // we need to strip unnecessary symbols (hash, partitioning columns).
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(project.getOutputVariables()), true).orElse(result));
    }

    private static boolean isSymbolToSymbolProjection(ProjectNode project)
    {
        return project.getAssignments().getExpressions().stream().allMatch(e -> e instanceof VariableReferenceExpression);
    }

    private static Map<VariableReferenceExpression, VariableReferenceExpression> extractExchangeOutputToInput(ExchangeNode exchange, int sourceIndex)
    {
        Map<VariableReferenceExpression, VariableReferenceExpression> outputToInputMap = new HashMap<>();
        for (int i = 0; i < exchange.getOutputVariables().size(); i++) {
            outputToInputMap.put(exchange.getOutputVariables().get(i), exchange.getInputs().get(sourceIndex).get(i));
        }
        return outputToInputMap;
    }
}
