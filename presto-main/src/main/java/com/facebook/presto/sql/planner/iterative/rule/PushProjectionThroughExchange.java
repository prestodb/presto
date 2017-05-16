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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.ExpressionSymbolInliner;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Pattern;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Assignments.identity;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * Transforms:
 *
 *  <pre>
 *  Project(x = e1, y = e2)
 *    Exchange()
 *      Source(a, b, c)
 *  </pre>
 *
 *  to:
 *
 *  <pre>
 *  Exchange()
 *    Project(x = e1, y = e2)
 *      Source(a, b, c)
 *  </pre>
 *
 *  Or if Exchange needs symbols from Source for partitioning or as hash symbol to:
 *
 *  <pre>
 *  Project(x, y)
 *    Exchange()
 *      Project(x = e1, y = e2, a)
 *        Source(a, b, c)
 *  </pre>
 *
 *
 *  To avoid looping this optimizer will not be fired if upper Project contains just symbol references.
 */
public class PushProjectionThroughExchange
        implements Rule
{
    private static final Pattern PATTERN = Pattern.node(ProjectNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode project = (ProjectNode) node;

        PlanNode child = lookup.resolve(project.getSource());
        if (!(child instanceof ExchangeNode)) {
            return Optional.empty();
        }

        if (isSymbolToSymbolProjection(project)) {
            return Optional.empty();
        }

        ExchangeNode exchange = (ExchangeNode) child;

        ImmutableList.Builder<PlanNode> newSourceBuilder = ImmutableList.builder();
        ImmutableList.Builder<List<Symbol>> inputsBuilder = ImmutableList.builder();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            Map<Symbol, SymbolReference> outputToInputMap = extractExchangeOutputToInput(exchange, i);

            Assignments.Builder projections = Assignments.builder();
            List<Symbol> inputs = new ArrayList<>();

            // Need to retain the partition keys for the exchange
            exchange.getPartitioningScheme().getPartitioning().getColumns().stream()
                    .map(outputToInputMap::get)
                    .forEach(nameReference -> {
                        Symbol symbol = Symbol.from(nameReference);
                        projections.put(symbol, nameReference);
                        inputs.add(symbol);
                    });

            if (exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                // Need to retain the hash symbol for the exchange
                projections.put(exchange.getPartitioningScheme().getHashColumn().get(), exchange.getPartitioningScheme().getHashColumn().get().toSymbolReference());
                inputs.add(exchange.getPartitioningScheme().getHashColumn().get());
            }

            if (exchange.getOrderingScheme().isPresent()) {
                // need to retain ordering columns for the exchange
                exchange.getOrderingScheme().get().getOrderBy().stream()
                        .map(outputToInputMap::get)
                        .forEach(nameReference -> {
                            Symbol symbol = Symbol.from(nameReference);
                            projections.put(symbol, nameReference);
                            inputs.add(symbol);
                        });
            }

            for (Map.Entry<Symbol, Expression> projection : project.getAssignments().entrySet()) {
                Expression translatedExpression = translateExpression(projection.getValue(), outputToInputMap);
                Type type = symbolAllocator.getTypes().get(projection.getKey());
                Symbol symbol = symbolAllocator.newSymbol(translatedExpression, type);
                projections.put(symbol, translatedExpression);
                inputs.add(symbol);
            }
            newSourceBuilder.add(new ProjectNode(idAllocator.getNextId(), exchange.getSources().get(i), projections.build()));

            // Need to deduplicate inputs in case partitioning/hash/ordering columns overlap
            ImmutableList<Symbol> deduplicatedInputs = deduplicateSymbols(inputs);
            inputsBuilder.add(deduplicatedInputs);
        }

        // Construct the output symbols in the same order as the sources
        List<Symbol> outputs = new ArrayList<>();
        exchange.getPartitioningScheme().getPartitioning().getColumns()
                .forEach(outputs::add);

        if (exchange.getPartitioningScheme().getHashColumn().isPresent()) {
            outputs.add(exchange.getPartitioningScheme().getHashColumn().get());
        }
        if (exchange.getOrderingScheme().isPresent()) {
            for (Symbol symbol : exchange.getOrderingScheme().get().getOrderBy()) {
                outputs.add(symbol);
            }
        }
        for (Map.Entry<Symbol, Expression> projection : project.getAssignments().entrySet()) {
            outputs.add(projection.getKey());
        }

        // outputBuilder contains all partition and hash symbols so simply swap the output layout
        PartitioningScheme partitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                deduplicateSymbols(outputs),
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
                exchange.isOrderSensitive(),
                exchange.getOrderingScheme());

        if (!result.getOutputSymbols().equals(project.getOutputSymbols())) {
            // we need to strip unnecessary symbols (hash, partitioning columns).
            result = new ProjectNode(idAllocator.getNextId(), result, identity(project.getOutputSymbols()));
        }

        return Optional.of(result);
    }

    private ImmutableList<Symbol> deduplicateSymbols(List<Symbol> symbols)
    {
        // use sequential mode to deduplicate the list because otherwise order isn't preserved
        return symbols.stream().sequential().distinct().collect(toImmutableList());
    }

    private boolean isSymbolToSymbolProjection(ProjectNode project)
    {
        return project.getAssignments().getExpressions().stream().allMatch(e -> e instanceof SymbolReference);
    }

    private static Map<Symbol, SymbolReference> extractExchangeOutputToInput(ExchangeNode exchange, int sourceIndex)
    {
        Map<Symbol, SymbolReference> outputToInputMap = new HashMap<>();
        for (int i = 0; i < exchange.getOutputSymbols().size(); i++) {
            outputToInputMap.put(exchange.getOutputSymbols().get(i), exchange.getInputs().get(sourceIndex).get(i).toSymbolReference());
        }
        return outputToInputMap;
    }

    private static Expression translateExpression(Expression inputExpression, Map<Symbol, SymbolReference> symbolMapping)
    {
        return new ExpressionSymbolInliner(symbolMapping::get).rewrite(inputExpression);
    }
}
