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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.ExpressionUtils;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.util.ImmutableCollectors;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.lang.String.format;

public class PlanBuilder
{
    private final PlanNodeIdAllocator idAllocator;
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public PlanBuilder(PlanNodeIdAllocator idAllocator)
    {
        this.idAllocator = idAllocator;
    }

    public ValuesNode values(Symbol... columns)
    {
        return new ValuesNode(
                idAllocator.getNextId(),
                ImmutableList.copyOf(columns),
                ImmutableList.of());
    }

    public ValuesNode values(List<Symbol> columns, List<List<Expression>> rows)
    {
        return new ValuesNode(idAllocator.getNextId(), columns, rows);
    }

    public LimitNode limit(long limit, PlanNode source)
    {
        return new LimitNode(idAllocator.getNextId(), source, limit, false);
    }

    public SampleNode sample(double sampleRatio, SampleNode.Type type, PlanNode source)
    {
        return new SampleNode(idAllocator.getNextId(), source, sampleRatio, type);
    }

    public ProjectNode project(Assignments assignments, PlanNode source)
    {
        return new ProjectNode(idAllocator.getNextId(), source, assignments);
    }

    public FilterNode filter(Expression predicate, PlanNode source)
    {
        return new FilterNode(idAllocator.getNextId(), source, predicate);
    }

    public ApplyNode apply(Assignments subqueryAssignments, List<Symbol> correlation, PlanNode input, PlanNode subquery)
    {
        return new ApplyNode(idAllocator.getNextId(), input, subquery, subqueryAssignments, correlation);
    }

    public ExchangeNode exchange(Consumer<ExchangeBuilder> exchangeBuilderConsumer)
    {
        ExchangeBuilder exchangeBuilder = new ExchangeBuilder();
        exchangeBuilderConsumer.accept(exchangeBuilder);
        return exchangeBuilder.build();
    }

    public class ExchangeBuilder
    {
        private ExchangeNode.Type type = ExchangeNode.Type.GATHER;
        private ExchangeNode.Scope scope = ExchangeNode.Scope.REMOTE;
        private PartitioningScheme partitioningScheme;
        private List<PlanNode> sources = new ArrayList<>();
        private List<List<Symbol>> inputs = new ArrayList<>();

        public ExchangeBuilder type(ExchangeNode.Type type)
        {
            this.type = type;
            return this;
        }

        public ExchangeBuilder scope(ExchangeNode.Scope scope)
        {
            this.scope = scope;
            return this;
        }

        public ExchangeBuilder singleDistributionPartitioningScheme(Symbol... outputSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(partitioningSymbols)), ImmutableList.copyOf(outputSymbols)));
        }

        public ExchangeBuilder fixedHashDistributionParitioningScheme(List<Symbol> outputSymbols, List<Symbol> partitioningSymbols, Symbol hashSymbol)
        {
            return partitioningScheme(new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.copyOf(partitioningSymbols)), ImmutableList.copyOf(outputSymbols), Optional.of(hashSymbol)));
        }

        public ExchangeBuilder partitioningScheme(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
            return this;
        }

        public ExchangeBuilder addSource(PlanNode source)
        {
            this.sources.add(source);
            return this;
        }

        public ExchangeBuilder addInputsSet(Symbol... inputs)
        {
            this.inputs.add(ImmutableList.copyOf(inputs));
            return this;
        }

        protected ExchangeNode build()
        {
            return new ExchangeNode(idAllocator.getNextId(), type, scope, partitioningScheme, sources, inputs);
        }
    }

    public Symbol symbol(String name, Type type)
    {
        Symbol symbol = new Symbol(name);

        Type old = symbols.get(symbol);
        if (old != null && !old.equals(type)) {
            throw new IllegalArgumentException(format("Symbol '%s' already registered with type '%s'", name, old));
        }

        if (old == null) {
            symbols.put(symbol, type);
        }

        return symbol;
    }

    public static Expression expression(String sql)
    {
        return ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql));
    }

    public static List<Expression> expressions(String... expressions)
    {
        return Stream.of(expressions)
                .map(PlanBuilder::expression)
                .collect(ImmutableCollectors.toImmutableList());
    }

    public Map<Symbol, Type> getSymbols()
    {
        return Collections.unmodifiableMap(symbols);
    }
}
