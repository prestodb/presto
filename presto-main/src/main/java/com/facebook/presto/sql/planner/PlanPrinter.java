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
package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.util.GraphvizPrinter;
import com.facebook.presto.util.JsonPlanPrinter;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.DomainUtils.simplifyDomain;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Optional<Map<PlanFragmentId, PlanFragment>> fragmentsById)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(types, "types is null");
        checkNotNull(fragmentsById, "fragmentsById is null");
        Visitor visitor = new Visitor(types, fragmentsById);
        plan.accept(visitor, 0);
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        return new PlanPrinter(plan, types, Optional.<Map<PlanFragmentId, PlanFragment>>absent()).toString();
    }

    public static String getJsonPlanSource(PlanNode plan, Metadata metadata)
    {
        return JsonPlanPrinter.getPlan(plan, metadata);
    }

    public static String textDistributedPlan(SubPlan plan)
    {
        List<PlanFragment> fragments = plan.getAllFragments();
        Map<PlanFragmentId, PlanFragment> fragmentsById = Maps.uniqueIndex(fragments, PlanFragment.idGetter());
        PlanFragment fragment = plan.getFragment();
        return new PlanPrinter(fragment.getRoot(), fragment.getSymbols(), Optional.of(fragmentsById)).toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId("graphviz_plan"), plan, types, PlanDistribution.NONE, plan.getId(), OutputPartitioning.NONE, ImmutableList.<Symbol>of());
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private void print(int indent, String format, Object... args)
    {
        String value;

        if (args.length == 0) {
            value = format;
        }
        else {
            value = format(format, args);
        }
        output.append(Strings.repeat("    ", indent)).append(value).append('\n');
    }

    private class Visitor
            extends PlanVisitor<Integer, Void>
    {
        private final Map<Symbol, Type> types;
        private final Optional<Map<PlanFragmentId, PlanFragment>> fragmentsById;

        public Visitor(Map<Symbol, Type> types, Optional<Map<PlanFragmentId, PlanFragment>> fragmentsById)
        {
            this.types = types;
            this.fragmentsById = fragmentsById;
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(clause.getLeft().toQualifiedName()),
                        new QualifiedNameReference(clause.getRight().toQualifiedName())));
            }

            print(indent, "- %s[%s] => [%s]", node.getType().getJoinLabel(), Joiner.on(" AND ").join(joinExpressions), formatOutputs(node.getOutputSymbols()));
            node.getLeft().accept(this, indent + 1);
            node.getRight().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Integer indent)
        {
            print(indent, "- SemiJoin[%s = %s] => [%s]", node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), formatOutputs(node.getOutputSymbols()));
            node.getSource().accept(this, indent + 1);
            node.getFilteringSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Integer indent)
        {
            print(indent, "- IndexSource[%s, lookup = %s] => [%s]", node.getIndexHandle(), node.getLookupSymbols(), formatOutputs(node.getOutputSymbols()));
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Integer indent)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                        new QualifiedNameReference(clause.getProbe().toQualifiedName()),
                        new QualifiedNameReference(clause.getIndex().toQualifiedName())));
            }

            print(indent, "- %sIndexJoin[%s] => [%s]", node.getType().getJoinLabel(), Joiner.on(" AND ").join(joinExpressions), formatOutputs(node.getOutputSymbols()));
            node.getProbeSource().accept(this, indent + 1);
            node.getIndexSource().accept(this, indent + 1);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Integer indent)
        {
            print(indent, "- Limit[%s] => [%s]", node.getCount(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Integer indent)
        {
            print(indent, "- DistinctLimit[%s] => [%s]", node.getLimit(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            String key = "";
            if (!node.getGroupBy().isEmpty()) {
                key = node.getGroupBy().toString();
            }
            String sampleWeight = "";
            if (node.getSampleWeight().isPresent()) {
                sampleWeight = format("[sampleWeight = %s]", node.getSampleWeight().get());
            }

            print(indent, "- Aggregate%s%s%s => [%s]", type, key, sampleWeight, formatOutputs(node.getOutputSymbols()));

            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                if (node.getMasks().containsKey(entry.getKey())) {
                    print(indent + 2, "%s := %s (mask = %s)", entry.getKey(), entry.getValue(), node.getMasks().get(entry.getKey()));
                }
                else {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Integer indent)
        {
            print(indent, "- MarkDistinct[distinct=%s marker=%s] => [%s]", formatOutputs(node.getDistinctSymbols()), node.getMarkerSymbol(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitWindow(final WindowNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), new Function<Symbol, String>()
            {
                @Override
                public String apply(Symbol input)
                {
                    return input + " " + node.getOrderings().get(input);
                }
            });

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }
            if (!orderBy.isEmpty()) {
                args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));
            }

            print(indent, "- Window[%s] => [%s]", Joiner.on(", ").join(args), formatOutputs(node.getOutputSymbols()));

            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                print(indent + 2, "%s := %s(%s)", entry.getKey(), entry.getValue().getName(), Joiner.on(", ").join(entry.getValue().getArguments()));
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TupleDomain partitionsDomainSummary = node.getPartitionsDomainSummary();
            print(indent, "- TableScan[%s, original constraint=%s] => [%s]", node.getTable(), node.getOriginalConstraint(), formatOutputs(node.getOutputSymbols()));
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                boolean isOutputSymbol = node.getOutputSymbols().contains(entry.getKey());
                boolean isInOriginalConstraint = node.getOriginalConstraint() == null ? false : DependencyExtractor.extractUnique(node.getOriginalConstraint()).contains(entry.getKey());
                boolean isInDomainSummary = !partitionsDomainSummary.isNone() && partitionsDomainSummary.getDomains().keySet().contains(entry.getValue());

                if (isOutputSymbol || isInOriginalConstraint || isInDomainSummary) {
                    print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
                    if (isInDomainSummary) {
                        print(indent + 3, ":: %s", simplifyDomain(partitionsDomainSummary.getDomains().get(entry.getValue())));
                    }
                    else if (partitionsDomainSummary.isNone()) {
                        print(indent + 3, ":: NONE");
                    }
                }
            }
            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            print(indent, "- Values => [%s]", formatOutputs(node.getOutputSymbols()));
            for (List<Expression> row : node.getRows()) {
                print(indent + 2, Joiner.on(", ").join(row));
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Integer indent)
        {
            print(indent, "- Filter[%s] => [%s]", node.getPredicate(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitProject(ProjectNode node, Integer indent)
        {
            print(indent, "- Project => [%s]", formatOutputs(node.getOutputSymbols()));
            for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
                if (entry.getValue() instanceof QualifiedNameReference && ((QualifiedNameReference) entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            print(indent, "- Output[%s]", Joiner.on(", ").join(node.getColumnNames()));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    print(indent + 2, "%s := %s", name, symbol);
                }
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopN(final TopNNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), new Function<Symbol, String>()
            {
                @Override
                public String apply(Symbol input)
                {
                    return input + " " + node.getOrderings().get(input);
                }
            });

            print(indent, "- TopN[%s by (%s)] => [%s]", node.getCount(), Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitMaterializeSample(final MaterializeSampleNode node, Integer indent)
        {
            print(indent, "- MaterializeSample[%s] => [%s]", node.getSampleWeightSymbol(), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSort(final SortNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), new Function<Symbol, String>()
            {
                @Override
                public String apply(Symbol input)
                {
                    return input + " " + node.getOrderings().get(input);
                }
            });

            print(indent, "- Sort[%s] => [%s]", Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            print(indent, "- Exchange[%s] => [%s]", node.getSourceFragmentIds(), formatOutputs(node.getOutputSymbols()));

            return processExchange(node, indent + 1);
        }

        @Override
        public Void visitSink(SinkNode node, Integer indent)
        {
            print(indent, "- Sink[%s] => [%s]", node.getId(), formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitUnion(UnionNode node, Integer indent)
        {
            print(indent, "- Union => [%s]", formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Integer indent)
        {
            print(indent, "- TableWriter => [%s]", formatOutputs(node.getOutputSymbols()));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                print(indent + 2, "%s := %s", name, symbol);
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableCommit(TableCommitNode node, Integer indent)
        {
            print(indent, "- TableCommit[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        protected Void visitPlan(PlanNode node, Integer context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, int indent)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, indent);
            }

            return null;
        }

        private Void processExchange(ExchangeNode node, int indent)
        {
            for (PlanFragmentId planFragmentId : node.getSourceFragmentIds()) {
                PlanFragment target = fragmentsById.get().get(planFragmentId);
                target.getRoot().accept(this, indent);
            }
            return null;
        }

        private String formatOutputs(List<Symbol> symbols)
        {
            return Joiner.on(", ").join(Iterables.transform(symbols, new Function<Symbol, String>()
            {
                @Override
                public String apply(Symbol input)
                {
                    return input + ":" + types.get(input);
                }
            }));
        }
    }
}
