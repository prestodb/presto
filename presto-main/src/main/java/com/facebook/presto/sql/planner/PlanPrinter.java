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
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.Marker;
import com.facebook.presto.spi.Range;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanFragment.OutputPartitioning;
import com.facebook.presto.sql.planner.PlanFragment.PlanDistribution;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DeleteNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.GraphvizPrinter;
import com.facebook.presto.util.ImmutableCollectors;
import com.facebook.presto.util.JsonPlanPrinter;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.DomainUtils.simplifyDomain;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class PlanPrinter
{
    private final StringBuilder output = new StringBuilder();
    private final Metadata metadata;

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata)
    {
        this(plan, types, metadata, 0);
    }

    private PlanPrinter(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, int indent)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(types, "types is null");
        checkNotNull(metadata, "metadata is null");

        this.metadata = metadata;

        Visitor visitor = new Visitor(types);
        plan.accept(visitor, indent);
    }

    @Override
    public String toString()
    {
        return output.toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata)
    {
        return new PlanPrinter(plan, types, metadata).toString();
    }

    public static String textLogicalPlan(PlanNode plan, Map<Symbol, Type> types, Metadata metadata, int indent)
    {
        return new PlanPrinter(plan, types, metadata, indent).toString();
    }

    public static String getJsonPlanSource(PlanNode plan, Metadata metadata)
    {
        return JsonPlanPrinter.getPlan(plan, metadata);
    }

    public static String textDistributedPlan(SubPlan plan, Metadata metadata)
    {
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(String.format("Fragment %s [%s]\n",
                    fragment.getId(),
                    fragment.getDistribution()));

            builder.append(indentString(1))
                    .append(String.format("Output layout: [%s]\n",
                            Joiner.on(", ").join(fragment.getOutputLayout())));

            if (fragment.getOutputPartitioning() == OutputPartitioning.HASH) {
                builder.append(indentString(1))
                        .append(String.format("Output partitioning: [%s]\n",
                                Joiner.on(", ").join(fragment.getPartitionBy())));
            }

            builder.append(textLogicalPlan(fragment.getRoot(), fragment.getSymbols(), metadata, 1))
                    .append("\n");
        }

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        PlanFragment fragment = new PlanFragment(new PlanFragmentId("graphviz_plan"), plan, types, plan.getOutputSymbols(), PlanDistribution.SINGLE, plan.getId(), OutputPartitioning.NONE, ImmutableList.<Symbol>of(), Optional.empty());
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
        output.append(indentString(indent)).append(value).append('\n');
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private class Visitor
            extends PlanVisitor<Integer, Void>
    {
        private final Map<Symbol, Type> types;

        public Visitor(Map<Symbol, Type> types)
        {
            this.types = types;
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

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(ImmutableCollectors.toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(ImmutableCollectors.toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(prePartitioned))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(notPrePartitioned));
                }
                args.add(format("partition by (%s)", builder));
            }
            if (!orderBy.isEmpty()) {
                args.add(format("order by (%s)", Stream.concat(
                        node.getOrderBy().stream()
                                .limit(node.getPreSortedOrderPrefix())
                                .map(symbol -> "<" + symbol + ">"),
                        node.getOrderBy().stream()
                                .skip(node.getPreSortedOrderPrefix())
                                .map(Symbol::toString))
                        .collect(Collectors.joining(", "))));
            }

            print(indent, "- Window[%s] => [%s]", Joiner.on(", ").join(args), formatOutputs(node.getOutputSymbols()));

            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                print(indent + 2, "%s := %s(%s)", entry.getKey(), entry.getValue().getName(), Joiner.on(", ").join(entry.getValue().getArguments()));
            }
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTopNRowNumber(final TopNRowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> orderBy = Lists.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            print(indent, "- TopNRowNumber[%s limit %s] => [%s]", Joiner.on(", ").join(args), node.getMaxRowCountPerPartition(), formatOutputs(node.getOutputSymbols()));

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRowNumber(final RowNumberNode node, Integer indent)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s) ", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit (%s) ", node.getMaxRowCountPerPartition().get()));
            }

            print(indent, "- RowNumber[%s] => [%s]", Joiner.on(", ").join(args), formatOutputs(node.getOutputSymbols()));

            print(indent + 2, "%s := %s", node.getRowNumberSymbol(), "row_number()");
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            TableHandle table = node.getTable();
            print(indent, "- TableScan[%s, original constraint=%s] => [%s]", table, node.getOriginalConstraint(), formatOutputs(node.getOutputSymbols()));

            TupleDomain<ColumnHandle> predicate = node.getLayout()
                    .map(metadata::getLayout)
                    .map(TableLayout::getPredicate)
                    .orElse(TupleDomain.<ColumnHandle>all());

            if (node.getLayout().isPresent()) {
                print(indent + 2, "LAYOUT: %s", node.getLayout().get().getConnectorHandle());
            }

            if (predicate.isNone()) {
                print(indent + 2, ":: NONE");
            }
            else {
                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    print(indent + 2, "%s := %s", assignment.getKey(), column);
                    printConstraint(indent + 3, table, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    predicate.getDomains()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                print(indent + 2, "%s", column);
                                printConstraint(indent + 3, table, column, predicate);
                            });
                }
            }

            return null;
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            print(indent, "- Values => [%s]", formatOutputs(node.getOutputSymbols()));
            for (List<Expression> row : node.getRows()) {
                print(indent + 2, "(" + Joiner.on(", ").join(row) + ")");
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
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                if (entry.getValue() instanceof QualifiedNameReference && ((QualifiedNameReference) entry.getValue()).getName().equals(entry.getKey().toQualifiedName())) {
                    // skip identity assignments
                    continue;
                }
                print(indent + 2, "%s := %s", entry.getKey(), entry.getValue());
            }

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitUnnest(UnnestNode node, Integer indent)
        {
            print(indent, "- Unnest [replicate=%s, unnest=%s] => [%s]", formatOutputs(node.getReplicateSymbols()), formatOutputs(node.getUnnestSymbols().keySet()), formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitOutput(OutputNode node, Integer indent)
        {
            print(indent, "- Output[%s] => [%s]", Joiner.on(", ").join(node.getColumnNames()), formatOutputs(node.getOutputSymbols()));
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
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- TopN[%s by (%s)] => [%s]", node.getCount(), Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitSort(final SortNode node, Integer indent)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderBy(), input -> input + " " + node.getOrderings().get(input));

            print(indent, "- Sort[%s] => [%s]", Joiner.on(", ").join(keys), formatOutputs(node.getOutputSymbols()));
            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Integer indent)
        {
            print(indent, "- RemoteSource[%s] => [%s]", Joiner.on(',').join(node.getSourceFragmentIds()), formatOutputs(node.getOutputSymbols()));

            return null;
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
        public Void visitSample(SampleNode node, Integer indent)
        {
            print(indent, "- Sample[%s: %s] => [%s]", node.getSampleType(), node.getSampleRatio(), formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            print(indent, "- Exchange[%s] => %s", node.getType(), formatOutputs(node.getOutputSymbols()));

            return processChildren(node, indent + 1);
        }

        @Override
        public Void visitDelete(DeleteNode node, Integer indent)
        {
            print(indent, "- Delete[%s] => [%s]", node.getTarget(), formatOutputs(node.getOutputSymbols()));

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

        private String formatOutputs(Iterable<Symbol> symbols)
        {
            return Joiner.on(", ").join(Iterables.transform(symbols, input -> input + ":" + types.get(input)));
        }
    }

    private void printConstraint(int indent, TableHandle table, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
    {
        if (!constraint.isAll() && constraint.getDomains().containsKey(column)) {
            print(indent, ":: %s", formatDomain(table, column, simplifyDomain(constraint.getDomains().get(column))));
        }
    }

    private String formatDomain(TableHandle table, ColumnHandle column, Domain domain)
    {
        ImmutableList.Builder<String> parts = ImmutableList.builder();

        if (domain.isNullAllowed()) {
            parts.add("NULL");
        }

        try {
            ColumnMetadata columnMetadata = metadata.getColumnMetadata(table, column);
            MethodHandle method = metadata.getFunctionRegistry().getCoercion(columnMetadata.getType(), VARCHAR)
                    .getMethodHandle();

            for (Range range : domain.getRanges()) {
                StringBuilder builder = new StringBuilder();
                if (range.isSingleValue()) {
                    String value = ((Slice) method.invokeWithArguments(range.getSingleValue())).toStringUtf8();
                    builder.append('[').append(value).append(']');
                }
                else {
                    builder.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');

                    if (range.getLow().isLowerUnbounded()) {
                        builder.append("<min>");
                    }
                    else {
                        builder.append(((Slice) method.invokeWithArguments(range.getLow().getValue())).toStringUtf8());
                    }

                    builder.append(", ");

                    if (range.getHigh().isUpperUnbounded()) {
                        builder.append("<max>");
                    }
                    else {
                        builder.append(((Slice) method.invokeWithArguments(range.getHigh().getValue())).toStringUtf8());
                    }

                    builder.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                }
                parts.add(builder.toString());
            }
        }
        catch (OperatorNotFoundException e) {
            parts.add("<UNREPRESENTABLE VALUE>");
        }
        catch (Throwable e) {
            throw Throwables.propagate(e);
        }

        return "[" + Joiner.on(", ").join(parts.build()) + "]";
    }
}
