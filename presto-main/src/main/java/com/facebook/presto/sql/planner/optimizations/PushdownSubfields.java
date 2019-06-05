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

import com.facebook.presto.Session;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.Subfield.NestedField;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.Subfield.allSubscripts;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class PushdownSubfields
        implements PlanOptimizer
{
    private final Metadata metadata;

    public PushdownSubfields(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");

        return SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata, types), plan, new Rewriter.Context());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Rewriter.Context>
    {
        static final class Context
        {
            private final Set<Symbol> symbols = new HashSet<>();
            private final Set<Subfield> subfields = new HashSet<>();

            private void addSymbol(Symbol newSymbol, Symbol existingSymbol)
            {
                if (symbols.contains(existingSymbol)) {
                    symbols.add(newSymbol);
                    return;
                }

                List<Subfield> matchingSubfields = findSubfields(existingSymbol.getName());
                verify(!matchingSubfields.isEmpty(), "Missing symbol: " + existingSymbol);

                matchingSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(newSymbol.getName(), path))
                        .forEach(subfields::add);
            }

            private void addSubfield(Subfield newSubfield, Symbol existingSymbol)
            {
                if (symbols.contains(existingSymbol)) {
                    subfields.add(newSubfield);
                    return;
                }

                List<Subfield> matchingSubfields = findSubfields(existingSymbol.getName());
                verify(!matchingSubfields.isEmpty(), "Missing symbol: " + existingSymbol);

                matchingSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(newSubfield.getRootName(), ImmutableList.<Subfield.PathElement>builder()
                                .addAll(newSubfield.getPath())
                                .addAll(path)
                                .build()))
                        .forEach(subfields::add);
            }

            private List<Subfield> findSubfields(String rootName)
            {
                return subfields.stream()
                        .filter(subfield -> rootName.equals(subfield.getRootName()))
                        .collect(toImmutableList());
            }
        }

        private final Session session;
        private final Metadata metadata;
        private final TypeProvider types;
        private final SubfieldExtractor subfieldExtractor = new SubfieldExtractor();

        public Rewriter(Session session, Metadata metadata, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getGroupingKeys());

            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                aggregation.getArguments().forEach(expression -> subfieldExtractor.process(expression, context.get()));

                aggregation.getFilter().ifPresent(expression -> subfieldExtractor.process(expression, context.get()));

                aggregation.getOrderBy()
                        .map(OrderingScheme::getOrderBy)
                        .ifPresent(context.get().symbols::addAll);

                aggregation.getMask().ifPresent(context.get().symbols::add);
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getCorrelation());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getDistinctSymbols());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getSource().getOutputSymbols());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Context> context)
        {
            subfieldExtractor.process(castToExpression(node.getPredicate()), context.get());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<Context> context)
        {
            for (Map.Entry<Symbol, Symbol> entry : node.getGroupingColumns().entrySet()) {
                context.get().addSymbol(entry.getValue(), entry.getKey());
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Context> context)
        {
            node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .forEach(context.get().symbols::add);
            node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .forEach(context.get().symbols::add);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Context> context)
        {
            node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .forEach(context.get().symbols::add);
            node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .forEach(context.get().symbols::add);

            node.getFilter()
                    .map(OriginalExpressionUtils::castToExpression)
                    .ifPresent(expression -> subfieldExtractor.process(expression, context.get()));

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getDistinctSymbols());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getOutputSymbols());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                Expression expression = entry.getValue();

                if (expression instanceof SymbolReference) {
                    context.get().addSymbol(Symbol.from(expression), symbol);
                    continue;
                }

                Optional<Subfield> subfield = toSubfield(expression);
                if (subfield.isPresent()) {
                    context.get().addSubfield(subfield.get(), symbol);
                    continue;
                }

                subfieldExtractor.process(expression, context.get());
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Context> context)
        {
            context.get().symbols.add(node.getRowNumberSymbol());
            context.get().symbols.addAll(node.getPartitionBy());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Context> context)
        {
            context.get().symbols.add(node.getSourceJoinSymbol());
            context.get().symbols.add(node.getFilteringSourceJoinSymbol());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getOrderingScheme().getOrderBy());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode node, RewriteContext<Context> context)
        {
            subfieldExtractor.process(castToExpression(node.getFilter()), context.get());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Context> context)
        {
            if (context.get().subfields.isEmpty()) {
                return node;
            }

            ImmutableMap.Builder<Symbol, ColumnHandle> newAssignments = ImmutableMap.builder();

            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                Symbol symbol = entry.getKey();
                if (context.get().symbols.contains(symbol)) {
                    newAssignments.put(entry);
                    continue;
                }

                List<Subfield> subfields = context.get().findSubfields(symbol.getName());

                verify(!subfields.isEmpty(), "Missing symbol: " + symbol);

                String columnName = getColumnName(session, metadata, node.getTable(), entry.getValue());

                // Prune subfields: if one subfield is a prefix of another subfield, keep the shortest one.
                // Example: {a.b.c, a.b} -> {a.b}
                List<Subfield> columnSubfields = subfields.stream()
                        .filter(subfield -> !prefixExists(subfield, subfields))
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(columnName, path))
                        .collect(toImmutableList());

                newAssignments.put(symbol, entry.getValue().withRequiredSubfields(columnSubfields));
            }

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    newAssignments.build(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.isTemporaryTable());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getColumns());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getOrderingScheme().getOrderBy());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Context> context)
        {
            context.get().symbols.add(node.getRowNumberSymbol());
            context.get().symbols.addAll(node.getPartitionBy());
            context.get().symbols.addAll(node.getOrderingScheme().getOrderBy());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Context> context)
        {
            for (Map.Entry<Symbol, Collection<Symbol>> entry : node.getSymbolMapping().asMap().entrySet()) {
                entry.getValue().forEach(symbol -> context.get().addSymbol(symbol, entry.getKey()));
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Context> context)
        {
            ImmutableList.Builder<Subfield> newSubfields = ImmutableList.builder();
            for (Map.Entry<Symbol, List<Symbol>> entry : node.getUnnestSymbols().entrySet()) {
                Symbol container = entry.getKey();
                boolean found = false;

                if (isRowType(container)) {
                    for (Symbol field : entry.getValue()) {
                        if (context.get().symbols.contains(field)) {
                            found = true;
                            newSubfields.add(new Subfield(container.getName(), ImmutableList.of(allSubscripts(), new NestedField(field.getName()))));
                        }
                        else {
                            List<Subfield> matchingSubfields = context.get().findSubfields(field.getName());
                            if (!matchingSubfields.isEmpty()) {
                                found = true;
                                matchingSubfields.stream()
                                        .map(Subfield::getPath)
                                        .map(path -> new Subfield(container.getName(), ImmutableList.<Subfield.PathElement>builder()
                                                .add(allSubscripts())
                                                .add(new NestedField(field.getName()))
                                                .addAll(path)
                                                .build()))
                                        .forEach(newSubfields::add);
                            }
                        }
                    }
                }
                else {
                    for (Symbol field : entry.getValue()) {
                        if (context.get().symbols.contains(field)) {
                            found = true;
                            context.get().symbols.add(container);
                        }
                        else {
                            List<Subfield> matchingSubfields = context.get().findSubfields(field.getName());

                            if (!matchingSubfields.isEmpty()) {
                                found = true;
                                matchingSubfields.stream()
                                        .map(Subfield::getPath)
                                        .map(path -> new Subfield(container.getName(), ImmutableList.<Subfield.PathElement>builder()
                                                .add(allSubscripts())
                                                .addAll(path)
                                                .build()))
                                        .forEach(newSubfields::add);
                            }
                        }
                    }
                }
                if (!found) {
                    context.get().symbols.add(container);
                }
            }
            context.get().subfields.addAll(newSubfields.build());

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            context.get().symbols.addAll(node.getSpecification().getPartitionBy());

            node.getSpecification().getOrderingScheme()
                    .map(OrderingScheme::getOrderBy)
                    .ifPresent(context.get().symbols::addAll);

            node.getWindowFunctions().values().stream()
                    .map(WindowNode.Function::getFunctionCall)
                    .map(CallExpression::getArguments)
                    .flatMap(List::stream)
                    .map(OriginalExpressionUtils::castToExpression)
                    .forEach(expression -> subfieldExtractor.process(expression, context.get()));

            node.getWindowFunctions().values().stream()
                    .map(WindowNode.Function::getFrame)
                    .map(WindowNode.Frame::getStartValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(context.get().symbols::add);

            node.getWindowFunctions().values().stream()
                    .map(WindowNode.Function::getFrame)
                    .map(WindowNode.Frame::getEndValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(context.get().symbols::add);

            return context.defaultRewrite(node, context.get());
        }

        private boolean isRowType(Symbol symbol)
        {
            Type sourceType = types.get(symbol);
            return sourceType instanceof ArrayType && ((ArrayType) sourceType).getElementType() instanceof RowType;
        }

        private static boolean prefixExists(Subfield subfieldPath, Collection<Subfield> subfieldPaths)
        {
            return subfieldPaths.stream()
                    .filter(path -> path.isPrefix(subfieldPath))
                    .findAny()
                    .isPresent();
        }

        private static String getColumnName(Session session, Metadata metadata, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
        }

        private static final class SubfieldExtractor
                extends DefaultExpressionTraversalVisitor<Void, Context>
        {
            @Override
            protected Void visitSubscriptExpression(SubscriptExpression node, Context context)
            {
                Optional<Subfield> subfield = toSubfield(node);
                if (subfield.isPresent()) {
                    context.subfields.add(subfield.get());
                }
                else {
                    process(node.getBase(), context);
                    process(node.getIndex(), context);
                }
                return null;
            }

            @Override
            protected Void visitDereferenceExpression(DereferenceExpression node, Context context)
            {
                Optional<Subfield> subfield = toSubfield(node);
                if (subfield.isPresent()) {
                    context.subfields.add(subfield.get());
                }
                else {
                    process(node.getBase(), context);
                    process(node.getField(), context);
                }
                return null;
            }

            @Override
            protected Void visitSymbolReference(SymbolReference node, Context context)
            {
                context.symbols.add(Symbol.from(node));
                return null;
            }
        }

        private static Optional<Subfield> toSubfield(Node expression)
        {
            ImmutableList.Builder<Subfield.PathElement> elements = ImmutableList.builder();
            while (true) {
                if (expression instanceof SymbolReference) {
                    return Optional.of(new Subfield(((SymbolReference) expression).getName(), elements.build().reverse()));
                }

                if (expression instanceof DereferenceExpression) {
                    DereferenceExpression dereference = (DereferenceExpression) expression;
                    elements.add(new NestedField(dereference.getField().getValue()));
                    expression = dereference.getBase();
                }
                else if (expression instanceof SubscriptExpression) {
                    SubscriptExpression subscript = (SubscriptExpression) expression;
                    Expression index = subscript.getIndex();
                    if (index instanceof Cast) {
                        index = ((Cast) index).getExpression();
                    }
                    if (index instanceof LongLiteral) {
                        elements.add(new Subfield.LongSubscript(((LongLiteral) index).getValue()));
                    }
                    else if (index instanceof StringLiteral) {
                        elements.add(new Subfield.StringSubscript(((StringLiteral) index).getValue()));
                    }
                    else if (index instanceof GenericLiteral) {
                        GenericLiteral literal = (GenericLiteral) index;
                        if (BIGINT.getTypeSignature().equals(TypeSignature.parseTypeSignature(literal.getType()))) {
                            elements.add(new Subfield.LongSubscript(Long.valueOf(literal.getValue())));
                        }
                        else {
                            return Optional.empty();
                        }
                    }
                    else {
                        return Optional.empty();
                    }
                    expression = subscript.getBase();
                }
                else {
                    return Optional.empty();
                }
            }
        }
    }
}
