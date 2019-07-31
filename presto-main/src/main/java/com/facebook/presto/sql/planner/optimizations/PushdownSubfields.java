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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.Subfield.NestedField;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
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

import static com.facebook.presto.SystemSessionProperties.isPushdownSubfieldsEnabled;
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
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");

        if (!isPushdownSubfieldsEnabled(session)) {
            return plan;
        }

        return SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata, types), plan, new Rewriter.Context());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Rewriter.Context>
    {
        private final Session session;
        private final Metadata metadata;
        private final TypeProvider types;
        private final SubfieldExtractor subfieldExtractor;

        public Rewriter(Session session, Metadata metadata, TypeProvider types)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
            this.subfieldExtractor = new SubfieldExtractor(types);
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getGroupingKeys());

            for (AggregationNode.Aggregation aggregation : node.getAggregations().values()) {
                aggregation.getArguments().forEach(expression -> subfieldExtractor.process(castToExpression(expression), context.get()));

                aggregation.getFilter().ifPresent(expression -> subfieldExtractor.process(castToExpression(expression), context.get()));

                aggregation.getOrderBy()
                        .map(OrderingScheme::getOrderByVariables)
                        .ifPresent(context.get().variables::addAll);

                aggregation.getMask().ifPresent(context.get().variables::add);
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getCorrelation());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getDistinctVariables());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getSource().getOutputVariables());
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
            for (Map.Entry<VariableReferenceExpression, VariableReferenceExpression> entry : node.getGroupingColumns().entrySet()) {
                context.get().addAssignment(entry.getKey(), entry.getValue());
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Context> context)
        {
            node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getProbe)
                    .forEach(context.get().variables::add);
            node.getCriteria().stream()
                    .map(IndexJoinNode.EquiJoinClause::getIndex)
                    .forEach(context.get().variables::add);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Context> context)
        {
            node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getLeft)
                    .forEach(context.get().variables::add);
            node.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::getRight)
                    .forEach(context.get().variables::add);

            node.getFilter()
                    .map(OriginalExpressionUtils::castToExpression)
                    .ifPresent(expression -> subfieldExtractor.process(expression, context.get()));

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getDistinctVariables());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getOutputVariables());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Context> context)
        {
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                Expression expression = castToExpression(entry.getValue());

                if (expression instanceof SymbolReference) {
                    context.get().addAssignment(variable, new VariableReferenceExpression(((SymbolReference) expression).getName(), types.get(expression)));
                    continue;
                }

                Optional<Subfield> subfield = toSubfield(expression);
                if (subfield.isPresent()) {
                    context.get().addAssignment(variable, subfield.get());
                    continue;
                }

                subfieldExtractor.process(expression, context.get());
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Context> context)
        {
            context.get().variables.add(node.getRowNumberVariable());
            context.get().variables.addAll(node.getPartitionBy());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Context> context)
        {
            context.get().variables.add(node.getSourceJoinVariable());
            context.get().variables.add(node.getFilteringSourceJoinVariable());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getOrderingScheme().getOrderByVariables());
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

            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> newAssignments = ImmutableMap.builder();

            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                if (context.get().variables.contains(variable)) {
                    newAssignments.put(entry);
                    continue;
                }

                List<Subfield> subfields = context.get().findSubfields(variable.getName());

                verify(!subfields.isEmpty(), "Missing variable: " + variable);

                String columnName = getColumnName(session, metadata, node.getTable(), entry.getValue());

                // Prune subfields: if one subfield is a prefix of another subfield, keep the shortest one.
                // Example: {a.b.c, a.b} -> {a.b}
                List<Subfield> columnSubfields = subfields.stream()
                        .filter(subfield -> !prefixExists(subfield, subfields))
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(columnName, path))
                        .collect(toImmutableList());

                newAssignments.put(variable, entry.getValue().withRequiredSubfields(columnSubfields));
            }

            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputVariables(),
                    newAssignments.build(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getColumns());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getOrderingScheme().getOrderByVariables());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Context> context)
        {
            context.get().variables.add(node.getRowNumberVariable());
            context.get().variables.addAll(node.getPartitionBy());
            context.get().variables.addAll(node.getOrderingScheme().getOrderByVariables());
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<Context> context)
        {
            for (Map.Entry<VariableReferenceExpression, Collection<VariableReferenceExpression>> entry : node.getVariableMapping().asMap().entrySet()) {
                entry.getValue().forEach(variable -> context.get().addAssignment(entry.getKey(), variable));
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Context> context)
        {
            ImmutableList.Builder<Subfield> newSubfields = ImmutableList.builder();
            for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getUnnestVariables().entrySet()) {
                VariableReferenceExpression container = entry.getKey();
                boolean found = false;

                if (isRowType(container)) {
                    for (VariableReferenceExpression field : entry.getValue()) {
                        if (context.get().variables.contains(field)) {
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
                    for (VariableReferenceExpression field : entry.getValue()) {
                        if (context.get().variables.contains(field)) {
                            found = true;
                            context.get().variables.add(container);
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
                    context.get().variables.add(container);
                }
            }
            context.get().subfields.addAll(newSubfields.build());

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getSpecification().getPartitionBy());

            node.getSpecification().getOrderingScheme()
                    .map(OrderingScheme::getOrderByVariables)
                    .ifPresent(context.get().variables::addAll);

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
                    .forEach(context.get().variables::add);

            node.getWindowFunctions().values().stream()
                    .map(WindowNode.Function::getFrame)
                    .map(WindowNode.Frame::getEndValue)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .forEach(context.get().variables::add);

            return context.defaultRewrite(node, context.get());
        }

        private boolean isRowType(VariableReferenceExpression variable)
        {
            return variable.getType() instanceof ArrayType && ((ArrayType) variable.getType()).getElementType() instanceof RowType;
        }

        private static boolean prefixExists(Subfield subfieldPath, Collection<Subfield> subfieldPaths)
        {
            return subfieldPaths.stream().anyMatch(path -> path.isPrefix(subfieldPath));
        }

        private static String getColumnName(Session session, Metadata metadata, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
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

        private static final class SubfieldExtractor
                extends DefaultExpressionTraversalVisitor<Void, Context>
        {
            private final TypeProvider typeProvider;

            private SubfieldExtractor(TypeProvider typeProvider)
            {
                this.typeProvider = requireNonNull(typeProvider, "typeProvider is null");
            }

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
                context.variables.add(new VariableReferenceExpression(node.getName(), typeProvider.get(node)));
                return null;
            }
        }

        private static final class Context
        {
            // Variables whose subfields cannot be pruned
            private final Set<VariableReferenceExpression> variables = new HashSet<>();
            private final Set<Subfield> subfields = new HashSet<>();

            private void addAssignment(VariableReferenceExpression variable, VariableReferenceExpression otherVariable)
            {
                if (variables.contains(variable)) {
                    variables.add(otherVariable);
                    return;
                }

                List<Subfield> matchingSubfields = findSubfields(variable.getName());
                verify(!matchingSubfields.isEmpty(), "Missing variable: " + variable);

                matchingSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(otherVariable.getName(), path))
                        .forEach(subfields::add);
            }

            private void addAssignment(VariableReferenceExpression variable, Subfield subfield)
            {
                if (variables.contains(variable)) {
                    subfields.add(subfield);
                    return;
                }

                List<Subfield> matchingSubfields = findSubfields(variable.getName());
                verify(!matchingSubfields.isEmpty(), "Missing variable: " + variable);

                matchingSubfields.stream()
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(subfield.getRootName(), ImmutableList.<Subfield.PathElement>builder()
                                .addAll(subfield.getPath())
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
    }
}
