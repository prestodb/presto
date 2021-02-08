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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.Subfield.NestedField;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.SpatialJoinNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isLegacyUnnest;
import static com.facebook.presto.SystemSessionProperties.isPushdownSubfieldsEnabled;
import static com.facebook.presto.common.Subfield.allSubscripts;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.google.common.base.Preconditions.checkState;
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

        return SimplePlanRewriter.rewriteWith(new Rewriter(session, metadata), plan, new Rewriter.Context());
    }

    private static class Rewriter
            extends SimplePlanRewriter<Rewriter.Context>
    {
        private final Session session;
        private final Metadata metadata;
        private final StandardFunctionResolution functionResolution;
        private final ExpressionOptimizer expressionOptimizer;
        private final SubfieldExtractor subfieldExtractor;
        private static final QualifiedObjectName ARBITRARY_AGGREGATE_FUNCTION = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "arbitrary");

        public Rewriter(Session session, Metadata metadata)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
            this.expressionOptimizer = new RowExpressionOptimizer(metadata);
            this.subfieldExtractor = new SubfieldExtractor(functionResolution, expressionOptimizer, session.toConnectorSession());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getGroupingKeys());

            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                AggregationNode.Aggregation aggregation = entry.getValue();

                // Allow sub-field pruning to pass through the arbitrary() aggregation
                QualifiedObjectName aggregateName = metadata.getFunctionAndTypeManager().getFunctionMetadata(aggregation.getCall().getFunctionHandle()).getName();
                if (ARBITRARY_AGGREGATE_FUNCTION.equals(aggregateName)) {
                    checkState(aggregation.getArguments().get(0) instanceof VariableReferenceExpression);
                    context.get().addAssignment(variable, (VariableReferenceExpression) aggregation.getArguments().get(0));
                }
                else {
                    aggregation.getArguments().forEach(expression -> expression.accept(subfieldExtractor, context.get()));
                }

                aggregation.getFilter().ifPresent(expression -> expression.accept(subfieldExtractor, context.get()));

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
            node.getPredicate().accept(subfieldExtractor, context.get());
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
                    .ifPresent(expression -> expression.accept(subfieldExtractor, context.get()));

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
                RowExpression expression = entry.getValue();

                if (expression instanceof VariableReferenceExpression) {
                    context.get().addAssignment(variable, (VariableReferenceExpression) expression);
                    continue;
                }

                Optional<Subfield> subfield = toSubfield(expression, functionResolution, expressionOptimizer, session.toConnectorSession());
                if (subfield.isPresent()) {
                    context.get().addAssignment(variable, subfield.get());
                    continue;
                }

                expression.accept(subfieldExtractor, context.get());
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
            node.getFilter().accept(subfieldExtractor, context.get());
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
            for (Map.Entry<VariableReferenceExpression, List<VariableReferenceExpression>> entry : node.getVariableMapping().entrySet()) {
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

                if (isRowType(container) && !isLegacyUnnest(session)) {
                    for (VariableReferenceExpression field : entry.getValue()) {
                        if (context.get().variables.contains(field)) {
                            found = true;
                            newSubfields.add(new Subfield(container.getName(), ImmutableList.of(allSubscripts(), nestedField(field.getName()))));
                        }
                        else {
                            List<Subfield> matchingSubfields = context.get().findSubfields(field.getName());
                            if (!matchingSubfields.isEmpty()) {
                                found = true;
                                matchingSubfields.stream()
                                        .map(Subfield::getPath)
                                        .map(path -> new Subfield(container.getName(), ImmutableList.<Subfield.PathElement>builder()
                                                .add(allSubscripts())
                                                .add(nestedField(field.getName()))
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
                    .forEach(expression -> expression.accept(subfieldExtractor, context.get()));

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

        private static Optional<Subfield> toSubfield(
                RowExpression expression,
                StandardFunctionResolution functionResolution,
                ExpressionOptimizer expressionOptimizer,
                ConnectorSession connectorSession)
        {
            ImmutableList.Builder<Subfield.PathElement> elements = ImmutableList.builder();
            while (true) {
                if (expression instanceof VariableReferenceExpression) {
                    return Optional.of(new Subfield(((VariableReferenceExpression) expression).getName(), elements.build().reverse()));
                }

                if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE) {
                    SpecialFormExpression dereference = (SpecialFormExpression) expression;
                    RowExpression base = dereference.getArguments().get(0);
                    RowType baseType = (RowType) base.getType();

                    RowExpression indexExpression = expressionOptimizer.optimize(
                            dereference.getArguments().get(1),
                            ExpressionOptimizer.Level.OPTIMIZED,
                            connectorSession);

                    if (indexExpression instanceof ConstantExpression) {
                        Object index = ((ConstantExpression) indexExpression).getValue();
                        verify(index != null, "Struct field index cannot be null");
                        if (index instanceof Number) {
                            Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                            if (fieldName.isPresent()) {
                                elements.add(nestedField(fieldName.get()));
                                expression = base;
                                continue;
                            }
                        }
                    }
                    return Optional.empty();
                }
                if (expression instanceof CallExpression && functionResolution.isSubscriptFunction(((CallExpression) expression).getFunctionHandle())) {
                    List<RowExpression> arguments = ((CallExpression) expression).getArguments();
                    RowExpression indexExpression = expressionOptimizer.optimize(
                            arguments.get(1),
                            ExpressionOptimizer.Level.OPTIMIZED,
                            connectorSession);

                    if (indexExpression instanceof ConstantExpression) {
                        Object index = ((ConstantExpression) indexExpression).getValue();
                        if (index == null) {
                            return Optional.empty();
                        }
                        if (index instanceof Number) {
                            elements.add(new Subfield.LongSubscript(((Number) index).longValue()));
                            expression = arguments.get(0);
                            continue;
                        }

                        if (isVarcharType(indexExpression.getType())) {
                            elements.add(new Subfield.StringSubscript(((Slice) index).toStringUtf8()));
                            expression = arguments.get(0);
                            continue;
                        }
                    }
                    return Optional.empty();
                }

                return Optional.empty();
            }
        }

        private static NestedField nestedField(String name)
        {
            return new NestedField(name.toLowerCase(Locale.ENGLISH));
        }

        private static final class SubfieldExtractor
                extends DefaultRowExpressionTraversalVisitor<Context>
        {
            private final StandardFunctionResolution functionResolution;
            private final ExpressionOptimizer expressionOptimizer;
            private final ConnectorSession connectorSession;

            private SubfieldExtractor(StandardFunctionResolution functionResolution, ExpressionOptimizer expressionOptimizer, ConnectorSession connectorSession)
            {
                this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
                this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
                this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
            }

            @Override
            public Void visitCall(CallExpression call, Context context)
            {
                if (!functionResolution.isSubscriptFunction(call.getFunctionHandle())) {
                    call.getArguments().forEach(argument -> argument.accept(this, context));
                    return null;
                }

                // visit subscript expressions only
                Optional<Subfield> subfield = toSubfield(call, functionResolution, expressionOptimizer, connectorSession);
                if (subfield.isPresent()) {
                    context.subfields.add(subfield.get());
                }
                else {
                    call.getArguments().forEach(argument -> argument.accept(this, context));
                }
                return null;
            }

            @Override
            public Void visitSpecialForm(SpecialFormExpression specialForm, Context context)
            {
                if (specialForm.getForm() != DEREFERENCE) {
                    specialForm.getArguments().forEach(argument -> argument.accept(this, context));
                    return null;
                }

                Optional<Subfield> subfield = toSubfield(specialForm, functionResolution, expressionOptimizer, connectorSession);
                if (subfield.isPresent()) {
                    context.subfields.add(subfield.get());
                }
                else {
                    specialForm.getArguments().forEach(argument -> argument.accept(this, context));
                }
                return null;
            }

            @Override
            public Void visitVariableReference(VariableReferenceExpression reference, Context context)
            {
                context.variables.add(reference);
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
