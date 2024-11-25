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
import com.facebook.presto.common.Subfield.PathElement;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.ComplexTypeFunctionDescriptor;
import com.facebook.presto.spi.function.LambdaArgumentDescriptor;
import com.facebook.presto.spi.function.LambdaDescriptor;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isLegacyUnnest;
import static com.facebook.presto.SystemSessionProperties.isPushdownSubfieldsEnabled;
import static com.facebook.presto.SystemSessionProperties.isPushdownSubfieldsFromArrayLambdasEnabled;
import static com.facebook.presto.common.Subfield.allSubscripts;
import static com.facebook.presto.common.Subfield.noSubfield;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class PushdownSubfields
        implements PlanOptimizer
{
    public static final QualifiedObjectName CARDINALITY = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "cardinality");
    public static final QualifiedObjectName ELEMENT_AT = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "element_at");
    public static final QualifiedObjectName CAST = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "$operator$cast");
    private final Metadata metadata;
    private boolean isEnabledForTesting;

    public PushdownSubfields(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public void setEnabledForTesting(boolean isSet)
    {
        isEnabledForTesting = isSet;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnabledForTesting || isPushdownSubfieldsEnabled(session);
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");

        if (!isEnabled(session)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }

        Rewriter rewriter = new Rewriter(session, metadata);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(rewriter, plan, new Rewriter.Context());
        return PlanOptimizerResult.optimizerResult(rewrittenPlan, rewriter.isPlanChanged());
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
        private boolean planChanged;

        public Rewriter(Session session, Metadata metadata)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
            this.expressionOptimizer = new RowExpressionOptimizer(metadata);
            this.subfieldExtractor = new SubfieldExtractor(
                    functionResolution,
                    expressionOptimizer,
                    session.toConnectorSession(),
                    metadata.getFunctionAndTypeManager(),
                    isPushdownSubfieldsFromArrayLambdasEnabled(session));
        }

        public boolean isPlanChanged()
        {
            return planChanged;
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
                    .map(EquiJoinClause::getLeft)
                    .forEach(context.get().variables::add);
            node.getCriteria().stream()
                    .map(EquiJoinClause::getRight)
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
        public PlanNode visitCteProducer(CteProducerNode node, RewriteContext<Context> context)
        {
            context.get().variables.addAll(node.getSource().getOutputVariables());
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

                Optional<Subfield> subfield = toSubfield(expression, functionResolution, expressionOptimizer, session.toConnectorSession(), metadata.getFunctionAndTypeManager());
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

                List<Subfield> subfieldsWithoutNoSubfield = subfields.stream().filter(subfield -> !containsNoSubfieldPathElement(subfield)).collect(toList());
                List<Subfield> subfieldsWithNoSubfield = subfields.stream().filter(subfield -> containsNoSubfieldPathElement(subfield)).collect(toList());

                // Prune subfields: if one subfield is a prefix of another subfield, keep the shortest one.
                // Example: {a.b.c, a.b} -> {a.b}
                List<Subfield> columnSubfields = subfieldsWithoutNoSubfield.stream()
                        .filter(subfield -> !prefixExists(subfield, subfieldsWithoutNoSubfield))
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(columnName, path))
                        .collect(toList());

                columnSubfields.addAll(subfieldsWithNoSubfield.stream()
                        .filter(subfield -> !isPrefixOf(dropNoSubfield(subfield), subfieldsWithoutNoSubfield))
                        .map(Subfield::getPath)
                        .map(path -> new Subfield(columnName, path))
                        .collect(toList()));

                planChanged = true;
                newAssignments.put(variable, entry.getValue().withRequiredSubfields(ImmutableList.copyOf(columnSubfields)));
            }

            return new TableScanNode(
                    node.getSourceLocation(),
                    node.getId(),
                    node.getTable(),
                    node.getOutputVariables(),
                    newAssignments.build(),
                    node.getTableConstraints(),
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
                                        .map(path -> new Subfield(container.getName(), ImmutableList.<PathElement>builder()
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

        private static Subfield dropNoSubfield(Subfield subfield)
        {
            return new Subfield(subfield.getRootName(),
                    subfield.getPath().stream().filter(pathElement -> !(pathElement instanceof Subfield.NoSubfield)).collect(toImmutableList()));
        }

        private static boolean containsNoSubfieldPathElement(Subfield subfield)
        {
            return subfield.getPath().stream().anyMatch(pathElement -> pathElement instanceof Subfield.NoSubfield);
        }

        private static boolean prefixExists(Subfield subfieldPath, Collection<Subfield> subfieldPaths)
        {
            return subfieldPaths.stream().anyMatch(path -> path.isPrefix(subfieldPath));
        }

        private static boolean isPrefixOf(Subfield subfieldPath, Collection<Subfield> subfieldPaths)
        {
            return subfieldPaths.stream().anyMatch(subfieldPath::isPrefix);
        }

        private static String getColumnName(Session session, Metadata metadata, TableHandle tableHandle, ColumnHandle columnHandle)
        {
            return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
        }

        private static Optional<Subfield> toSubfield(
                RowExpression expression,
                StandardFunctionResolution functionResolution,
                ExpressionOptimizer expressionOptimizer,
                ConnectorSession connectorSession,
                FunctionAndTypeManager functionAndTypeManager)
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
                if (expression instanceof CallExpression &&
                        isSubscriptOrElementAtFunction((CallExpression) expression, functionResolution, functionAndTypeManager)) {
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
                            //Fix for issue https://github.com/prestodb/presto/issues/22690
                            //Avoid negative index pushdown
                            if (((Number) index).longValue() < 0) {
                                return Optional.empty();
                            }

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
            private final FunctionAndTypeManager functionAndTypeManager;
            private final boolean isPushDownSubfieldsFromLambdasEnabled;

            private SubfieldExtractor(
                    StandardFunctionResolution functionResolution,
                    ExpressionOptimizer expressionOptimizer,
                    ConnectorSession connectorSession,
                    FunctionAndTypeManager functionAndTypeManager,
                    boolean isPushDownSubfieldsFromLambdasEnabled)
            {
                this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
                this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimizer is null");
                this.connectorSession = connectorSession;
                this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
                this.isPushDownSubfieldsFromLambdasEnabled = isPushDownSubfieldsFromLambdasEnabled;
            }

            @Override
            public Void visitCall(CallExpression call, Context context)
            {
                ComplexTypeFunctionDescriptor functionDescriptor = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getDescriptor();
                if (isSubscriptOrElementAtFunction(call, functionResolution, functionAndTypeManager)) {
                    Optional<Subfield> subfield = toSubfield(call, functionResolution, expressionOptimizer, connectorSession, functionAndTypeManager);
                    if (subfield.isPresent()) {
                        if (context.isPruningLambdaSubfieldsPossible()) {
                            addRequiredLambdaSubfields(context, subfield.get());
                        }
                        else {
                            context.subfields.add(subfield.get());
                        }
                    }
                    else {
                        call.getArguments().forEach(argument -> argument.accept(this, context));
                    }
                    return null;
                }
                if (!isPushDownSubfieldsFromLambdasEnabled) {
                    context.setLambdaSubfields(Context.ALL_SUBFIELDS_OF_ARRAY_ELEMENT_OR_MAP_VALUE);
                    call.getArguments().forEach(argument -> argument.accept(this, context));
                    return null;
                }
                Set<Subfield> lambdaSubfieldsOriginal = context.getLambdaSubfields();
                if ((functionDescriptor.isAccessingInputValues() && functionDescriptor.getLambdaDescriptors().isEmpty())) {
                    // If function internally accesses the input values we cannot prune any unaccessed lambda subfields since we do not know what subfields function accessed.
                    context.giveUpOnCollectingLambdaSubfields();
                }

                // We need to apply output to input transformation function in order to make sense of all lambda subfields accessed in outer functions w.r.t. the
                // input of the current function.
                if (functionDescriptor.getOutputToInputTransformationFunction().isPresent()) {
                    Set<Subfield> transformedLambdaSubfields =
                            functionDescriptor.getOutputToInputTransformationFunction().get().apply(context.getLambdaSubfields());
                    context.setLambdaSubfields(ImmutableSet.copyOf(transformedLambdaSubfields));
                }

                Set<Integer> argumentIndicesContainingMapOrArray = functionDescriptor.getArgumentIndicesContainingMapOrArray()
                        .orElse(IntStream.range(0, call.getArguments().size())
                                .filter(argIndex -> isMapOrArrayOfRowType(call.getArguments().get(argIndex)))
                                .boxed()
                                .collect(toImmutableSet()));

                // All the lambda subfields collected in outer functions relate only to the arguments of the function specified in
                // functionDescriptor.argumentIndicesContainingMapOrArray.
                Map<Integer, Set<Subfield>> lambdaSubfieldsFromOuterFunctions = argumentIndicesContainingMapOrArray.stream()
                        .collect(toImmutableMap(callArgumentIndex -> callArgumentIndex, unused -> ImmutableSet.copyOf(context.getLambdaSubfields())));

                // If the function accepts lambdas, add all the lambda subfields from each lambda.
                Map<Integer, Set<Subfield>> lambdaSubfieldsFromCurrentFunction = ImmutableMap.of();
                for (LambdaDescriptor lambdaDescriptor : functionDescriptor.getLambdaDescriptors()) {
                    Optional<Map<Integer, Set<Subfield>>> lambdaSubfields = collectLambdaSubfields(call, lambdaDescriptor);
                    if (!lambdaSubfields.isPresent()) {
                        context.giveUpOnCollectingLambdaSubfields();
                        call.getArguments().forEach(argument -> argument.accept(this, context));
                        return null;
                    }
                    lambdaSubfieldsFromCurrentFunction = merge(lambdaSubfieldsFromCurrentFunction, lambdaSubfields.get());
                }

                Map<Integer, Set<Subfield>> lambdaSubfields = merge(lambdaSubfieldsFromOuterFunctions, lambdaSubfieldsFromCurrentFunction);

                lambdaSubfields = addNoSubfieldIfNoAccessedSubfieldsFound(call, lambdaSubfields);

                // We need to continue visiting the function arguments and collect all lambda subfields in inner function calls as well as non-lambda subfields in all
                // function arguments. Once reached the leaf node, we will try to prune the subfields of the input field, subscript, or subfield.
                for (int callArgumentIndex = 0; callArgumentIndex < call.getArguments().size(); callArgumentIndex++) {
                    // Since context is global during the traversal of all the nodes in expression tree, we need to pass lambda subfields only to those  function
                    // arguments that they relate to.
                    if (lambdaSubfields.containsKey(callArgumentIndex)) {
                        context.setLambdaSubfields(lambdaSubfields.get(callArgumentIndex));
                    }
                    else {
                        context.setLambdaSubfields(Context.ALL_SUBFIELDS_OF_ARRAY_ELEMENT_OR_MAP_VALUE);
                    }
                    call.getArguments().get(callArgumentIndex).accept(this, context);
                }

                // When we are done with inner calls (child nodes) we need to restore lambda subfields we received from parent expression to handle such situations like
                // in example below
                // SELECT * FROM my_table WHERE ANY_MATCH(column1, x -> x.ds > '2023-01-01') AND ALL_MATCH(column2, x -> STRPOS(x.comment,  'Presto') > 0)
                // After we are done with ANY_MATCH, we need to restore the lambda subfields to what we received from parent node 'AND' so that it does not collide with
                // lambda subfields of ALL_MATCH function.
                context.setLambdaSubfields(lambdaSubfieldsOriginal);
                return null;
            }

            private static Map<Integer, Set<Subfield>> merge(Map<Integer, Set<Subfield>> s1, Map<Integer, Set<Subfield>> s2)
            {
                Map<Integer, Set<Subfield>> result = new HashMap<>(s1);
                s2.forEach((callArgumentIndex, subfields) -> result.merge(
                        callArgumentIndex,
                        subfields,
                        (lambdaSubfields1, lambdaSubfields2) -> ImmutableSet.<Subfield>builder().addAll(lambdaSubfields1).addAll(lambdaSubfields2).build()));
                return ImmutableMap.copyOf(result);
            }

            private static Map<Integer, Set<Subfield>> addNoSubfieldIfNoAccessedSubfieldsFound(CallExpression call, Map<Integer, Set<Subfield>> argumentIndexToLambdaSubfieldsMap)
            {
                ImmutableMap.Builder<Integer, Set<Subfield>> argumentIndexToLambdaSubfieldsMapBuilder = ImmutableMap.builder();
                for (Integer callArgumentIndex : argumentIndexToLambdaSubfieldsMap.keySet()) {
                    if (!argumentIndexToLambdaSubfieldsMap.get(callArgumentIndex).isEmpty()) {
                        argumentIndexToLambdaSubfieldsMapBuilder.put(callArgumentIndex, argumentIndexToLambdaSubfieldsMap.get(callArgumentIndex));
                    }
                    else {
                        RowExpression argument = call.getArguments().get(callArgumentIndex);
                        if (isMapOrArrayOfRowType(argument)) {
                            argumentIndexToLambdaSubfieldsMapBuilder.put(callArgumentIndex, ImmutableSet.of(new Subfield("", ImmutableList.of(allSubscripts(), noSubfield()))));
                        }
                    }
                }
                return argumentIndexToLambdaSubfieldsMapBuilder.build();
            }

            private static boolean isMapOrArrayOfRowType(RowExpression argument)
            {
                return (argument.getType() instanceof ArrayType && ((ArrayType) argument.getType()).getElementType() instanceof RowType) ||
                        (argument.getType() instanceof MapType && ((MapType) argument.getType()).getValueType() instanceof RowType);
            }

            private Optional<Map<Integer, Set<Subfield>>> collectLambdaSubfields(CallExpression call, LambdaDescriptor lambdaDescriptor)
            {
                Map<Integer, Set<Subfield>> argumentIndexToLambdaSubfieldsMap = new HashMap<>();
                if (!(call.getArguments().get(lambdaDescriptor.getCallArgumentIndex()) instanceof LambdaDefinitionExpression)) {
                    // In this case, we cannot prune the subfields because the function can potentially access all subfields
                    return Optional.empty();
                }
                LambdaDefinitionExpression lambda = (LambdaDefinitionExpression) call.getArguments().get(lambdaDescriptor.getCallArgumentIndex());

                Context subContext = new Context();
                lambda.getBody().accept(this, subContext);
                for (int lambdaArgumentIndex : lambdaDescriptor.getLambdaArgumentDescriptors().keySet()) {
                    final LambdaArgumentDescriptor lambdaArgumentDescriptor = lambdaDescriptor.getLambdaArgumentDescriptors().get(lambdaArgumentIndex);
                    int callArgumentIndex = lambdaArgumentDescriptor.getCallArgumentIndex();
                    argumentIndexToLambdaSubfieldsMap.putIfAbsent(callArgumentIndex, new HashSet<>());
                    String root = lambda.getArguments().get(lambdaArgumentIndex);
                    if (subContext.variables.stream().anyMatch(variable -> variable.getName().equals(root))) {
                        // The entire struct was accessed.
                        return Optional.empty();
                    }
                    Set<Subfield> transformedLambdaSubfields = lambdaArgumentDescriptor.getLambdaArgumentToInputTransformationFunction().apply(
                            subContext.subfields.stream()
                                    .filter(x -> x.getRootName().equals(root))
                                    .collect(toImmutableSet()));
                    argumentIndexToLambdaSubfieldsMap.get(callArgumentIndex).addAll(transformedLambdaSubfields);
                }
                return Optional.of(ImmutableMap.copyOf(argumentIndexToLambdaSubfieldsMap));
            }

            @Override
            public Void visitSpecialForm(SpecialFormExpression specialForm, Context context)
            {
                if (specialForm.getForm() == IS_NULL) {
                    if (specialForm.getArguments().get(0) instanceof VariableReferenceExpression && specialForm.getArguments().get(0).getType() instanceof RowType) {
                        context.subfields.add(new Subfield(((VariableReferenceExpression) specialForm.getArguments().get(0)).getName(), ImmutableList.of(noSubfield())));
                        return null;
                    }
                }
                else if (specialForm.getForm() != DEREFERENCE) {
                    specialForm.getArguments().forEach(argument -> argument.accept(this, context));
                    return null;
                }

                Optional<Subfield> subfield = toSubfield(specialForm, functionResolution, expressionOptimizer, connectorSession, functionAndTypeManager);

                if (subfield.isPresent()) {
                    if (context.isPruningLambdaSubfieldsPossible()) {
                        addRequiredLambdaSubfields(context, subfield.get());
                    }
                    else {
                        context.subfields.add(subfield.get());
                    }
                }
                else {
                    specialForm.getArguments().forEach(argument -> argument.accept(this, context));
                }
                return null;
            }

            /**
             * Adds lambda subfields from the context to the list of the required subfields of the field/subscript/subfield provided in parameter 'input'. This function should be
             * invoked
             * once we reached leaf node while visiting the expression tree. Effectively, it prunes all unaccessed subfields of the 'input'.
             *
             * @param context - SubfieldExtractor context
             * @param input - input field, subscript, or subfield, for which lambda subfields were collected.
             */
            private void addRequiredLambdaSubfields(Context context, Subfield input)
            {
                Set<Subfield> lambdaSubfields = context.getLambdaSubfields();
                for (Subfield lambdaSubfield : lambdaSubfields) {
                    List<PathElement> newPath = ImmutableList.<PathElement>builder()
                            .addAll(input.getPath())
                            .addAll(lambdaSubfield.getPath())
                            .build();
                    context.subfields.add(new Subfield(input.getRootName(), newPath));
                }
            }

            @Override
            public Void visitVariableReference(VariableReferenceExpression reference, Context context)
            {
                if (context.isPruningLambdaSubfieldsPossible()) {
                    addRequiredLambdaSubfields(context, toSubfield(reference, functionResolution, expressionOptimizer, connectorSession, functionAndTypeManager).get());
                    return null;
                }
                context.variables.add(reference);
                return null;
            }
        }

        private static final class Context
        {
            public static final Set<Subfield> ALL_SUBFIELDS_OF_ARRAY_ELEMENT_OR_MAP_VALUE = ImmutableSet.of(new Subfield("", ImmutableList.of(allSubscripts())));
            // Variables whose subfields cannot be pruned
            private final Set<VariableReferenceExpression> variables = new HashSet<>();
            private final Set<Subfield> subfields = new HashSet<>();
            private Set<Subfield> lambdaSubfields = ALL_SUBFIELDS_OF_ARRAY_ELEMENT_OR_MAP_VALUE;

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
                        .map(path -> new Subfield(subfield.getRootName(), ImmutableList.<PathElement>builder()
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

            public void setLambdaSubfields(Set<Subfield> lambdaSubfields)
            {
                this.lambdaSubfields = lambdaSubfields;
            }

            public Set<Subfield> getLambdaSubfields()
            {
                return lambdaSubfields;
            }

            private void giveUpOnCollectingLambdaSubfields()
            {
                setLambdaSubfields(ALL_SUBFIELDS_OF_ARRAY_ELEMENT_OR_MAP_VALUE);
            }

            private boolean isPruningLambdaSubfieldsPossible()
            {
                return !getLambdaSubfields().isEmpty() &&
                        getLambdaSubfields().stream()
                                .noneMatch(
                                        subfield -> subfield.getPath().stream()
                                                .skip(subfield.getPath().size() - 1)
                                                .anyMatch(pathElement -> pathElement.equals(allSubscripts())));
            }
        }
    }

    private static boolean isSubscriptOrElementAtFunction(CallExpression expression, StandardFunctionResolution functionResolution, FunctionAndTypeManager functionAndTypeManager)
    {
        return functionResolution.isSubscriptFunction(expression.getFunctionHandle()) ||
                functionAndTypeManager.getFunctionMetadata(expression.getFunctionHandle()).getName().equals(ELEMENT_AT);
    }
}
