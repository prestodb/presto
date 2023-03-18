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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.lang.invoke.MethodHandle;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.variable;
import static com.facebook.presto.type.TypeUtils.NULL_HASH_CODE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.forEachPair;
import static java.util.Arrays.asList;

public class PlannerUtils
{
    public static final long INITIAL_HASH_VALUE = 0;
    public static final String HASH_CODE = OperatorType.HASH_CODE.getFunctionName().getObjectName();

    private PlannerUtils() {}

    public static SortOrder toSortOrder(SortItem sortItem)
    {
        if (sortItem.getOrdering() == SortItem.Ordering.ASCENDING) {
            if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
                return SortOrder.ASC_NULLS_FIRST;
            }
            return SortOrder.ASC_NULLS_LAST;
        }
        if (sortItem.getNullOrdering() == SortItem.NullOrdering.FIRST) {
            return SortOrder.DESC_NULLS_FIRST;
        }
        return SortOrder.DESC_NULLS_LAST;
    }

    public static OrderingScheme toOrderingScheme(OrderBy orderBy, TypeProvider types)
    {
        return toOrderingScheme(
                orderBy.getSortItems().stream()
                        .map(SortItem::getSortKey)
                        .map(item -> {
                            checkArgument(item instanceof SymbolReference, "Sort items in order by must be symbol reference");
                            return variable(getSourceLocation(item), ((SymbolReference) item).getName(), types.get(item));
                        }).collect(toImmutableList()),
                orderBy.getSortItems().stream()
                        .map(PlannerUtils::toSortOrder)
                        .collect(toImmutableList()));
    }

    public static OrderingScheme toOrderingScheme(List<VariableReferenceExpression> orderingSymbols, List<SortOrder> sortOrders)
    {
        ImmutableList.Builder<Ordering> builder = ImmutableList.builder();

        // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
        Set<VariableReferenceExpression> keysSeen = new HashSet<>();

        forEachPair(orderingSymbols.stream(), sortOrders.stream(), (variable, sortOrder) -> {
            if (!keysSeen.contains(variable)) {
                keysSeen.add(variable);
                builder.add(new Ordering(variable, sortOrder));
            }
        });

        return new OrderingScheme(builder.build());
    }

    public static VariableReferenceExpression toVariableReference(Expression expression, TypeProvider types)
    {
        checkArgument(expression instanceof SymbolReference);
        return variable(getSourceLocation(expression), ((SymbolReference) expression).getName(), types.get(expression));
    }

    public static Type createMapType(FunctionAndTypeManager functionAndTypeManager, Type keyType, Type valueType)
    {
        MethodHandle keyNativeEquals = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyNativeHashCode = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        return new MapType(keyType, valueType, keyNativeEquals, keyNativeHashCode);
    }

    public static RowExpression orNullHashCode(RowExpression expression)
    {
        checkArgument(BIGINT.equals(expression.getType()), "expression should be BIGINT type");
        return new SpecialFormExpression(expression.getSourceLocation(), SpecialFormExpression.Form.COALESCE, BIGINT, expression, constant(NULL_HASH_CODE, BIGINT));
    }

    public static Optional<RowExpression> getHashExpression(FunctionAndTypeManager functionAndTypeManager, List<VariableReferenceExpression> variables)
    {
        if (variables.isEmpty()) {
            return Optional.empty();
        }

        RowExpression result = constant(INITIAL_HASH_VALUE, BIGINT);
        for (VariableReferenceExpression variable : variables) {
            RowExpression hashField = call(functionAndTypeManager, HASH_CODE, BIGINT, variable);
            hashField = orNullHashCode(hashField);
            result = call(functionAndTypeManager, "combine_hash", BIGINT, result, hashField);
        }
        return Optional.of(result);
    }

    public static PlanNode projectExpressions(PlanNode source, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator, List<? extends RowExpression> expressions, List<VariableReferenceExpression> variableMap)
    {
        Assignments.Builder assignments = Assignments.builder();
        checkArgument(variableMap.isEmpty() || variableMap.size() == expressions.size());
        int i = 0;
        for (RowExpression expression : expressions) {
            VariableReferenceExpression variable = variableMap.isEmpty() ? variableAllocator.newVariable(expression) : variableMap.get(i++);
            assignments.put(variable, expression);
        }

        return new ProjectNode(
                source.getSourceLocation(),
                planNodeIdAllocator.getNextId(),
                source,
                assignments.build(),
                LOCAL);
    }

    public static PlanNode addProjections(PlanNode source, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator, List<RowExpression> expressions,
            List<VariableReferenceExpression> variablesToUse)
    {
        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression variableReferenceExpression : source.getOutputVariables()) {
            assignments.put(variableReferenceExpression, variableReferenceExpression);
        }

        checkState(variablesToUse.isEmpty() || variablesToUse.size() == expressions.size());
        for (int i = 0; i < expressions.size(); i++) {
            RowExpression expression = expressions.get(i);
            VariableReferenceExpression variable = variablesToUse.isEmpty() ? variableAllocator.newVariable(expression) : variablesToUse.get(i);
            assignments.put(variable, expression);
        }

        return new ProjectNode(
                source.getSourceLocation(),
                planNodeIdAllocator.getNextId(),
                source,
                assignments.build(),
                LOCAL);
    }

    public static PlanNode addAggregation(PlanNode planNode, FunctionAndTypeManager functionAndTypeManager, PlanNodeIdAllocator planNodeIdAllocator, VariableAllocator variableAllocator, String aggregationFunction, Type type, List<VariableReferenceExpression> groupingKeys, VariableReferenceExpression resultVariable, RowExpression... args)
    {
        CallExpression callExpression = call(functionAndTypeManager, aggregationFunction, type, args);
        Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregationMap = ImmutableMap.of(
                resultVariable,
                new AggregationNode.Aggregation(
                        callExpression,
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty()));

        AggregationNode.GroupingSetDescriptor groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(groupingKeys, 1, ImmutableSet.of(1));
        return projectExpressions(
                new AggregationNode(
                    Optional.empty(),
                    planNodeIdAllocator.getNextId(),
                    planNode,
                    aggregationMap,
                    groupingSetDescriptor,
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty()),
                planNodeIdAllocator,
                variableAllocator,
                ImmutableList.of(resultVariable),
                ImmutableList.of());
    }

    private static PlanNode cloneFilterNode(FilterNode filterNode, Session session, Metadata metadata, PlanNodeIdAllocator planNodeIdAllocator, List<VariableReferenceExpression> variablesToKeep, Map<VariableReferenceExpression, VariableReferenceExpression> varMap, PlanNodeIdAllocator idAllocator)
    {
        PlanNode newSource = clonePlanNode(filterNode.getSource(), session, metadata, planNodeIdAllocator, variablesToKeep, varMap);
        return new FilterNode(
                filterNode.getSourceLocation(),
                idAllocator.getNextId(),
                newSource,
                RowExpressionVariableInliner.inlineVariables(varMap, filterNode.getPredicate()));
    }

    private static PlanNode cloneProjectNode(ProjectNode projectNode, Session session, Metadata metadata, PlanNodeIdAllocator planNodeIdAllocator, List<VariableReferenceExpression> fieldsToKeep, Map<VariableReferenceExpression, VariableReferenceExpression> varMap, PlanNodeIdAllocator idAllocator)
    {
        PlanNode newSource = clonePlanNode(projectNode.getSource(), session, metadata, planNodeIdAllocator, fieldsToKeep, varMap);

        Assignments.Builder newAssignments = Assignments.builder();

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectNode.getAssignments().entrySet()) {
            VariableReferenceExpression var = entry.getKey();
            if (!varMap.containsKey(var)) {
                varMap.put(var, var);
            }
            newAssignments.put(varMap.getOrDefault(var, var), RowExpressionVariableInliner.inlineVariables(varMap, entry.getValue()));
        }

        return new ProjectNode(
                idAllocator.getNextId(),
                newSource,
                newAssignments.build());
    }

    private static TableScanNode cloneTableScan(TableScanNode scanNode, Session session, Metadata metadata, PlanNodeIdAllocator planNodeIdAllocator, List<VariableReferenceExpression> fieldsToKeep, Map<VariableReferenceExpression, VariableReferenceExpression> varMap)
    {
        Map<VariableReferenceExpression, ColumnHandle> assignments = scanNode.getAssignments();
        TableLayout scanLayout = metadata.getLayout(session, scanNode.getTable());

        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();

        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignmentsBuilder = ImmutableMap.builder();

        for (VariableReferenceExpression var : scanNode.getOutputVariables()) {
            VariableReferenceExpression newVar = varMap.getOrDefault(var, var);
            outputVariablesBuilder.add(newVar);
            assignmentsBuilder.put(newVar, assignments.get(var));
            varMap.putIfAbsent(var, newVar);
        }

        List<VariableReferenceExpression> newOutputVariables = outputVariablesBuilder.build();
        ImmutableMap<VariableReferenceExpression, ColumnHandle> newAssignments = assignmentsBuilder.build();

        return new TableScanNode(
                scanNode.getSourceLocation(),
                planNodeIdAllocator.getNextId(),
                scanLayout.getNewTableHandle(),
                newOutputVariables,
                newAssignments,
                scanNode.getTableConstraints(),
                scanLayout.getPredicate(),
                scanNode.getEnforcedConstraint());
    }

    public static PlanNode clonePlanNode(PlanNode planNode, Session session, Metadata metadata, PlanNodeIdAllocator planNodeIdAllocator, List<VariableReferenceExpression> fieldsToKeep, Map<VariableReferenceExpression, VariableReferenceExpression> varMap)
    {
        if (planNode instanceof TableScanNode) {
            TableScanNode scanNode = (TableScanNode) planNode;
            return cloneTableScan(scanNode, session, metadata, planNodeIdAllocator, fieldsToKeep, varMap);
        }
        else if (planNode instanceof FilterNode) {
            return cloneFilterNode((FilterNode) planNode, session, metadata, planNodeIdAllocator, fieldsToKeep, varMap, planNodeIdAllocator);
        }
        else if (planNode instanceof ProjectNode) {
            return cloneProjectNode((ProjectNode) planNode, session, metadata, planNodeIdAllocator, fieldsToKeep, varMap, planNodeIdAllocator);
        }

        checkState(false, "Currently cannot clone: " + planNode.getClass().getName() + " nodes.");
        return null;
    }

    public static String getPlanString(PlanNode planNode, Session session, TypeProvider types, Metadata metadata)
    {
        return PlanPrinter.textLogicalPlan(planNode, types, StatsAndCosts.empty(), metadata.getFunctionAndTypeManager(), session, 0);
    }

    private static String getNameHint(Expression expression)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }
        return nameHint;
    }

    public static VariableReferenceExpression newVariable(VariableAllocator variableAllocator, Expression expression, Type type, String suffix)
    {
        return variableAllocator.newVariable(getSourceLocation(expression), getNameHint(expression), type, suffix);
    }

    public static VariableReferenceExpression newVariable(VariableAllocator variableAllocator, Expression expression, Type type)
    {
        return variableAllocator.newVariable(getSourceLocation(expression), getNameHint(expression), type, null);
    }

    public static VariableReferenceExpression newVariable(VariableAllocator variableAllocator, Field field)
    {
        return variableAllocator.newVariable(getSourceLocation(field.getNodeLocation()), field.getName().orElse("field"), field.getType(), null);
    }

    public static VariableReferenceExpression newVariable(VariableAllocator variableAllocator, Optional<SourceLocation> sourceLocation, Field field)
    {
        return variableAllocator.newVariable(sourceLocation, field.getName().orElse("field"), field.getType(), null);
    }

    public static VariableReferenceExpression toVariableReference(VariableAllocator variableAllocator, Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: " + expression);
        String name = ((SymbolReference) expression).getName();
        return variableAllocator.getVariableReferenceExpression(getSourceLocation(expression), name);
    }

    public static Optional<TableScanNode> getTableScanNodeWithOnlyFilterAndProject(PlanNode source)
    {
        if (source instanceof FilterNode) {
            return getTableScanNodeWithOnlyFilterAndProject(((FilterNode) source).getSource());
        }
        if (source instanceof ProjectNode) {
            return getTableScanNodeWithOnlyFilterAndProject(((ProjectNode) source).getSource());
        }
        if (source instanceof TableScanNode) {
            return Optional.of((TableScanNode) source);
        }

        return Optional.empty();
    }

    public static boolean isFilterAboveTableScan(PlanNode node)
    {
        return node instanceof FilterNode && ((FilterNode) node).getSource() instanceof TableScanNode;
    }

    public static boolean isProjectAboveTableScan(PlanNode node)
    {
        return node instanceof ProjectNode && ((ProjectNode) node).getSource() instanceof TableScanNode;
    }

    public static boolean isScanFilterProject(PlanNode node)
    {
        return node instanceof TableScanNode ||
            node instanceof ProjectNode && isScanFilterProject(((ProjectNode) node).getSource()) ||
                node instanceof FilterNode && isScanFilterProject(((FilterNode) node).getSource());
    }

    public static CallExpression equalityPredicate(FunctionResolution functionResolution, RowExpression leftExpr, RowExpression rightExpr)
    {
        return new CallExpression(EQUAL.name(),
                functionResolution.comparisonFunction(ComparisonExpression.Operator.EQUAL, BOOLEAN, BOOLEAN),
                BOOLEAN,
                asList(leftExpr, rightExpr));
    }

    public static RowExpression coalesce(List<RowExpression> expressions)
    {
        return new SpecialFormExpression(SpecialFormExpression.Form.COALESCE, expressions.get(0).getType(), expressions);
    }
}
