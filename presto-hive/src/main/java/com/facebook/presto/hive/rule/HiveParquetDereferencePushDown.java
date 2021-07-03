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
package com.facebook.presto.hive.rule;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.expressions.RowExpressionTreeRewriter.rewriteWith;
import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.SYNTHESIZED;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetDereferencePushdownEnabled;
import static com.facebook.presto.hive.HiveStorageFormat.PARQUET;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.parquet.ParquetTypeUtils.pushdownColumnNameForSubfield;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class HiveParquetDereferencePushDown
        implements ConnectorPlanOptimizer
{
    private final HiveTransactionManager transactionManager;
    private final RowExpressionService rowExpressionService;

    public HiveParquetDereferencePushDown(HiveTransactionManager transactionManager, RowExpressionService rowExpressionService)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
    }

    private static Map<RowExpression, Subfield> extractDereferences(
            Map<String, HiveColumnHandle> regularHiveColumnHandles,
            ConnectorSession session, ExpressionOptimizer expressionOptimizer,
            Set<RowExpression> expressions)
    {
        Set<RowExpression> dereferenceAndVariableExpressions = new HashSet<>();
        expressions.forEach(e -> e.accept(new ExtractDereferenceAndVariables(session, expressionOptimizer), dereferenceAndVariableExpressions));

        // keep prefix only expressions
        List<RowExpression> dereferences = dereferenceAndVariableExpressions.stream()
                .filter(expression -> !prefixExists(expression, dereferenceAndVariableExpressions))
                .filter(expression -> expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE)
                .collect(Collectors.toList());

        return dereferences.stream().collect(toMap(identity(), dereference -> createNestedColumn(
                regularHiveColumnHandles, dereference, expressionOptimizer, session)));
    }

    private static boolean prefixExists(RowExpression expression, Set<RowExpression> allExpressions)
    {
        int[] referenceCount = {0};
        expression.accept(
                new DefaultRowExpressionTraversalVisitor<int[]>()
                {
                    @Override
                    public Void visitSpecialForm(SpecialFormExpression specialForm, int[] context)
                    {
                        if (specialForm.getForm() != DEREFERENCE) {
                            return super.visitSpecialForm(specialForm, context);
                        }

                        if (allExpressions.contains(specialForm)) {
                            referenceCount[0] += 1;
                        }

                        RowExpression base = specialForm.getArguments().get(0);
                        base.accept(this, context);
                        return null;
                    }

                    @Override
                    public Void visitVariableReference(VariableReferenceExpression reference, int[] context)
                    {
                        if (allExpressions.contains(reference)) {
                            referenceCount[0] += 1;
                        }
                        return null;
                    }
                }, referenceCount);

        return referenceCount[0] > 1;
    }

    private static Subfield createNestedColumn(Map<String, HiveColumnHandle> regularHiveColumnHandles,
                                               RowExpression rowExpression, ExpressionOptimizer expressionOptimizer,
                                               ConnectorSession session)
    {
        if (!(rowExpression instanceof SpecialFormExpression) || ((SpecialFormExpression) rowExpression).getForm() != DEREFERENCE) {
            throw new IllegalArgumentException("expecting SpecialFormExpression(DEREFERENCE), but got: " + rowExpression);
        }

        List<Subfield.PathElement> elements = new ArrayList<>();
        while (true) {
            if (rowExpression instanceof VariableReferenceExpression) {
                Collections.reverse(elements);
                String name = ((VariableReferenceExpression) rowExpression).getName();
                HiveColumnHandle handle = regularHiveColumnHandles.get(name);
                checkArgument(handle != null, "Missing Hive column handle: " + name);
                String originalColumnName = regularHiveColumnHandles.get(name).getName();
                return new Subfield(originalColumnName, unmodifiableList(elements));
            }

            if (rowExpression instanceof SpecialFormExpression && ((SpecialFormExpression) rowExpression).getForm() == DEREFERENCE) {
                SpecialFormExpression dereferenceExpression = (SpecialFormExpression) rowExpression;
                RowExpression base = dereferenceExpression.getArguments().get(0);
                RowType baseType = (RowType) base.getType();

                RowExpression indexExpression = expressionOptimizer.optimize(
                        dereferenceExpression.getArguments().get(1),
                        ExpressionOptimizer.Level.OPTIMIZED,
                        session);

                if (indexExpression instanceof ConstantExpression) {
                    Object index = ((ConstantExpression) indexExpression).getValue();
                    if (index instanceof Number) {
                        Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                        if (fieldName.isPresent()) {
                            elements.add(new Subfield.NestedField(fieldName.get()));
                            rowExpression = base;
                            continue;
                        }
                    }
                }
            }
            break;
        }

        throw new IllegalArgumentException("expecting SpecialFormExpression(DEREFERENCE) with constants for indices, but got: " + rowExpression);
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return maxSubplan.accept(new Visitor(session, variableAllocator, idAllocator), null);
    }

    private boolean isParquetDereferenceEnabled(ConnectorSession session, TableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle, "Dereference pushdown into reader is not supported on a non-hive TableHandle");

        if (!isParquetDereferencePushdownEnabled(session)) {
            return false;
        }

        return PARQUET == getHiveStorageFormat(getMetadata(tableHandle).getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
    }

    protected HiveMetadata getMetadata(TableHandle tableHandle)
    {
        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");
        return (HiveMetadata) metadata;
    }

    /**
     * Visitor to extract all dereference expressions and variable references.
     * <p>
     * If a dereference expression contains dereference expression, inner dereference expression are not returned
     * * sub(deref(deref(x, 1), 2)) --> deref(deref(x,1), 2)
     * Variable expressions returned are the ones not referenced by the dereference expressions
     * * sub(x + 1) --> x
     * * sub(deref(x, 1)) -> deref(x,1)
     */
    private static class ExtractDereferenceAndVariables
            extends DefaultRowExpressionTraversalVisitor<Set<RowExpression>>
    {
        private final ConnectorSession connectorSession;
        private final ExpressionOptimizer expressionOptimizer;

        public ExtractDereferenceAndVariables(ConnectorSession connectorSession, ExpressionOptimizer expressionOptimizer)
        {
            this.connectorSession = connectorSession;
            this.expressionOptimizer = expressionOptimizer;
        }

        @Override
        public Void visitSpecialForm(SpecialFormExpression specialForm, Set<RowExpression> context)
        {
            if (specialForm.getForm() != DEREFERENCE) {
                return super.visitSpecialForm(specialForm, context);
            }

            RowExpression expression = specialForm;
            while (true) {
                if (expression instanceof VariableReferenceExpression) {
                    context.add(specialForm);
                    return null;
                }

                if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE) {
                    SpecialFormExpression dereferenceExpression = (SpecialFormExpression) expression;
                    RowExpression base = dereferenceExpression.getArguments().get(0);
                    RowType baseType = (RowType) base.getType();

                    RowExpression indexExpression = expressionOptimizer.optimize(
                            dereferenceExpression.getArguments().get(1),
                            ExpressionOptimizer.Level.OPTIMIZED,
                            connectorSession);

                    if (indexExpression instanceof ConstantExpression) {
                        Object index = ((ConstantExpression) indexExpression).getValue();
                        if (index instanceof Number) {
                            Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                            if (fieldName.isPresent()) {
                                expression = base;
                                continue;
                            }
                        }
                    }
                }
                break;
            }

            return super.visitSpecialForm(specialForm, context);
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Set<RowExpression> context)
        {
            context.add(reference);
            return null;
        }
    }

    private static class DereferenceExpressionRewriter
            extends RowExpressionRewriter<Void>
    {
        private final Map<RowExpression, VariableReferenceExpression> dereferenceMap;

        public DereferenceExpressionRewriter(Map<RowExpression, VariableReferenceExpression> dereferenceMap)
        {
            this.dereferenceMap = dereferenceMap;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialFormExpression node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
        {
            return dereferenceMap.get(node);
        }
    }

    /**
     * Looks for ProjectNode -> TableScanNode patterns. Goes through the project expressions to extract out the DEREFERENCE expressions,
     * pushes the dereferences down to TableScan and creates new project expressions with the pushed down column coming from the TableScan.
     * Returned plan nodes could contain unreferenced outputs which will be pruned later in the planning process.
     */
    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final VariableAllocator variableAllocator;
        private final PlanNodeIdAllocator idAllocator;

        Visitor(ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitPlan(PlanNode node, Void context)
        {
            ImmutableList.Builder<PlanNode> children = ImmutableList.builder();
            boolean changed = false;
            for (PlanNode child : node.getSources()) {
                PlanNode newChild = child.accept(this, null);
                if (newChild != child) {
                    changed = true;
                }
                children.add(newChild);
            }

            if (!changed) {
                return node;
            }
            return node.replaceChildren(children.build());
        }

        @Override
        public PlanNode visitProject(ProjectNode project, Void context)
        {
            if (!(project.getSource() instanceof TableScanNode)) {
                return visitPlan(project, context);
            }

            TableScanNode tableScan = (TableScanNode) project.getSource();
            if (!isParquetDereferenceEnabled(session, tableScan.getTable())) {
                return visitPlan(project, context);
            }
            Map<String, HiveColumnHandle> regularHiveColumnHandles = new HashMap<>();
            regularHiveColumnHandles.putAll(tableScan.getAssignments().entrySet().stream()
                    .collect(toMap(e -> e.getKey().getName(), e -> (HiveColumnHandle) e.getValue())));
            regularHiveColumnHandles.putAll(tableScan.getAssignments().values().stream()
                    .map(columnHandle -> (HiveColumnHandle) columnHandle)
                    .collect(toMap(HiveColumnHandle::getName, identity())));

            Map<RowExpression, Subfield> dereferenceToNestedColumnMap = extractDereferences(
                    regularHiveColumnHandles,
                    session,
                    rowExpressionService.getExpressionOptimizer(),
                    new HashSet<>(project.getAssignments().getExpressions()));
            if (dereferenceToNestedColumnMap.isEmpty()) {
                return visitPlan(project, context);
            }

            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>(tableScan.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> newAssignments = new HashMap<>(tableScan.getAssignments());

            Map<RowExpression, VariableReferenceExpression> dereferenceToVariableMap = new HashMap<>();

            for (Map.Entry<RowExpression, Subfield> dereference : dereferenceToNestedColumnMap.entrySet()) {
                Subfield nestedColumn = dereference.getValue();
                RowExpression dereferenceExpression = dereference.getKey();

                // Find the nested column Hive Type
                HiveColumnHandle regularColumnHandle = regularHiveColumnHandles.get(nestedColumn.getRootName());
                if (regularColumnHandle == null) {
                    throw new IllegalArgumentException("nested column [" + nestedColumn + "]'s base column " + nestedColumn.getRootName() + " is not present in table scan output");
                }

                Optional<HiveType> nestedColumnHiveType = regularHiveColumnHandles.get(nestedColumn.getRootName())
                        .getHiveType()
                        .findChildType(
                                nestedColumn.getPath().stream()
                                        .map(p -> ((Subfield.NestedField) p).getName())
                                        .collect(Collectors.toList()));

                if (!nestedColumnHiveType.isPresent()) {
                    throw new IllegalArgumentException("nested column [" + nestedColumn + "] type is not present in Hive column type");
                }

                String pushdownColumnName = pushdownColumnNameForSubfield(nestedColumn);
                // Create column handle for nested column
                HiveColumnHandle nestedColumnHandle = new HiveColumnHandle(
                        pushdownColumnName,
                        nestedColumnHiveType.get(),
                        dereferenceExpression.getType().getTypeSignature(),
                        -1,
                        SYNTHESIZED,
                        Optional.of("nested column pushdown"),
                        ImmutableList.of(nestedColumn),
                        Optional.empty());

                VariableReferenceExpression newOutputVariable = variableAllocator.newVariable(pushdownColumnName, dereferenceExpression.getType());
                newOutputVariables.add(newOutputVariable);
                newAssignments.put(newOutputVariable, nestedColumnHandle);

                dereferenceToVariableMap.put(dereferenceExpression, newOutputVariable);
            }

            TableScanNode newTableScan = new TableScanNode(
                    idAllocator.getNextId(),
                    tableScan.getTable(),
                    newOutputVariables,
                    newAssignments,
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint());

            Assignments.Builder newProjectAssignmentBuilder = Assignments.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
                RowExpression newExpression = rewriteWith(new DereferenceExpressionRewriter(dereferenceToVariableMap), entry.getValue());
                newProjectAssignmentBuilder.put(entry.getKey(), newExpression);
            }

            return new ProjectNode(idAllocator.getNextId(), newTableScan, newProjectAssignmentBuilder.build(), project.getLocality());
        }
    }
}
