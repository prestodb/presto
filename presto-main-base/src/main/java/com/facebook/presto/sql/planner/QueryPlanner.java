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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.MergeHandle;
import com.facebook.presto.spi.NewTableLayout;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.RowChangeParadigm;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.ExpressionTreeUtils;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldId;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.MergeProcessorNode;
import com.facebook.presto.sql.planner.plan.MergeWriterNode;
import com.facebook.presto.sql.planner.plan.OffsetNode;
import com.facebook.presto.sql.planner.plan.UpdateNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Delete;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FrameBound;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Merge;
import com.facebook.presto.sql.tree.MergeCase;
import com.facebook.presto.sql.tree.MergeInsert;
import com.facebook.presto.sql.tree.MergeUpdate;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.Offset;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.Update;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.sql.tree.WindowFrame;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isSkipRedundantSort;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.ConnectorMergeSink.INSERT_OPERATION_NUMBER;
import static com.facebook.presto.spi.ConnectorMergeSink.UPDATE_OPERATION_NUMBER;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_LIMIT_CLAUSE;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.isNumericType;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.PlanBuilder.newPlanBuilder;
import static com.facebook.presto.sql.planner.PlannerUtils.newVariable;
import static com.facebook.presto.sql.planner.PlannerUtils.toOrderingScheme;
import static com.facebook.presto.sql.planner.PlannerUtils.toSortOrder;
import static com.facebook.presto.sql.planner.PlannerUtils.toVariableReference;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.TranslateExpressionsUtil.analyzeCallExpressionTypes;
import static com.facebook.presto.sql.planner.TranslateExpressionsUtil.toRowExpression;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toBoundType;
import static com.facebook.presto.sql.planner.optimizations.WindowNodeUtil.toWindowType;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.sql.tree.IntervalLiteral.IntervalField.DAY;
import static com.facebook.presto.sql.tree.IntervalLiteral.IntervalField.YEAR;
import static com.facebook.presto.sql.tree.IntervalLiteral.Sign.POSITIVE;
import static com.facebook.presto.sql.tree.WindowFrame.Type.GROUPS;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.sql.tree.WindowFrame.Type.ROWS;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Streams.stream;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class QueryPlanner
{
    private final Analysis analysis;
    private final VariableAllocator variableAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap;
    private final Metadata metadata;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final SqlPlannerContext sqlPlannerContext;
    private final SqlParser sqlParser;

    QueryPlanner(
            Analysis analysis,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap,
            Metadata metadata,
            Session session,
            SqlPlannerContext sqlPlannerContext,
            SqlParser sqlParser)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToVariableMap, "lambdaDeclarationToVariableMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(sqlParser, "sqlParser is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToVariableMap = lambdaDeclarationToVariableMap;
        this.metadata = metadata;
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser);
        this.sqlPlannerContext = sqlPlannerContext;
        this.sqlParser = sqlParser;
    }

    public RelationPlan plan(Query query)
    {
        PlanBuilder builder = planQueryBody(query);

        List<Expression> orderBy = analysis.getOrderByExpressions(query);
        builder = handleSubqueries(builder, query, orderBy);
        List<Expression> outputs = analysis.getOutputExpressions(query);
        builder = handleSubqueries(builder, query, outputs);
        builder = project(builder, Iterables.concat(orderBy, outputs));
        builder = sort(builder, query);
        builder = offset(builder, query.getOffset());
        builder = limit(builder, query);
        builder = project(builder, analysis.getOutputExpressions(query));

        return new RelationPlan(builder.getRoot(), analysis.getScope(query), computeOutputs(builder, analysis.getOutputExpressions(query)));
    }

    public RelationPlan plan(QuerySpecification node)
    {
        PlanBuilder builder = planFrom(node);
        RelationPlan fromRelationPlan = builder.getRelationPlan();

        builder = filter(builder, analysis.getWhere(node), node);
        builder = aggregate(builder, node);
        builder = filter(builder, analysis.getHaving(node), node);

        builder = window(builder, node);

        List<Expression> outputs = analysis.getOutputExpressions(node);
        builder = handleSubqueries(builder, node, outputs);

        if (node.getOrderBy().isPresent()) {
            if (!analysis.isAggregation(node)) {
                // ORDER BY requires both output and source fields to be visible if there are no aggregations
                builder = project(builder, outputs, fromRelationPlan);
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()));
            }
            else {
                // ORDER BY requires output fields, groups and translated aggregations to be visible for queries with aggregation
                List<Expression> orderByAggregates = analysis.getOrderByAggregates(node.getOrderBy().get());
                builder = project(builder, Iterables.concat(outputs, orderByAggregates));
                outputs = toSymbolReferences(computeOutputs(builder, outputs));
                List<Expression> complexOrderByAggregatesToRemap = orderByAggregates.stream()
                        .filter(expression -> !analysis.isColumnReference(expression))
                        .collect(toImmutableList());
                builder = planBuilderFor(builder, analysis.getScope(node.getOrderBy().get()), complexOrderByAggregatesToRemap);
            }

            builder = window(builder, node.getOrderBy().get());
        }

        List<Expression> orderBy = analysis.getOrderByExpressions(node);
        builder = handleSubqueries(builder, node, orderBy);
        builder = project(builder, Iterables.concat(orderBy, outputs));

        builder = distinct(builder, node);
        builder = sort(builder, node);
        builder = offset(builder, node.getOffset());
        builder = limit(builder, node);
        builder = project(builder, outputs);

        return new RelationPlan(builder.getRoot(), analysis.getScope(node), computeOutputs(builder, outputs));
    }

    public DeleteNode plan(Delete node)
    {
        RelationType descriptor = analysis.getOutputDescriptor(node.getTable());
        TableHandle handle = analysis.getTableHandle(node.getTable());

        // add table columns
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        for (Field field : descriptor.getAllFields()) {
            VariableReferenceExpression variable = variableAllocator.newVariable(getSourceLocation(field.getNodeLocation()), field.getName().get(), field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
            fields.add(field);
        }

        // add rowId column
        Optional<ColumnHandle> rowIdHandle = metadata.getDeleteRowIdColumn(session, handle);
        Optional<Field> rowIdField = Optional.empty();
        if (rowIdHandle.isPresent()) {
            Type rowIdType = metadata.getColumnMetadata(session, handle, rowIdHandle.get()).getType();
            rowIdField = Optional.of(Field.newUnqualified(node.getLocation(), Optional.empty(), rowIdType));
            VariableReferenceExpression rowIdVariable = variableAllocator.newVariable(getSourceLocation(node), "$rowId", rowIdType);
            outputVariablesBuilder.add(rowIdVariable);
            columns.put(rowIdVariable, rowIdHandle.get());
            fields.add(rowIdField.get());
        }

        // create table scan
        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        PlanNode tableScan = new TableScanNode(getSourceLocation(node), idAllocator.getNextId(), handle, outputVariables, columns.build(), TupleDomain.all(), TupleDomain.all(), Optional.empty());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields.build())).build();
        RelationPlan relationPlan = new RelationPlan(tableScan, scope, outputVariables);

        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);
        translations.setFieldMappings(relationPlan.getFieldMappings());

        PlanBuilder builder = new PlanBuilder(translations, relationPlan.getRoot());

        if (node.getWhere().isPresent()) {
            builder = filter(builder, node.getWhere().get(), node);
        }

        // create delete node
        PlanBuilder finalBuilder = builder;
        Optional<VariableReferenceExpression> rowId = rowIdField.map(f ->
                new VariableReferenceExpression(Optional.empty(), finalBuilder.translate(new FieldReference(relationPlan.getDescriptor().indexOf(f))).getName(), f.getType()));
        List<VariableReferenceExpression> deleteNodeOutputVariables = ImmutableList.of(
                variableAllocator.newVariable("partialrows", BIGINT),
                variableAllocator.newVariable("fragment", VARBINARY));

        return new DeleteNode(getSourceLocation(node), idAllocator.getNextId(), finalBuilder.getRoot(), rowId, deleteNodeOutputVariables, Optional.empty());
    }

    public UpdateNode plan(Update node)
    {
        RelationType descriptor = analysis.getOutputDescriptor(node.getTable());
        TableHandle handle = analysis.getTableHandle(node.getTable());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, handle);
        List<ColumnMetadata> updatedColumnMetadata = analysis.getUpdatedColumns()
                .orElseThrow(() -> new VerifyException("updated columns not set"));
        Set<String> updatedColumnNames = updatedColumnMetadata.stream().map(ColumnMetadata::getName).collect(toImmutableSet());
        List<ColumnHandle> updatedColumns = columnHandles.entrySet().stream()
                .filter(entry -> updatedColumnNames.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());
        handle = metadata.beginUpdate(session, handle, updatedColumns);

        List<String> targetColumnNames = node.getAssignments().stream()
                .map(assignment -> assignment.getName().getValue())
                .collect(toImmutableList());

        // Create lists of columnnames and SET expressions, in table column order
        ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        ImmutableList.Builder<Field> fields = ImmutableList.builder();
        ImmutableList.Builder<Expression> orderedColumnValuesBuilder = ImmutableList.builder();
        for (Field field : descriptor.getAllFields()) {
            String name = field.getName().get();
            VariableReferenceExpression variable = variableAllocator.newVariable(getSourceLocation(field.getNodeLocation()), name, field.getType());
            outputVariablesBuilder.add(variable);
            columns.put(variable, analysis.getColumn(field));
            fields.add(field);
            int index = targetColumnNames.indexOf(name);
            if (index >= 0) {
                orderedColumnValuesBuilder.add(node.getAssignments().get(index).getValue());
            }
        }
        List<Expression> orderedColumnValues = orderedColumnValuesBuilder.build();

        // add rowId column
        Optional<ColumnHandle> rowIdHandle = metadata.getUpdateRowIdColumn(session, handle, updatedColumns);
        Optional<Field> rowIdField = Optional.empty();
        if (rowIdHandle.isPresent()) {
            Type rowIdType = metadata.getColumnMetadata(session, handle, rowIdHandle.get()).getType();
            rowIdField = Optional.of(Field.newUnqualified(node.getLocation(), Optional.empty(), rowIdType));
            VariableReferenceExpression rowIdVariable = variableAllocator.newVariable(getSourceLocation(node), "$rowId", rowIdType);
            outputVariablesBuilder.add(rowIdVariable);
            columns.put(rowIdVariable, rowIdHandle.get());
            fields.add(rowIdField.get());
        }

        // create table scan
        List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
        PlanNode tableScan = new TableScanNode(getSourceLocation(node), idAllocator.getNextId(), handle, outputVariables, columns.build(), TupleDomain.all(), TupleDomain.all(), Optional.empty());
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(fields.build())).build();
        RelationPlan relationPlan = new RelationPlan(tableScan, scope, outputVariables);

        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);
        translations.setFieldMappings(relationPlan.getFieldMappings());
        PlanBuilder builder = new PlanBuilder(translations, relationPlan.getRoot());

        if (node.getWhere().isPresent()) {
            builder = filter(builder, node.getWhere().get(), node);
        }

        builder = builder.appendProjections(orderedColumnValues, variableAllocator, idAllocator, session, metadata, sqlParser, analysis, sqlPlannerContext);

        PlanAndMappings planAndMappings = coerce(builder, orderedColumnValues, analysis, idAllocator, variableAllocator, metadata);
        builder = planAndMappings.getSubPlan();

        ImmutableList.Builder<VariableReferenceExpression> updatedColumnValuesBuilder = ImmutableList.builder();
        orderedColumnValues.forEach(columnValue -> updatedColumnValuesBuilder.add(planAndMappings.get(columnValue)));
        PlanBuilder finalBuilder = builder;
        Optional<VariableReferenceExpression> rowId = rowIdField.map(f ->
                new VariableReferenceExpression(Optional.empty(), finalBuilder.translate(new FieldReference(relationPlan.getDescriptor().indexOf(f))).getName(), f.getType()));
        rowId.ifPresent(r -> updatedColumnValuesBuilder.add(r));

        List<VariableReferenceExpression> outputs = ImmutableList.of(
                variableAllocator.newVariable("partialrows", BIGINT),
                variableAllocator.newVariable("fragment", VARBINARY));

        Optional<PlanNodeId> tableScanId = getIdForLeftTableScan(relationPlan.getRoot());
        checkArgument(tableScanId.isPresent(), "tableScanId not present");

        // create update node
        return new UpdateNode(
                getSourceLocation(node),
                idAllocator.getNextId(),
                finalBuilder.getRoot(),
                rowId,
                updatedColumnValuesBuilder.build(),
                outputs);
    }

    /**
     * Plan a MERGE statement. The MERGE statement is processed by creating a RIGHT JOIN between the target table and the source.
     * Example of converting a MERGE statement into a SELECT statement with a RIGHT JOIN:
     * Merge statement:
     *    MERGE INTO <target_table> t USING <source_table> s
     *    ON (t.<column> = s.<column>)
     *    WHEN MATCHED THEN
     *        UPDATE SET <column1> = s.<column1> + t.<column1>,
     *                              <column2> = s.<column2>
     *    WHEN NOT MATCHED THEN
     *         INSERT (column1, column2, column3)
     *         VALUES (s.column1, s.column2, s.column3);
     *
     * SELECT statement with a RIGHT JOIN created to process the previous MERGE statement:
     *    SELECT
     *        CASE
     *            WHEN present THEN
     *                -- Update column values:                  present=true,     operation UPDATE=3,   case_number=0
     *                row(t.column1, s.column1 + t.column1, s.column2, true, 3, 0)
     *            WHEN (present IS NULL) THEN
     *                -- Insert column values:                     present=false;    operation INSERT=1,     case_number=1
     *                row(s.column1, s.column2, s.column3, false, 1, 1)
     *            ELSE
     *                -- Null values for no case matched:  present=false,    operation=-1,                 case_number=-1
     *                row(null, null, null, false, -1, -1)
     *            END
     *    FROM
     *        (SELECT *, true AS present FROM <target_table>) t
     *        RIGHT JOIN <source_table> s
     *        ON s.<column> = t.<column>;
     *
     * @param mergeStmt the MERGE statement to plan into a MergeWriterNode.
     * @return a MergeWriterNode that represents the plan for the MERGE statement.
     */
    public MergeWriterNode plan(Merge mergeStmt)
    {
        //                                 MergeWriterNode
        //                                             |
        //                              MergeProcessorNode
        //                                             |
        //                       FilterDuplicateMatchingRows
        //                                             |
        //                                MarkDistinctNode
        //                                             |
        //                                  RightEquiJoin
        //                                     /             \
        //       ProjectPresentColumn          \
        //                      |                               \
        //           AssignUniqueID                   \
        //                      |                                 \
        //              TableScan                  TableScan
        //                      |                                 |
        //             (target table)             (source table)
        Analysis.MergeAnalysis mergeAnalysis = analysis.getMergeAnalysis().orElseThrow(() -> new IllegalArgumentException("analysis.getMergeAnalysis() isn't present"));

        Table targetTable = mergeStmt.getTargetTable();
        TableHandle targetTableHandle = analysis.getTableHandle(targetTable);

        List<ColumnMetadata> updatedColumnMetadata = analysis.getUpdatedColumns()
                .orElseThrow(() -> new VerifyException("updated columns not set"));

        Set<String> updatedColumnNames = updatedColumnMetadata.stream().map(ColumnMetadata::getName).collect(toImmutableSet());

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, targetTableHandle);
        List<ColumnHandle> updatedColumns = columnHandles.entrySet().stream()
                .filter(entry -> updatedColumnNames.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .collect(toImmutableList());

        Optional<MergeHandle> mergeHandle = Optional.of(metadata.beginMerge(session, targetTableHandle, updatedColumns));

        targetTableHandle = mergeHandle.get().getTableHandle();

        analysis.registerTable(targetTable, targetTableHandle);

        // Make the plan for the merge target table scan
        RelationPlan targetTableRelationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                .process(mergeStmt.getTarget(), sqlPlannerContext);

        // Assign a unique id to every target table row
        VariableReferenceExpression uniqueIdSymbol = variableAllocator.newVariable("unique_id", BIGINT);
        AssignUniqueId assignUniqueRowIdToTargetTable = new AssignUniqueId(getSourceLocation(mergeStmt), idAllocator.getNextId(), targetTableRelationPlan.getRoot(), uniqueIdSymbol);
        RelationPlan relationPlanWithUniqueRowIds = new RelationPlan(
                assignUniqueRowIdToTargetTable,
                mergeAnalysis.getTargetTableScope(),
                targetTableRelationPlan.getFieldMappings());

        // Project the "present" column
        Assignments.Builder projections = Assignments.builder();
        relationPlanWithUniqueRowIds.getRoot().getOutputVariables().forEach(variable ->
                // TODO #20578: Is it necessary to create a new instance of VariableReferenceExpression? Maybe using 'projections.put(variable, variable)' would be enough.
//                projections.put(variable, new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), variable.getType()))
                  projections.put(variable, variable));

        VariableReferenceExpression presentColumn = variableAllocator.newVariable("present", BOOLEAN);
        projections.put(presentColumn, rowExpression(TRUE_LITERAL, sqlPlannerContext));

        ProjectNode projectNodeWithUniqueRowIdsAndPresentColumn =
                new ProjectNode(idAllocator.getNextId(), relationPlanWithUniqueRowIds.getRoot(), projections.build());
        RelationPlan planWithWithUniqueRowIdsAndPresentColumn = new RelationPlan(
                projectNodeWithUniqueRowIdsAndPresentColumn,
                mergeAnalysis.getTargetTableScope(),
                relationPlanWithUniqueRowIds.getFieldMappings());

        RelationPlan sourceRelationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                .process(mergeStmt.getSource(), sqlPlannerContext);

        RelationPlan joinRelationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                .planJoin(
                        coerceIfNecessary(analysis, mergeStmt.getPredicate(), mergeStmt.getPredicate()),
                        Join.Type.RIGHT, mergeAnalysis.getJoinScope(),
                        planWithWithUniqueRowIdsAndPresentColumn, sourceRelationPlan,
                        mergeStmt, sqlPlannerContext);

        PlanBuilder subPlan = newPlanBuilder(joinRelationPlan, analysis, lambdaDeclarationToVariableMap);

        // Build the SearchedCaseExpression that creates the project merge_row

        ImmutableList.Builder<WhenClause> whenClauses = ImmutableList.builder();
        for (int caseNumber = 0; caseNumber < mergeStmt.getMergeCases().size(); caseNumber++) {
            MergeCase mergeCase = mergeStmt.getMergeCases().get(caseNumber);

            // TODO #20578: Trino uses the following code because they support adding extra conditions to cases.
            //                           In Presto, we'll only support one condition "MERGE INTO ON(<condition>)".
//            Optional<Expression> casePredicate = Optional.empty();
//            if (mergeCase.getExpression().isPresent()) {
//                Expression original = mergeCase.getExpression().get();
//                Expression predicate = coerceIfNecessary(analysis, original, original);
//                casePredicate = Optional.of(predicate);
//                subPlan = subqueryPlanner.handleSubqueries(subPlan, predicate, analysis.getSubqueries(mergeStmt));
//            }

            ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
            List<List<ColumnHandle>> mergeCaseColumnsHandles = mergeAnalysis.getMergeCaseColumnHandles();
            List<ColumnHandle> mergeCaseSetColumns = mergeCaseColumnsHandles.get(caseNumber);
            for (ColumnHandle dataColumnHandle : mergeAnalysis.getDataColumnHandles()) {
                int index = mergeCaseSetColumns.indexOf(dataColumnHandle);
                if (index >= 0) {
                    Expression original = mergeCase.getSetExpressions().get(index);
                    Expression setExpression = coerceIfNecessary(analysis, original, original);
                    subPlan = subqueryPlanner.handleSubqueries(subPlan, setExpression, mergeStmt, sqlPlannerContext);
                    rowBuilder.add(subPlan.rewrite(setExpression));
                }
                else {
                    Integer fieldNumber = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(dataColumnHandle), "Field number for ColumnHandle is null");
                    rowBuilder.add(createSymbolReference(planWithWithUniqueRowIdsAndPresentColumn.getFieldMappings().get(fieldNumber)));
                }
            }

            // Build the match condition for the MERGE case

            // TODO #20578: The call to "createSymbolReference(presentColumn)" method is repeated fourth times. Check if is possible to do it only once.

            // Add a boolean column which is true if a target table row was matched
            rowBuilder.add(new IsNotNullPredicate(createSymbolReference(presentColumn)));

            // Add the operation number
            rowBuilder.add(new GenericLiteral("TINYINT", String.valueOf(getMergeCaseOperationNumber(mergeCase))));

            // Add the mergeStmt case number, needed by MarkDistinct
            rowBuilder.add(new GenericLiteral("INTEGER", String.valueOf(caseNumber)));

//            Optional<Expression> rewritten = casePredicate.map(subPlan::rewrite);  // TODO #20578: Trino uses the following code because they support adding extra conditions to cases.
                                                                                                                                   //                           In Presto, we'll only support one condition "MERGE INTO ON(<condition>)".
            Expression condition = createSymbolReference(presentColumn);
            if (mergeCase instanceof MergeInsert) {
                condition = new IsNullPredicate(createSymbolReference(presentColumn));
            }

// TODO #20578: Trino uses the following code because they support adding extra conditions to cases.
//                           In Presto, we'll only support one condition "MERGE INTO ON(<condition>)".
//            if (rewritten.isPresent()) {
//                condition = ExpressionUtils.and(condition, rewritten.get());
//            }

            whenClauses.add(new WhenClause(condition, new Row(rowBuilder.build())));
        }

// TODO #20578: Presto doesn't need this because we don't allow extra conditions in the MERGE.
//        // Build the "else" clause for the SearchedCaseExpression
//        ImmutableList.Builder<Expression> rowBuilder = ImmutableList.builder();
//        mergeAnalysis.getDataColumnSchemas().forEach(columnSchema ->
//                rowBuilder.add(new Cast(new NullLiteral(), columnSchema.getType().getDisplayName())));
//        rowBuilder.add(new IsNotNullPredicate(createSymbolReference(presentColumn)));
//        // The operation number
//        rowBuilder.add(new GenericLiteral("TINYINT", "-1"));
//        // The case number
//        rowBuilder.add(new GenericLiteral("INTEGER", "-1"));
//        SearchedCaseExpression caseExpression = new SearchedCaseExpression(whenClauses.build(), Optional.of(new Row(rowBuilder.build())));

        SearchedCaseExpression caseExpression = new SearchedCaseExpression(whenClauses.build(), Optional.empty());
        RowType rowType = createMergeRowType(mergeAnalysis.getDataColumnSchemas());

        FieldReference rowIdReference = analysis.getRowIdField(mergeAnalysis.getTargetTable());
        VariableReferenceExpression rowIdVariable = planWithWithUniqueRowIdsAndPresentColumn.getFieldMappings().get(rowIdReference.getFieldIndex());
        VariableReferenceExpression mergeRowSymbol = variableAllocator.newVariable("merge_row", rowType);
        VariableReferenceExpression caseNumberSymbol = variableAllocator.newVariable("case_number", INTEGER);

        // Project the partition symbols, the merge_row, the rowId, and the unique_id symbol
        Assignments.Builder projectionAssignmentsBuilder = Assignments.builder();
        for (ColumnHandle column : mergeAnalysis.getRedistributionColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(column), "Could not find fieldIndex for redistribution column");
            VariableReferenceExpression symbol = planWithWithUniqueRowIdsAndPresentColumn.getFieldMappings().get(fieldIndex);
            projectionAssignmentsBuilder.put(symbol, symbol);
            // TODO #20578: Is the previous line correct or we should use the following line?
//            projectionAssignmentsBuilder.put(symbol, new VariableReferenceExpression(symbol.getSourceLocation(), symbol.getName(), symbol.getType()));
        }
        // TODO #20578: Original Trino code.
//        projectionAssignmentsBuilder.put(uniqueIdSymbol, uniqueIdSymbol.toSymbolReference());
//        projectionAssignmentsBuilder.put(rowIdSymbol, rowIdSymbol.toSymbolReference());
        projectionAssignmentsBuilder.put(uniqueIdSymbol, uniqueIdSymbol);
        projectionAssignmentsBuilder.put(rowIdVariable, rowIdVariable);
        projectionAssignmentsBuilder.put(mergeRowSymbol, rowExpression(caseExpression, sqlPlannerContext));

        ProjectNode subPlanProject = new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projectionAssignmentsBuilder.build());

        // Now add a column for the case_number, gotten from the merge_row
        SubscriptExpression caseNumberExpression = new SubscriptExpression(
                createSymbolReference(mergeRowSymbol), new LongLiteral(Long.toString(rowType.getFields().size())));

        ProjectNode project = new ProjectNode(
                subPlanProject.getSourceLocation(),
                idAllocator.getNextId(),
                subPlanProject,
                Assignments.builder()
                        .putAll(subPlanProject.getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())))
                        .put(caseNumberSymbol, rowExpression(caseNumberExpression, sqlPlannerContext))
                        .build(),
                LOCAL); // TODO #20578: Is LOCAL the correct value?

        // Mark distinct combinations of the unique_id value and the case_number
        VariableReferenceExpression isDistinctSymbol = variableAllocator.newVariable("is_distinct", BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(
                getSourceLocation(mergeStmt), idAllocator.getNextId(), project, isDistinctSymbol,
                ImmutableList.of(uniqueIdSymbol, caseNumberSymbol), Optional.empty());

        // Raise an error if unique_id symbol is non-null and the unique_id/case_number combination was not distinct
        Expression filter = new IfExpression(
                LogicalBinaryExpression.and(
                        new NotExpression(createSymbolReference(isDistinctSymbol)),
                        new IsNotNullPredicate(createSymbolReference(uniqueIdSymbol))),
                new Cast(
                        new FunctionCall(
                                QualifiedName.of("presto", "default", "fail"),
                                ImmutableList.of(new Cast(new StringLiteral(
                                        "MERGE INTO operation failed for target table '" + targetTable.getName() + "'. " +
                                        "One or more rows in the target table matched multiple source rows. " +
                                        "The MERGE INTO command requires each target row to match at most one source row. " +
                                        "Please review the ON condition to ensure it produces a one-to-one or one-to-none match."),
                                VARCHAR.getTypeSignature().toString()))),
                        BOOLEAN.getTypeSignature().toString()),
                TRUE_LITERAL);

        FilterNode filterNode = new FilterNode(getSourceLocation(mergeStmt), idAllocator.getNextId(), markDistinctNode, rowExpression(filter, sqlPlannerContext));

        TableMetadata tableMetadata = metadata.getTableMetadata(session, targetTableHandle);

        RowChangeParadigm paradigm = metadata.getRowChangeParadigm(session, targetTableHandle);

        // TODO #20578: Delete the following two lines if finally they are not necessary.
//        ColumnHandle rowIdHandle = metadata.getMergeRowIdColumnHandle(session, targetTableHandle);
//        Type rowIdType = metadata.getColumnMetadata(session, targetTableHandle, rowIdHandle).getType();

        Type rowIdType = analysis.getType(analysis.getRowIdField(targetTable));
        List<Type> dataColumnTypes = tableMetadata.getMetadata().getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());

        TableWriterNode.MergeParadigmAndTypes mergeParadigmAndTypes = new TableWriterNode.MergeParadigmAndTypes(paradigm, dataColumnTypes, rowIdType);
        TableWriterNode.MergeTarget mergeTarget = new TableWriterNode.MergeTarget(targetTableHandle, mergeHandle /*Optional.empty()*/, tableMetadata.getTable(), mergeParadigmAndTypes);

        ImmutableList.Builder<VariableReferenceExpression> columnSymbolsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : mergeAnalysis.getDataColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(columnHandle), "Could not find field number for column handle");
            columnSymbolsBuilder.add(planWithWithUniqueRowIdsAndPresentColumn.getFieldMappings().get(fieldIndex));
        }
        List<VariableReferenceExpression> columnSymbols = columnSymbolsBuilder.build();
        ImmutableList.Builder<VariableReferenceExpression> redistributionSymbolsBuilder = ImmutableList.builder();
        for (ColumnHandle columnHandle : mergeAnalysis.getRedistributionColumnHandles()) {
            int fieldIndex = requireNonNull(mergeAnalysis.getColumnHandleFieldNumbers().get(columnHandle), "Could not find field number for column handle");
            redistributionSymbolsBuilder.add(planWithWithUniqueRowIdsAndPresentColumn.getFieldMappings().get(fieldIndex));
        }

        VariableReferenceExpression operationSymbol = variableAllocator.newVariable("operation", TINYINT);
        VariableReferenceExpression insertFromUpdateSymbol = variableAllocator.newVariable("insert_from_update", TINYINT);

        List<VariableReferenceExpression> projectedSymbols = ImmutableList.<VariableReferenceExpression>builder()
                .addAll(columnSymbols)
                .add(operationSymbol)
                .add(rowIdVariable)
                .add(insertFromUpdateSymbol)
                .build();

        MergeProcessorNode mergeProcessorNode = new MergeProcessorNode(
                getSourceLocation(mergeStmt),
                idAllocator.getNextId(),
                filterNode,
                mergeTarget,
                rowIdVariable,
                mergeRowSymbol,
                columnSymbols,
                redistributionSymbolsBuilder.build(),
                projectedSymbols);

        Optional<PartitioningScheme> partitioningScheme = createMergePartitioningScheme(
                mergeAnalysis.getInsertLayout(),
                columnSymbols,
                mergeAnalysis.getInsertPartitioningArgumentIndexes(),
                mergeAnalysis.getUpdateLayout(),
                rowIdVariable,
                operationSymbol);

        List<VariableReferenceExpression> outputs = ImmutableList.of(
                variableAllocator.newVariable("partialrows", BIGINT),
                variableAllocator.newVariable("fragment", VARBINARY));

        return new MergeWriterNode(
                getSourceLocation(mergeStmt),
                idAllocator.getNextId(),
                mergeProcessorNode,
                mergeTarget,
                projectedSymbols,
                partitioningScheme,
                outputs);
    }

    private static int getMergeCaseOperationNumber(MergeCase mergeCase)
    {
        if (mergeCase instanceof MergeInsert) {
            return INSERT_OPERATION_NUMBER;
        }
        if (mergeCase instanceof MergeUpdate) {
            return UPDATE_OPERATION_NUMBER;
        }
        // TODO: Currently, Presto doesn't support DELETE in MERGE command.
//        if (mergeCase instanceof MergeDelete) {
//            return DELETE_OPERATION_NUMBER;
//        }
        throw new IllegalArgumentException("Unrecognized MergeCase: " + mergeCase);
    }

    private static RowType createMergeRowType(List<ColumnMetadata> allColumnsSchema)
    {
        // create the RowType that holds all column values
        List<RowType.Field> fields = new ArrayList<>();
        for (ColumnMetadata schema : allColumnsSchema) {
            // TODO #20578: This was the original line but it is giving me problems when validating data types:
            //            fields.add(new RowType.Field(Optional.of(schema.getName()), schema.getType()));
            // SQL Error [65536]: Query failed (#20250423_090416_00009_ay9hk): type of variable 'merge_row' is
            // expected to be           row(intsample integer,floatsample decimal(2,1),doublesample decimal(5,2),boolean,tinyint,integer),
            // but the actual type is row(          integer,            decimal(2,1),             decimal(5,2),boolean,tinyint,integer)

            // TODO #20578:  So, I changed it to the following line which is working fine:
            fields.add(new RowType.Field(Optional.empty(), schema.getType()));
        }
        fields.add(new RowType.Field(Optional.empty(), BOOLEAN)); // present
        fields.add(new RowType.Field(Optional.empty(), TINYINT)); // operation_number
        fields.add(new RowType.Field(Optional.empty(), INTEGER)); // case_number
        return RowType.from(fields);
    }

    public static Optional<PartitioningScheme> createMergePartitioningScheme(
            Optional<NewTableLayout> insertLayout,
            List<VariableReferenceExpression> symbols,
            List<Integer> insertPartitioningArgumentIndexes,
            Optional<PartitioningHandle> updateLayout,
            VariableReferenceExpression rowIdSymbol,
            VariableReferenceExpression operationSymbol)
    {
        if (!insertLayout.isPresent() && !updateLayout.isPresent()) {
            return Optional.empty();
        }

        Optional<PartitioningScheme> insertPartitioning = insertLayout.map(layout -> {
            List<VariableReferenceExpression> arguments = insertPartitioningArgumentIndexes.stream()
                    .map(symbols::get)
                    .collect(toImmutableList());

            return layout.getPartitioning()
                    .map(handle -> new PartitioningScheme(Partitioning.create(handle, arguments), symbols))
                    // empty connector partitioning handle means evenly partitioning on partitioning columns
                    .orElseGet(() -> new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, arguments), symbols));
        });

        Optional<PartitioningScheme> updatePartitioning = updateLayout.map(handle ->
                new PartitioningScheme(Partitioning.create(handle, ImmutableList.of(rowIdSymbol)), ImmutableList.of(rowIdSymbol)));

        PartitioningHandle partitioningHandle = new PartitioningHandle(
                Optional.empty(),
                Optional.empty(),
                new MergePartitioningHandle(insertPartitioning, updatePartitioning));

        List<VariableReferenceExpression> combinedSymbols = new ArrayList<>();
        combinedSymbols.add(operationSymbol);
        insertPartitioning.ifPresent(scheme -> combinedSymbols.addAll(partitioningSymbols(scheme)));
        updatePartitioning.ifPresent(scheme -> combinedSymbols.addAll(partitioningSymbols(scheme)));

        return Optional.of(new PartitioningScheme(Partitioning.create(partitioningHandle, combinedSymbols), combinedSymbols));
    }

    private static List<VariableReferenceExpression> partitioningSymbols(PartitioningScheme scheme)
    {
        return new ArrayList<>(scheme.getPartitioning().getVariableReferences());
    }

    public static Expression coerceIfNecessary(Analysis analysis, Expression original, Expression rewritten)
    {
        Type coercion = analysis.getCoercion(original);
        if (coercion == null) {
            return rewritten;
        }

        return new Cast(
                rewritten,
                coercion.getDisplayName(),
                false,
                analysis.isTypeOnlyCoercion(original));
    }

    private Optional<PlanNodeId> getIdForLeftTableScan(PlanNode node)
    {
        if (node instanceof TableScanNode) {
            return Optional.of(node.getId());
        }
        List<PlanNode> sources = node.getSources();
        if (sources.isEmpty()) {
            return Optional.empty();
        }
        return getIdForLeftTableScan(sources.get(0));
    }

    private static List<VariableReferenceExpression> computeOutputs(PlanBuilder builder, List<Expression> outputExpressions)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (Expression expression : outputExpressions) {
            outputs.add(builder.translate(expression));
        }
        return outputs.build();
    }

    private PlanBuilder planQueryBody(Query query)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                .process(query.getQueryBody(), sqlPlannerContext);

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planFrom(QuerySpecification node)
    {
        RelationPlan relationPlan;

        if (node.getFrom().isPresent()) {
            relationPlan = new RelationPlanner(analysis, variableAllocator, idAllocator, lambdaDeclarationToVariableMap, metadata, session, sqlParser)
                    .process(node.getFrom().get(), sqlPlannerContext);
        }
        else {
            relationPlan = planImplicitTable();
        }

        return planBuilderFor(relationPlan);
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope, Iterable<? extends Expression> expressionsToRemap)
    {
        PlanBuilder newBuilder = planBuilderFor(builder, scope);
        // We can't deduplicate expressions here because even if two expressions are equal,
        // the TranslationMap maps sql names to symbols, and any lambda expressions will be
        // resolved differently since the lambdaDeclarationToVariableMap is identity based.
        stream(expressionsToRemap)
                .forEach(expression -> newBuilder.getTranslations().put(expression, builder.translate(expression)));
        return newBuilder;
    }

    private PlanBuilder planBuilderFor(PlanBuilder builder, Scope scope)
    {
        return planBuilderFor(new RelationPlan(builder.getRoot(), scope, builder.getRoot().getOutputVariables()));
    }

    private PlanBuilder planBuilderFor(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);

        // Make field->variable mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    private RelationPlan planImplicitTable()
    {
        Scope scope = Scope.create();
        return new RelationPlan(
                new ValuesNode(Optional.empty(), idAllocator.getNextId(), ImmutableList.of(), ImmutableList.of(ImmutableList.of()), Optional.empty()),
                scope,
                ImmutableList.of());
    }

    private PlanBuilder filter(PlanBuilder subPlan, Expression predicate, Node node)
    {
        if (predicate == null) {
            return subPlan;
        }

        // rewrite expressions which contain already handled subqueries
        Expression rewrittenBeforeSubqueries = subPlan.rewrite(predicate);
        subPlan = subqueryPlanner.handleSubqueries(subPlan, rewrittenBeforeSubqueries, node, sqlPlannerContext);
        Expression rewrittenAfterSubqueries = subPlan.rewrite(predicate);

        return subPlan.withNewRoot(new FilterNode(getSourceLocation(node), idAllocator.getNextId(), subPlan.getRoot(), rowExpression(rewrittenAfterSubqueries, sqlPlannerContext)));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions, RelationPlan parentRelationPlan)
    {
        return project(subPlan, Iterables.concat(expressions, toSymbolReferences(parentRelationPlan.getFieldMappings())));
    }

    private PlanBuilder project(PlanBuilder subPlan, Iterable<Expression> expressions)
    {
        TranslationMap outputTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);

        Assignments.Builder projections = Assignments.builder();
        for (Expression expression : expressions) {
            if (expression instanceof SymbolReference) {
                VariableReferenceExpression variable = toVariableReference(variableAllocator, expression);
                projections.put(variable, rowExpression(expression, sqlPlannerContext));
                outputTranslations.put(expression, variable);
                continue;
            }

            VariableReferenceExpression variable = newVariable(variableAllocator, expression, analysis.getTypeWithCoercions(expression));
            projections.put(variable, rowExpression(subPlan.rewrite(expression), sqlPlannerContext));
            outputTranslations.put(expression, variable);
        }

        return new PlanBuilder(outputTranslations, new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build()));
    }

    /**
     * Creates a projection with any additional coercions by identity of the provided expressions.
     *
     * @return the new subplan and a mapping of each expression to the symbol representing the coercion or an existing symbol if a coercion wasn't needed
     */
    private PlanAndMappings coerce(PlanBuilder subPlan, List<Expression> expressions, Analysis analysis, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, Metadata metadata)
    {
        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(subPlan.getRoot().getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())));
        ImmutableMap.Builder<NodeRef<Expression>, VariableReferenceExpression> mappings = ImmutableMap.builder();
        for (Expression expression : expressions) {
            Type coercion = analysis.getCoercion(expression);
            if (coercion != null) {
                Type type = analysis.getType(expression);
                VariableReferenceExpression variable = newVariable(variableAllocator, expression, coercion);
                assignments.put(variable, rowExpression(
                        new Cast(
                                subPlan.rewrite(expression),
                                coercion.getTypeSignature().toString(),
                                false,
                                metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(type, coercion)),
                        sqlPlannerContext));
                mappings.put(NodeRef.of(expression), variable);
            }
            else {
                mappings.put(NodeRef.of(expression), subPlan.translate(expression));
            }
        }
        subPlan = subPlan.withNewRoot(
                new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        assignments.build()));
        return new PlanAndMappings(subPlan, mappings.build());
    }

    private Map<VariableReferenceExpression, RowExpression> coerce(Iterable<? extends Expression> expressions, PlanBuilder subPlan, TranslationMap translations)
    {
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> projections = ImmutableMap.builder();

        for (Expression expression : expressions) {
            Type type = analysis.getType(expression);
            Type coercion = analysis.getCoercion(expression);
            VariableReferenceExpression variable = newVariable(variableAllocator, expression, firstNonNull(coercion, type));
            Expression rewritten = subPlan.rewrite(expression);
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(type, coercion));
            }
            projections.put(variable, rowExpression(rewritten, sqlPlannerContext));
            translations.put(expression, variable);
        }

        return projections.build();
    }

    private PlanBuilder explicitCoercionFields(PlanBuilder subPlan, Iterable<Expression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        Assignments.Builder projections = Assignments.builder();

        projections.putAll(coerce(uncoerced, subPlan, translations));

        for (Expression expression : alreadyCoerced) {
            if (expression instanceof SymbolReference) {
                // If this is an identity projection, no need to rewrite it
                // This is needed because certain synthetic identity expressions such as "group id" introduced when planning GROUPING
                // don't have a corresponding analysis, so the code below doesn't work for them
                projections.put(toVariableReference(variableAllocator, expression), rowExpression(expression, sqlPlannerContext));
                continue;
            }

            VariableReferenceExpression variable = newVariable(variableAllocator, expression, analysis.getType(expression));
            Expression rewritten = subPlan.rewrite(expression);
            projections.put(variable, rowExpression(rewritten, sqlPlannerContext));
            translations.put(expression, variable);
        }

        return new PlanBuilder(translations, new ProjectNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                projections.build(),
                LOCAL));
    }

    private PlanBuilder explicitCoercionVariables(PlanBuilder subPlan, List<VariableReferenceExpression> alreadyCoerced, Iterable<? extends Expression> uncoerced)
    {
        TranslationMap translations = subPlan.copyTranslations();

        Assignments assignments = Assignments.builder()
                .putAll(coerce(uncoerced, subPlan, translations))
                .putAll(alreadyCoerced.stream().collect(toImmutableMap(Function.identity(), Function.identity())))
                .build();

        return new PlanBuilder(translations, new ProjectNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                assignments,
                LOCAL));
    }

    private PlanBuilder aggregate(PlanBuilder subPlan, QuerySpecification node)
    {
        if (!analysis.isAggregation(node)) {
            return subPlan;
        }

        // 1. Pre-project all scalar inputs (arguments and non-trivial group by expressions)
        Set<Expression> groupByExpressions = ImmutableSet.copyOf(analysis.getGroupByExpressions(node));

        ImmutableList.Builder<Expression> arguments = ImmutableList.builder();
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getArguments)
                .flatMap(List::stream)
                .filter(exp -> !(exp instanceof LambdaExpression)) // lambda expression is generated at execution time
                .forEach(arguments::add);

        analysis.getAggregates(node).stream()
                .map(FunctionCall::getOrderBy)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(OrderBy::getSortItems)
                .flatMap(List::stream)
                .map(SortItem::getSortKey)
                .forEach(arguments::add);

        // filter expressions need to be projected first
        analysis.getAggregates(node).stream()
                .map(FunctionCall::getFilter)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(arguments::add);

        Iterable<Expression> inputs = Iterables.concat(groupByExpressions, arguments.build());
        subPlan = handleSubqueries(subPlan, node, inputs);

        if (!Iterables.isEmpty(inputs)) { // avoid an empty projection if the only aggregation is COUNT (which has no arguments)
            subPlan = project(subPlan, inputs);
        }

        // 2. Aggregate

        // 2.a. Rewrite aggregate arguments
        TranslationMap argumentTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);

        ImmutableList.Builder<VariableReferenceExpression> aggregationArgumentsBuilder = ImmutableList.builder();
        for (Expression argument : arguments.build()) {
            VariableReferenceExpression variable = subPlan.translate(argument);
            argumentTranslations.put(argument, variable);
            aggregationArgumentsBuilder.add(variable);
        }
        List<VariableReferenceExpression> aggregationArguments = aggregationArgumentsBuilder.build();

        // 2.b. Rewrite grouping columns
        TranslationMap groupingTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        Map<VariableReferenceExpression, VariableReferenceExpression> groupingSetMappings = new LinkedHashMap<>();

        for (Expression expression : groupByExpressions) {
            VariableReferenceExpression input = subPlan.translate(expression);
            VariableReferenceExpression output = newVariable(variableAllocator, expression, analysis.getTypeWithCoercions(expression), "gid");
            groupingTranslations.put(expression, output);
            groupingSetMappings.put(output, input);
        }

        // This tracks the grouping sets before complex expressions are considered (see comments below)
        // It's also used to compute the descriptors needed to implement grouping()
        List<Set<FieldId>> columnOnlyGroupingSets = ImmutableList.of(ImmutableSet.of());
        List<List<VariableReferenceExpression>> groupingSets = ImmutableList.of(ImmutableList.of());

        if (node.getGroupBy().isPresent()) {
            // For the purpose of "distinct", we need to canonicalize column references that may have varying
            // syntactic forms (e.g., "t.a" vs "a"). Thus we need to enumerate grouping sets based on the underlying
            // fieldId associated with each column reference expression.

            // The catch is that simple group-by expressions can be arbitrary expressions (this is a departure from the SQL specification).
            // But, they don't affect the number of grouping sets or the behavior of "distinct" . We can compute all the candidate
            // grouping sets in terms of fieldId, dedup as appropriate and then cross-join them with the complex expressions.
            Analysis.GroupingSetAnalysis groupingSetAnalysis = analysis.getGroupingSets(node);
            columnOnlyGroupingSets = enumerateGroupingSets(groupingSetAnalysis);

            if (node.getGroupBy().get().isDistinct()) {
                columnOnlyGroupingSets = columnOnlyGroupingSets.stream()
                        .distinct()
                        .collect(toImmutableList());
            }

            // add in the complex expressions an turn materialize the grouping sets in terms of plan columns
            ImmutableList.Builder<List<VariableReferenceExpression>> groupingSetBuilder = ImmutableList.builder();
            for (Set<FieldId> groupingSet : columnOnlyGroupingSets) {
                ImmutableList.Builder<VariableReferenceExpression> columns = ImmutableList.builder();
                groupingSetAnalysis.getComplexExpressions().stream()
                        .map(groupingTranslations::get)
                        .forEach(columns::add);

                groupingSet.stream()
                        .map(field -> groupingTranslations.get(new FieldReference(field.getFieldIndex())))
                        .forEach(columns::add);

                groupingSetBuilder.add(columns.build());
            }

            groupingSets = groupingSetBuilder.build();
        }

        // 2.c. Generate GroupIdNode (multiple grouping sets) or ProjectNode (single grouping set)
        Optional<VariableReferenceExpression> groupIdVariable = Optional.empty();
        if (groupingSets.size() > 1) {
            groupIdVariable = Optional.of(variableAllocator.newVariable("groupId", BIGINT));
            GroupIdNode groupId = new GroupIdNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), groupingSets, groupingSetMappings, aggregationArguments, groupIdVariable.get());
            subPlan = new PlanBuilder(groupingTranslations, groupId);
        }
        else {
            Assignments.Builder assignments = Assignments.builder();
            aggregationArguments.stream().forEach(var -> assignments.put(var, var));
            groupingSetMappings.forEach((key, value) -> assignments.put(key, value));

            ProjectNode project = new ProjectNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), assignments.build(), LOCAL);
            subPlan = new PlanBuilder(groupingTranslations, project);
        }

        TranslationMap aggregationTranslations = new TranslationMap(subPlan.getRelationPlan(), analysis, lambdaDeclarationToVariableMap);
        aggregationTranslations.copyMappingsFrom(groupingTranslations);

        // 2.d. Rewrite aggregates
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregationsBuilder = ImmutableMap.builder();
        boolean needPostProjectionCoercion = false;
        for (FunctionCall aggregate : analysis.getAggregates(node)) {
            Expression rewritten = argumentTranslations.rewrite(aggregate);
            VariableReferenceExpression newVariable = newVariable(variableAllocator, rewritten, analysis.getType(aggregate));

            // TODO: this is a hack, because we apply coercions to the output of expressions, rather than the arguments to expressions.
            // Therefore we can end up with this implicit cast, and have to move it into a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
                needPostProjectionCoercion = true;
            }
            aggregationTranslations.put(aggregate, newVariable);
            FunctionCall rewrittenFunction = (FunctionCall) rewritten;
            FunctionHandle functionHandle = analysis.getFunctionHandle(aggregate);

            aggregationsBuilder.put(newVariable,
                    new Aggregation(
                            new CallExpression(
                                    getSourceLocation(rewrittenFunction),
                                    aggregate.getName().getSuffix(),
                                    functionHandle,
                                    analysis.getType(aggregate),
                                    callArgumentsToRowExpression(functionHandle, rewrittenFunction.getArguments())),
                            rewrittenFunction.getFilter().map(expression -> rowExpression(expression, sqlPlannerContext)),
                            rewrittenFunction.getOrderBy().map(orderBy -> toOrderingScheme(orderBy, TypeProvider.viewOf(variableAllocator.getVariables()))),
                            rewrittenFunction.isDistinct(),
                            Optional.empty()));
        }
        Map<VariableReferenceExpression, Aggregation> aggregations = aggregationsBuilder.build();

        ImmutableSet.Builder<Integer> globalGroupingSets = ImmutableSet.builder();
        for (int i = 0; i < groupingSets.size(); i++) {
            if (groupingSets.get(i).isEmpty()) {
                globalGroupingSets.add(i);
            }
        }

        ImmutableList.Builder<VariableReferenceExpression> groupingKeys = ImmutableList.builder();
        groupingSets.stream()
                .flatMap(List::stream)
                .distinct()
                .forEach(groupingKeys::add);
        groupIdVariable.ifPresent(groupingKeys::add);

        AggregationNode aggregationNode = new AggregationNode(
                subPlan.getRoot().getSourceLocation(),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                aggregations,
                groupingSets(
                        groupingKeys.build(),
                        groupingSets.size(),
                        globalGroupingSets.build()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                groupIdVariable,
                Optional.empty());

        subPlan = new PlanBuilder(aggregationTranslations, aggregationNode);

        // 3. Post-projection
        // Add back the implicit casts that we removed in 2.a
        // TODO: this is a hack, we should change type coercions to coerce the inputs to functions/operators instead of coercing the output
        if (needPostProjectionCoercion) {
            ImmutableList.Builder<Expression> alreadyCoerced = ImmutableList.builder();
            alreadyCoerced.addAll(groupByExpressions);
            groupIdVariable.map(ExpressionTreeUtils::createSymbolReference).ifPresent(alreadyCoerced::add);

            subPlan = explicitCoercionFields(subPlan, alreadyCoerced.build(), analysis.getAggregates(node));
        }

        // 4. Project and re-write all grouping functions
        return handleGroupingOperations(subPlan, node, groupIdVariable, columnOnlyGroupingSets);
    }

    private List<Set<FieldId>> enumerateGroupingSets(Analysis.GroupingSetAnalysis groupingSetAnalysis)
    {
        List<List<Set<FieldId>>> partialSets = new ArrayList<>();

        for (Set<FieldId> cube : groupingSetAnalysis.getCubes()) {
            partialSets.add(ImmutableList.copyOf(Sets.powerSet(cube)));
        }

        for (List<FieldId> rollup : groupingSetAnalysis.getRollups()) {
            List<Set<FieldId>> sets = IntStream.rangeClosed(0, rollup.size())
                    .mapToObj(i -> ImmutableSet.copyOf(rollup.subList(0, i)))
                    .collect(toImmutableList());

            partialSets.add(sets);
        }

        partialSets.addAll(groupingSetAnalysis.getOrdinarySets());

        if (partialSets.isEmpty()) {
            return ImmutableList.of(ImmutableSet.of());
        }

        // compute the cross product of the partial sets
        List<Set<FieldId>> allSets = new ArrayList<>();
        partialSets.get(0)
                .stream()
                .map(ImmutableSet::copyOf)
                .forEach(allSets::add);

        for (int i = 1; i < partialSets.size(); i++) {
            List<Set<FieldId>> groupingSets = partialSets.get(i);
            List<Set<FieldId>> oldGroupingSetsCrossProduct = ImmutableList.copyOf(allSets);
            allSets.clear();
            for (Set<FieldId> existingSet : oldGroupingSetsCrossProduct) {
                for (Set<FieldId> groupingSet : groupingSets) {
                    Set<FieldId> concatenatedSet = ImmutableSet.<FieldId>builder()
                            .addAll(existingSet)
                            .addAll(groupingSet)
                            .build();
                    allSets.add(concatenatedSet);
                }
            }
        }

        return allSets;
    }

    private PlanBuilder handleGroupingOperations(PlanBuilder subPlan, QuerySpecification node, Optional<VariableReferenceExpression> groupIdVariable, List<Set<FieldId>> groupingSets)
    {
        if (analysis.getGroupingOperations(node).isEmpty()) {
            return subPlan;
        }

        TranslationMap newTranslations = subPlan.copyTranslations();

        Assignments.Builder projections = Assignments.builder();
        projections.putAll(subPlan.getRoot().getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())));

        List<Set<Integer>> descriptor = groupingSets.stream()
                .map(set -> set.stream()
                        .map(FieldId::getFieldIndex)
                        .collect(toImmutableSet()))
                .collect(toImmutableList());

        for (GroupingOperation groupingOperation : analysis.getGroupingOperations(node)) {
            Expression rewritten = GroupingOperationRewriter.rewriteGroupingOperation(groupingOperation, descriptor, analysis.getColumnReferenceFields(), groupIdVariable);
            Type coercion = analysis.getCoercion(groupingOperation);
            VariableReferenceExpression variable = newVariable(variableAllocator, rewritten, analysis.getTypeWithCoercions(groupingOperation));
            if (coercion != null) {
                rewritten = new Cast(
                        rewritten,
                        coercion.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(analysis.getType(groupingOperation), coercion));
            }
            projections.put(variable, rowExpression(rewritten, sqlPlannerContext));
            newTranslations.put(groupingOperation, variable);
        }

        return new PlanBuilder(newTranslations, new ProjectNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), projections.build(), LOCAL));
    }

    private PlanBuilder window(PlanBuilder subPlan, OrderBy node)
    {
        return window(subPlan, ImmutableList.copyOf(analysis.getOrderByWindowFunctions(node)));
    }

    private PlanBuilder window(PlanBuilder subPlan, QuerySpecification node)
    {
        return window(subPlan, ImmutableList.copyOf(analysis.getWindowFunctions(node)));
    }

    private PlanBuilder window(PlanBuilder subPlan, List<FunctionCall> windowFunctions)
    {
        if (windowFunctions.isEmpty()) {
            return subPlan;
        }

        for (FunctionCall windowFunction : windowFunctions) {
            Window window = windowFunction.getWindow().get();

            // Extract frame
            WindowFrame.Type frameType = RANGE;
            FrameBound.Type frameStartType = FrameBound.Type.UNBOUNDED_PRECEDING;
            FrameBound.Type frameEndType = FrameBound.Type.CURRENT_ROW;
            Optional<Expression> startValue = Optional.empty();
            Optional<Expression> endValue = Optional.empty();

            if (window.getFrame().isPresent()) {
                WindowFrame frame = window.getFrame().get();
                frameType = frame.getType();

                frameStartType = frame.getStart().getType();
                startValue = frame.getStart().getValue();

                if (frame.getEnd().isPresent()) {
                    frameEndType = frame.getEnd().get().getType();
                    endValue = frame.getEnd().get().getValue();
                }
            }

            // Pre-project inputs
            ImmutableList.Builder<Expression> inputsBuilder = ImmutableList.<Expression>builder()
                    .addAll(windowFunction.getArguments())
                    .addAll(window.getPartitionBy())
                    .addAll(Iterables.transform(getSortItemsFromOrderBy(window.getOrderBy()), SortItem::getSortKey));

            if (startValue.isPresent()) {
                inputsBuilder.add(startValue.get());
            }
            if (endValue.isPresent()) {
                inputsBuilder.add(endValue.get());
            }

            ImmutableList<Expression> inputs = inputsBuilder.build();
            subPlan = subPlan.appendProjections(inputs, variableAllocator, idAllocator, session, metadata, sqlParser, analysis, sqlPlannerContext);

            // Add projection to coerce inputs to their site-specific types.
            // This is important because the same lexical expression may need to be coerced
            // in different ways if it's referenced by multiple arguments to the window function.
            // For example, given v::integer,
            //    avg(v) OVER (ORDER BY v)
            // Needs to be rewritten as
            //    avg(CAST(v AS double)) OVER (ORDER BY v)
            PlanAndMappings coercions = coerce(subPlan, inputs, analysis, idAllocator, variableAllocator, metadata);
            subPlan = coercions.getSubPlan();

            // For frame of type RANGE, append casts and functions necessary for frame bound calculations
            Optional<VariableReferenceExpression> frameStart = Optional.empty();
            Optional<VariableReferenceExpression> frameEnd = Optional.empty();
            Optional<VariableReferenceExpression> sortKeyCoercedForFrameStartComparison = Optional.empty();
            Optional<VariableReferenceExpression> sortKeyCoercedForFrameEndComparison = Optional.empty();

            if (window.getFrame().isPresent() && window.getFrame().get().getType() == RANGE) {
                // record sortKey coercions for reuse
                Map<Type, VariableReferenceExpression> sortKeyCoercions = new HashMap<>();

                // process frame start
                FrameBoundPlanAndSymbols plan = planFrameBound(subPlan, coercions, startValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameStart = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameStartComparison = plan.getSortKeyCoercedForFrameBoundComparison();

                // process frame end
                plan = planFrameBound(subPlan, coercions, endValue, window, sortKeyCoercions);
                subPlan = plan.getSubPlan();
                frameEnd = plan.getFrameBoundSymbol();
                sortKeyCoercedForFrameEndComparison = plan.getSortKeyCoercedForFrameBoundComparison();
            }
            else if (window.getFrame().isPresent() && (window.getFrame().get().getType() == ROWS || window.getFrame().get().getType() == GROUPS)) {
                frameStart = window.getFrame().get().getStart().getValue().map(coercions::get);
                frameEnd = window.getFrame().get().getEnd().flatMap(FrameBound::getValue).map(coercions::get);
            }
            else if (window.getFrame().isPresent()) {
                throw new IllegalArgumentException("unexpected window frame type: " + window.getFrame().get().getType());
            }

            // Rewrite PARTITION BY in terms of pre-projected inputs
            ImmutableList.Builder<VariableReferenceExpression> partitionByVariables = ImmutableList.builder();
            for (Expression expression : window.getPartitionBy()) {
                partitionByVariables.add(subPlan.translateToVariable(expression));
            }

            // Rewrite ORDER BY in terms of pre-projected inputs
            LinkedHashMap<VariableReferenceExpression, SortOrder> orderings = new LinkedHashMap<>();
            for (SortItem item : getSortItemsFromOrderBy(window.getOrderBy())) {
                VariableReferenceExpression variable = subPlan.translateToVariable(item.getSortKey());
                // don't override existing keys, i.e. when "ORDER BY a ASC, a DESC" is specified
                orderings.putIfAbsent(variable, toSortOrder(item));
            }

            // Rewrite frame bounds in terms of pre-projected inputs

            WindowNode.Frame frame = new WindowNode.Frame(
                    toWindowType(frameType),
                    toBoundType(frameStartType),
                    frameStart,
                    sortKeyCoercedForFrameStartComparison,
                    toBoundType(frameEndType),
                    frameEnd,
                    sortKeyCoercedForFrameEndComparison,
                    startValue.map(Expression::toString),
                    endValue.map(Expression::toString));

            TranslationMap outputTranslations = subPlan.copyTranslations();

            // Rewrite function call in terms of pre-projected inputs
            Expression rewritten = subPlan.rewrite(windowFunction);

            boolean needCoercion = rewritten instanceof Cast;
            // Strip out the cast and add it back as a post-projection
            if (rewritten instanceof Cast) {
                rewritten = ((Cast) rewritten).getExpression();
            }

            // If refers to existing variable, don't create another PlanNode
            if (rewritten instanceof SymbolReference) {
                if (needCoercion) {
                    subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
                }

                continue;
            }

            Type returnType = analysis.getType(windowFunction);
            VariableReferenceExpression newVariable = newVariable(variableAllocator, rewritten, returnType);
            outputTranslations.put(windowFunction, newVariable);

            // TODO: replace arguments with RowExpression once we introduce subquery expression for RowExpression (#12745).
            // Wrap all arguments in CallExpression to be RawExpression.
            // The utility that work on the CallExpression should be aware of the RawExpression handling.
            // The interface will be dirty until we introduce subquery expression for RowExpression.
            // With subqueries, the translation from Expression to RowExpression can happen here.
            FunctionHandle functionHandle = analysis.getFunctionHandle(windowFunction);
            WindowNode.Function function = new WindowNode.Function(
                    call(
                            windowFunction.getName().toString(),
                            functionHandle,
                            returnType,
                            callArgumentsToRowExpression(functionHandle, ((FunctionCall) rewritten).getArguments())),
                    frame,
                    windowFunction.isIgnoreNulls());

            ImmutableList.Builder<VariableReferenceExpression> orderByVariables = ImmutableList.builder();
            orderByVariables.addAll(orderings.keySet());
            Optional<OrderingScheme> orderingScheme = Optional.empty();
            if (!orderings.isEmpty()) {
                orderingScheme = Optional.of(new OrderingScheme(orderByVariables.build().stream().map(variable -> new Ordering(variable, orderings.get(variable))).collect(toImmutableList())));
            }

            // create window node
            subPlan = new PlanBuilder(outputTranslations,
                    new WindowNode(
                            subPlan.getRoot().getSourceLocation(),
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            new DataOrganizationSpecification(
                                    partitionByVariables.build(),
                                    orderingScheme),
                            ImmutableMap.of(newVariable, function),
                            Optional.empty(),
                            ImmutableSet.of(),
                            0));

            if (needCoercion) {
                subPlan = explicitCoercionVariables(subPlan, subPlan.getRoot().getOutputVariables(), ImmutableList.of(windowFunction));
            }
        }

        return subPlan;
    }

    private FrameBoundPlanAndSymbols planFrameBound(PlanBuilder subPlan, PlanAndMappings coercions, Optional<Expression> frameOffset, Window window, Map<Type, VariableReferenceExpression> sortKeyCoercions)
    {
        Optional<FunctionHandle> frameBoundCalculationFunction = frameOffset.map(analysis::getFrameBoundCalculation);

        // Empty frameBoundCalculationFunction indicates that frame bound type is CURRENT ROW or UNBOUNDED.
        // Handling it doesn't require any additional symbols.
        if (!frameBoundCalculationFunction.isPresent()) {
            return new FrameBoundPlanAndSymbols(subPlan, Optional.empty(), Optional.empty());
        }

        // Present frameBoundCalculationFunction indicates that frame bound type is <expression> PRECEDING or <expression> FOLLOWING.
        // It requires adding certain projections to the plan so that the operator can determine frame bounds.

        // First, append filter to validate offset values. They mustn't be negative or null.
        VariableReferenceExpression offsetSymbol = coercions.get(frameOffset.get());
        Expression zeroOffset = zeroOfType(TypeProvider.viewOf(variableAllocator.getVariables()).get(offsetSymbol));
        FunctionHandle fail = metadata.getFunctionAndTypeManager().resolveFunction(Optional.empty(), Optional.empty(), QualifiedObjectName.valueOf("presto.default.fail"), fromTypes(VARCHAR));
        Expression predicate = new IfExpression(
                new ComparisonExpression(
                        GREATER_THAN_OR_EQUAL,
                        new SymbolReference(offsetSymbol.getName()),
                        zeroOffset),
                TRUE_LITERAL,
                new Cast(
                        new FunctionCall(
                                QualifiedName.of("presto", "default", "fail"),
                                ImmutableList.of(new Cast(new StringLiteral("Window frame offset value must not be negative or null"), VARCHAR.getTypeSignature().toString()))),
                        BOOLEAN.getTypeSignature().toString()));
        subPlan = subPlan.withNewRoot(new FilterNode(
                getSourceLocation(window),
                idAllocator.getNextId(),
                subPlan.getRoot(),
                rowExpression(predicate, sqlPlannerContext)));

        // Then, coerce the sortKey so that we can add / subtract the offset.
        // Note: for that we cannot rely on the usual mechanism of using the coerce() method. The coerce() method can only handle one coercion for a node,
        // while the sortKey node might require several different coercions, e.g. one for frame start and one for frame end.
        Expression sortKey = Iterables.getOnlyElement(window.getOrderBy().get().getSortItems()).getSortKey();
        VariableReferenceExpression sortKeyCoercedForFrameBoundCalculation = coercions.get(sortKey);
        Optional<Type> coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundCalculation);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            VariableReferenceExpression alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundCalculation = alreadyCoerced;
            }
            else {
                Expression cast = new Cast(
                        new SymbolReference(coercions.get(sortKey).getName()),
                        expectedType.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                sortKeyCoercedForFrameBoundCalculation = newVariable(variableAllocator, cast, expectedType);
                sortKeyCoercions.put(expectedType, sortKeyCoercedForFrameBoundCalculation);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putAll(subPlan.getRoot().getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())))
                                .put(sortKeyCoercedForFrameBoundCalculation, rowExpression(cast, sqlPlannerContext))
                                .build()));
            }
        }

        // Next, pre-project the function which combines sortKey with the offset.
        // Note: if frameOffset needs a coercion, it was added before by a call to coerce() method.
        FunctionHandle function = frameBoundCalculationFunction.get();
        FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(function);
        QualifiedObjectName name = functionMetadata.getName();
        Expression functionCall = new FunctionCall(
                QualifiedName.of(name.getCatalogName(), name.getSchemaName(), name.getObjectName()),
                ImmutableList.of(
                        new SymbolReference(sortKeyCoercedForFrameBoundCalculation.getName()),
                        new SymbolReference(offsetSymbol.getName())));
        VariableReferenceExpression frameBoundVariable = newVariable(variableAllocator, functionCall, metadata.getFunctionAndTypeManager().getType(functionMetadata.getReturnType()));
        subPlan = subPlan.withNewRoot(new ProjectNode(
                idAllocator.getNextId(),
                subPlan.getRoot(),
                Assignments.builder()
                        .putAll(subPlan.getRoot().getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())))
                        .put(frameBoundVariable, rowExpression(functionCall, sqlPlannerContext))
                        .build()));

        // Finally, coerce the sortKey to the type of frameBound so that the operator can perform comparisons on them
        Optional<VariableReferenceExpression> sortKeyCoercedForFrameBoundComparison = Optional.of(coercions.get(sortKey));
        coercion = frameOffset.map(analysis::getSortKeyCoercionForFrameBoundComparison);
        if (coercion.isPresent()) {
            Type expectedType = coercion.get();
            VariableReferenceExpression alreadyCoerced = sortKeyCoercions.get(expectedType);
            if (alreadyCoerced != null) {
                sortKeyCoercedForFrameBoundComparison = Optional.of(alreadyCoerced);
            }
            else {
                Expression cast = new Cast(
                        new SymbolReference(coercions.get(sortKey).getName()),
                        expectedType.getTypeSignature().toString(),
                        false,
                        metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(analysis.getType(sortKey), expectedType));
                VariableReferenceExpression castSymbol = newVariable(variableAllocator, cast, expectedType);
                sortKeyCoercions.put(expectedType, castSymbol);
                subPlan = subPlan.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        Assignments.builder()
                                .putAll(subPlan.getRoot().getOutputVariables().stream().collect(toImmutableMap(Function.identity(), Function.identity())))
                                .put(castSymbol, rowExpression(cast, sqlPlannerContext))
                                .build()));
                sortKeyCoercedForFrameBoundComparison = Optional.of(castSymbol);
            }
        }

        return new FrameBoundPlanAndSymbols(subPlan, Optional.of(frameBoundVariable), sortKeyCoercedForFrameBoundComparison);
    }

    private Expression zeroOfType(Type type)
    {
        if (isNumericType(type)) {
            return new Cast(new LongLiteral("0"), type.getTypeSignature().toString());
        }
        if (type.equals(INTERVAL_DAY_TIME)) {
            return new IntervalLiteral("0", POSITIVE, DAY);
        }
        if (type.equals(INTERVAL_YEAR_MONTH)) {
            return new IntervalLiteral("0", POSITIVE, YEAR);
        }
        throw new IllegalArgumentException("unexpected type: " + type);
    }

    private PlanBuilder handleSubqueries(PlanBuilder subPlan, Node node, Iterable<Expression> inputs)
    {
        for (Expression input : inputs) {
            subPlan = subqueryPlanner.handleSubqueries(subPlan, subPlan.rewrite(input), node, sqlPlannerContext);
        }
        return subPlan;
    }

    private PlanBuilder distinct(PlanBuilder subPlan, QuerySpecification node)
    {
        if (node.getSelect().isDistinct()) {
            return subPlan.withNewRoot(
                    new AggregationNode(
                            getSourceLocation(node),
                            idAllocator.getNextId(),
                            subPlan.getRoot(),
                            ImmutableMap.of(),
                            singleGroupingSet(subPlan.getRoot().getOutputVariables()),
                            ImmutableList.of(),
                            AggregationNode.Step.SINGLE,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));
        }

        return subPlan;
    }

    private PlanBuilder sort(PlanBuilder subPlan, Query node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, QuerySpecification node)
    {
        return sort(subPlan, node.getOrderBy(), analysis.getOrderByExpressions(node));
    }

    private PlanBuilder sort(PlanBuilder subPlan, Optional<OrderBy> orderBy, List<Expression> orderByExpressions)
    {
        if (!orderBy.isPresent() || (isSkipRedundantSort(session)) && analysis.isOrderByRedundant(orderBy.get())) {
            return subPlan;
        }

        PlanNode planNode;
        OrderingScheme orderingScheme = toOrderingScheme(
                orderByExpressions.stream().map(subPlan::translate).collect(toImmutableList()),
                orderBy.get().getSortItems().stream().map(PlannerUtils::toSortOrder).collect(toImmutableList()));
        planNode = new SortNode(getSourceLocation(orderBy.get()), idAllocator.getNextId(), subPlan.getRoot(), orderingScheme, false, ImmutableList.of());

        return subPlan.withNewRoot(planNode);
    }

    private PlanBuilder offset(PlanBuilder subPlan, Optional<Offset> offset)
    {
        if (!offset.isPresent()) {
            return subPlan;
        }

        return subPlan.withNewRoot(
                new OffsetNode(
                        getSourceLocation(offset.get()),
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        analysis.getOffset(offset.get())));
    }

    private PlanBuilder limit(PlanBuilder subPlan, Query node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, QuerySpecification node)
    {
        return limit(subPlan, node.getLimit());
    }

    private PlanBuilder limit(PlanBuilder subPlan, Optional<String> limit)
    {
        if (!limit.isPresent()) {
            return subPlan;
        }

        if (!limit.get().equalsIgnoreCase("all")) {
            try {
                long limitValue = Long.parseLong(limit.get());
                subPlan = subPlan.withNewRoot(new LimitNode(subPlan.getRoot().getSourceLocation(), idAllocator.getNextId(), subPlan.getRoot(), limitValue, FINAL));
            }
            catch (NumberFormatException e) {
                throw new PrestoException(INVALID_LIMIT_CLAUSE, format("Invalid limit: %s", limit.get()));
            }
        }

        return subPlan;
    }

    // Special treatment of CallExpression
    private List<RowExpression> callArgumentsToRowExpression(FunctionHandle functionHandle, List<Expression> arguments)
    {
        return arguments.stream()
                .map(expression -> toRowExpression(
                        expression,
                        metadata,
                        session,
                        analyzeCallExpressionTypes(functionHandle, arguments, metadata, sqlParser, session, TypeProvider.viewOf(variableAllocator.getVariables())),
                        sqlPlannerContext.getTranslatorContext()))
                .collect(toImmutableList());
    }

    private RowExpression rowExpression(Expression expression, SqlPlannerContext context)
    {
        return toRowExpression(
                expression,
                metadata,
                session,
                sqlParser,
                variableAllocator,
                analysis,
                context.getTranslatorContext());
    }

    private static List<Expression> toSymbolReferences(List<VariableReferenceExpression> variables)
    {
        return variables.stream()
                .map(variable -> new SymbolReference(
                        variable.getSourceLocation().map(location -> new NodeLocation(location.getLine(), location.getColumn())),
                        variable.getName()))
                .collect(toImmutableList());
    }

    public static class PlanAndMappings
    {
        private final PlanBuilder subPlan;
        private final Map<NodeRef<Expression>, VariableReferenceExpression> mappings;

        public PlanAndMappings(PlanBuilder subPlan, Map<NodeRef<Expression>, VariableReferenceExpression> mappings)
        {
            this.subPlan = subPlan;
            this.mappings = mappings;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public VariableReferenceExpression get(Expression expression)
        {
            return tryGet(expression)
                    .orElseThrow(() -> new IllegalArgumentException(format("No mapping for expression: %s (%s)", expression, System.identityHashCode(expression))));
        }

        public Optional<VariableReferenceExpression> tryGet(Expression expression)
        {
            VariableReferenceExpression result = mappings.get(NodeRef.of(expression));
            if (result != null) {
                return Optional.of(result);
            }
            return Optional.empty();
        }
    }

    private static class FrameBoundPlanAndSymbols
    {
        private final PlanBuilder subPlan;
        private final Optional<VariableReferenceExpression> frameBoundSymbol;
        private final Optional<VariableReferenceExpression> sortKeyCoercedForFrameBoundComparison;

        public FrameBoundPlanAndSymbols(PlanBuilder subPlan, Optional<VariableReferenceExpression> frameBoundSymbol, Optional<VariableReferenceExpression> sortKeyCoercedForFrameBoundComparison)
        {
            this.subPlan = subPlan;
            this.frameBoundSymbol = frameBoundSymbol;
            this.sortKeyCoercedForFrameBoundComparison = sortKeyCoercedForFrameBoundComparison;
        }

        public PlanBuilder getSubPlan()
        {
            return subPlan;
        }

        public Optional<VariableReferenceExpression> getFrameBoundSymbol()
        {
            return frameBoundSymbol;
        }

        public Optional<VariableReferenceExpression> getSortKeyCoercedForFrameBoundComparison()
        {
            return sortKeyCoercedForFrameBoundComparison;
        }
    }
}
