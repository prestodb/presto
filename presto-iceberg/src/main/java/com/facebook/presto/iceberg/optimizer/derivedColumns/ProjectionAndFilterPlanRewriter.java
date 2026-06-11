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
package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.StandardWarningCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.derivedColumns.DerivedColumnSpec;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.iceberg.IcebergSessionProperties.isDerivedColumnsEnabled;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class ProjectionAndFilterPlanRewriter
        extends ConnectorPlanRewriter<Set<VariableReferenceExpression>>
{
    private static final Logger LOG = Logger.get(ProjectionAndFilterPlanRewriter.class);

    private final ConnectorSession session;
    private final SqlParser sqlParser;
    private final VariableAllocator variableAllocator;
    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;
    private final IcebergTransactionManager transactionManager;
    private final PlanNodeIdAllocator idAllocator;
    private final BiMap<String, VariableReferenceExpression> derivedColumnAliasMap;

    public ProjectionAndFilterPlanRewriter(
            StandardFunctionResolution functionResolution,
            TypeManager typeManager,
            IcebergTransactionManager transactionManager,
            PlanNodeIdAllocator idAllocator,
            ConnectorSession session,
            SqlParser sqlParser,
            VariableAllocator variableAllocator)
    {
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
        this.transactionManager = transactionManager;
        this.idAllocator = idAllocator;
        this.session = session;
        this.sqlParser = sqlParser;
        this.variableAllocator = variableAllocator;
        this.derivedColumnAliasMap = HashBiMap.create();
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        if (!isDerivedColumnsEnabled(session)) {
            return node;
        }
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();
        List<VariableReferenceExpression> leftOldOutputVariables = left.getOutputVariables();
        List<VariableReferenceExpression> rightOldOutputVariables = right.getOutputVariables();
        left = context.rewrite(left, context.get());
        Set<VariableReferenceExpression> leftDiff = Sets.difference(new HashSet<>(leftOldOutputVariables), new HashSet<>(left.getOutputVariables()));
        List<VariableReferenceExpression> outputVariables = new ArrayList<>(node.getOutputVariables());
        right = context.rewrite(right, context.get());
        Set<VariableReferenceExpression> rightDiff = Sets.difference(new HashSet<>(rightOldOutputVariables), new HashSet<>(right.getOutputVariables()));
        outputVariables.addAll(leftDiff);
        outputVariables.addAll(rightDiff);
        if (node.isCrossJoin()) {
            outputVariables = ImmutableList.<VariableReferenceExpression>builder().addAll(left.getOutputVariables()).addAll(right.getOutputVariables()).build();
        }
        return new JoinNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                left, right,
                node.getCriteria(),
                outputVariables,
                node.getFilter(),
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType(),
                node.getDynamicFilters());
    }

    @Override
    public PlanNode visitProject(ProjectNode projectNode, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        if (!isDerivedColumnsEnabled(session)) {
            return projectNode;
        }
        Assignments assignments = projectNode.getAssignments();
        if (!(projectNode.getSource() instanceof FilterNode) && !(projectNode.getSource() instanceof JoinNode)) {
            return context.defaultRewrite(projectNode, context.get());
        }
        if (projectNode.getSource() instanceof FilterNode) {
            FilterNode filter = (FilterNode) projectNode.getSource();
            if (!(filter.getSource() instanceof TableScanNode) && !(filter.getSource() instanceof JoinNode)) {
                // Any node type other than TableScanNode and JoinNode not supported for rewrite under FilterNode.
                return context.defaultRewrite(projectNode, context.get());
            }

            if (filter.getSource() instanceof JoinNode) {
                Set<VariableReferenceExpression> derivedColumnsAdded = new HashSet<>();
                Map<VariableReferenceExpression, RowExpression> assignmentsMap = new HashMap<>();
                for (PlanNode planNode : filter.getSource().getSources()) {
                    if (planNode instanceof TableScanNode) {
                        // TODO: refactor , this code repeats thrice.
                        TableScanNode tableScan = (TableScanNode) planNode;
                        TableHandle handle = tableScan.getTable();
                        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
                        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
                        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
                        List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                                .filter(colString -> colString != null && !colString.isBlank()).toList();
                        if (!derivedColumns.isEmpty()) {
                            ExpressionRewriteInfoMaps expressionRewriteInfoMaps = buildExpressionRewriteInfoMaps(tableMetadata, metadata, tableScan);
                            Map<VariableReferenceExpression, RewrittenExpressionMetadata> rewrittenAssignments = assignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                    rowExpression -> rowExpression.getValue().accept(new RewriteCommonSubExpression(),
                                            expressionRewriteInfoMaps.derivedColumnExpressionToDerivedColumnMap())));
                            derivedColumnsAdded.addAll(rewrittenAssignments.entrySet().stream()
                                    .map(assignment -> assignment.getValue().derivedColumnsAdded())
                                    .reduce((leftList, rightList) ->
                                            ImmutableList.<VariableReferenceExpression>builder().addAll(leftList).addAll(rightList).build()).orElse(ImmutableList.of()));
                            assignmentsMap.putAll(rewrittenAssignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                                    value -> value.getValue().rewrittenExpression())));
                        }
                    }
                }
                PlanNode joinNode = context.rewrite(filter.getSource(), derivedColumnsAdded);
                return new ProjectNode(projectNode.getSourceLocation(), idAllocator.getNextId(), joinNode, new Assignments(assignmentsMap), projectNode.getLocality());
            }
            else {
                TableScanNode tableScan = (TableScanNode) filter.getSource();
                TableHandle handle = tableScan.getTable();
                IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
                IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
                ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
                List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                        .filter(colString -> colString != null && !colString.isBlank()).toList();
                if (derivedColumns.isEmpty()) {
                    return projectNode;
                }
                ExpressionRewriteInfoMaps expressionRewriteInfoMaps = buildExpressionRewriteInfoMaps(tableMetadata, metadata, tableScan);

                Map<VariableReferenceExpression, RewrittenExpressionMetadata> rewrittenAssignments = assignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                        rowExpression -> rowExpression.getValue().accept(new RewriteCommonSubExpression(),
                                expressionRewriteInfoMaps.derivedColumnExpressionToDerivedColumnMap())));
                Set<VariableReferenceExpression> derivedColumnsAdded = new HashSet<>(rewrittenAssignments.entrySet().stream()
                        .map(assignment -> assignment.getValue().derivedColumnsAdded())
                        .reduce((leftList, rightList) ->
                                ImmutableList.<VariableReferenceExpression>builder().addAll(leftList).addAll(rightList).build()).orElse(ImmutableList.of()));

                PlanNode rewrittenFilter = context.rewrite(filter, derivedColumnsAdded);
                Map<VariableReferenceExpression, RowExpression> assignmentsMap =
                        rewrittenAssignments.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, value -> value.getValue().rewrittenExpression()));
                return new ProjectNode(projectNode.getSourceLocation(), idAllocator.getNextId(), rewrittenFilter, new Assignments(assignmentsMap), projectNode.getLocality());
            }
        }
        return projectNode;
    }

    @Override
    public PlanNode visitFilter(FilterNode filter, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        if (!isDerivedColumnsEnabled(session)) {
            return filter;
        }

        TableScanNode tableScan = (TableScanNode) filter.getSource();
        TableHandle handle = tableScan.getTable();
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                .filter(colString -> colString != null && !colString.isBlank()).toList();
        if (derivedColumns.isEmpty()) {
            return filter;
        }
        ExpressionRewriteInfoMaps expressionRewriteInfoMaps = buildExpressionRewriteInfoMaps(tableMetadata, metadata, tableScan);

        checkState(expressionRewriteInfoMaps.columnsMap.keySet().containsAll(derivedColumns),
                format("inconsistent derived column definition, derived columns: %s does not exist in table: %s", Joiner.on(',').join(derivedColumns),
                        tableHandle.getIcebergTableName()));
        RewrittenExpressionMetadata rewrittenExpressionMetadata =
                filter.getPredicate().accept(new RewriteCommonSubExpression(), expressionRewriteInfoMaps.derivedColumnExpressionToDerivedColumnMap());
        Set<VariableReferenceExpression> derivedColumnsAdded;
        if (context.get() != null) {
            derivedColumnsAdded = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(rewrittenExpressionMetadata.derivedColumnsAdded())
                    .addAll(context.get()).build();
        }
        else {
            derivedColumnsAdded = new HashSet<>(rewrittenExpressionMetadata.derivedColumnsAdded());
        }

        if (!derivedColumnsAdded.isEmpty()) {
            Set<String> derivedColumnsAddedNames = derivedColumnsAdded.stream().map(VariableReferenceExpression::getName).collect(Collectors.toSet());
            Map<VariableReferenceExpression, ColumnHandle> tableAssignments = new HashMap<>(tableScan.getAssignments().entrySet().stream()
                    .filter(entry ->
                            !derivedColumnsAddedNames.contains(((IcebergColumnHandle) entry.getValue()).getColumnIdentity().getName())
                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
            RowExpression rewrittenFilterPredicate = rewrittenExpressionMetadata.rewrittenExpression();

            derivedColumnsAdded.forEach(col -> tableAssignments.put(col, expressionRewriteInfoMaps.newAssignmentsMap.get(col)));
            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            icebergTableLayoutHandle.getDomainPredicate(),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            Optional.of(ImmutableSet.<IcebergColumnHandle>builder().addAll(icebergTableLayoutHandle.getRequestedColumns().orElse(ImmutableSet.of()))
                                    .addAll(derivedColumnsAdded.stream().map(expressionRewriteInfoMaps.newAssignmentsMap::get).collect(Collectors.toSet())).build()),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            icebergTableLayoutHandle.getPartitionColumnPredicate(),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));

            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    tableAssignments.keySet().stream().toList(),
                    tableAssignments,
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
            return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), filter.getStatsEquivalentPlanNode(), newTableScan, rewrittenFilterPredicate);
        }
        return filter;
    }

    private ExpressionRewriteInfoMaps buildExpressionRewriteInfoMaps(ConnectorTableMetadata tableMetadata, IcebergAbstractMetadata metadata, TableScanNode tableScan)
    {
        Map<String, ColumnIdentity> columnIdentityMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                .collect(Collectors.toMap(ColumnMetadata::getName, col ->
                {
                    Types.NestedField nestedField = IcebergUtil.getIcebergTable(metadata, session,
                            ((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getSchemaTableName()).schema().caseInsensitiveFindField(col.getName());
                    return ColumnIdentity.createColumnIdentity(nestedField);
                }));
        Map<VariableReferenceExpression, ColumnHandle> existingTableAssignments = new HashMap<>(tableScan.getAssignments());

        Map<String, ColumnMetadata> columnsMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                .collect(Collectors.toMap(ColumnMetadata::getName, col -> col));

        Map<VariableReferenceExpression, IcebergColumnHandle> newAssignmentsMap = new HashMap<>();
        List<DerivedColumnSpec> derivedColumnSpecs = IcebergTableProperties.getDerivedColumnSpec(tableMetadata.getProperties()).getDerivedColumnSpecs();
        TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap = new TreeMap<>(new RowExpressionComparator());
        Function<ColumnHandle, VariableReferenceExpression> columnHandleToVariableRefExpr = columnHandle ->
        {
            String columnName = ((IcebergColumnHandle) columnHandle).getName();
            return new VariableReferenceExpression(Optional.empty(),
                    columnName,
                    columnsMap.get(columnName).getType());
        };
        Map<VariableReferenceExpression, VariableReferenceExpression> aliasMap = existingTableAssignments.entrySet().stream().collect(
                ImmutableMap.toImmutableMap(entry -> columnHandleToVariableRefExpr.apply(entry.getValue()), Map.Entry::getKey));
        Function<String, IcebergColumnHandle> derivedColumnHandleGenerator = colName -> new IcebergColumnHandle(
                columnIdentityMap.get(colName),
                columnsMap.get(colName).getType(),
                Optional.of("derived column"),
                BaseHiveColumnHandle.ColumnType.REGULAR);
        derivedColumnSpecs.forEach(udfSpec -> {
            Expression expression = sqlParser.createExpression(udfSpec.getDerivedColumnExpression(),
                    ParsingOptions.builder().setWarningConsumer(parsingWarning -> {
                        String message = format("derived column expression: %s has parse warnings: %s", udfSpec.getDerivedColumnExpression(), parsingWarning.getMessage());
                        LOG.warn(message);
                        session.getWarningCollector().add(new PrestoWarning(PARSER_WARNING, message));
                    }).setDecimalLiteralTreatment(AS_DECIMAL).build());
            AstExpressionToRowExpression astExpressionToRowExpression = new AstExpressionToRowExpression(functionResolution, typeManager);
            ApplyAliasesRewriter applyAliasesRewriter = new ApplyAliasesRewriter();
            // Expression configured on a derived column
            RowExpression derivedColumnRowExpression = astExpressionToRowExpression.process(expression, columnsMap);
            // Apply column aliases, as per the TableScan's assignment map.
            RowExpression aliasedDerivedColExpression = new RowExpressionTreeRewriter<>(applyAliasesRewriter).rewrite(derivedColumnRowExpression, aliasMap);
            VariableReferenceExpression derivedColumn = new VariableReferenceExpression(Optional.empty(), udfSpec.getDerivedColumnName(),
                    columnsMap.get(udfSpec.getDerivedColumnName()).getType());
            String derivedColumnUniqueId = udfSpec.getDerivedColumnName() + "_" + tableMetadata.getTable().toString();
            if (derivedColumnAliasMap.containsKey(derivedColumnUniqueId)) {
                derivedColumn = derivedColumnAliasMap.get(derivedColumnUniqueId);
            }
            else {
                if (derivedColumnAliasMap.containsValue(derivedColumn)) {
                    derivedColumn = variableAllocator.newVariable(udfSpec.getDerivedColumnName(), columnsMap.get(udfSpec.getDerivedColumnName()).getType());
                }
                derivedColumnAliasMap.put(derivedColumnUniqueId, derivedColumn);
            }

            IcebergColumnHandle derivedColumnHandle = derivedColumnHandleGenerator.apply(udfSpec.getDerivedColumnName());
            newAssignmentsMap.put(derivedColumn, derivedColumnHandle);
            columnIdentityMap.put(derivedColumn.getName(), derivedColumnHandle.getColumnIdentity());
            if (!aliasedDerivedColExpression.getType().equals(derivedColumn.getType())) {
                session.getWarningCollector().add(new PrestoWarning(StandardWarningCode.SEMANTIC_WARNING,
                        format("derivedColumn: %s 's Type: %s did not match with return type :%s of the expression :%s, consider adding explicit cast",
                                derivedColumn.getName(), derivedColumn.getType(), aliasedDerivedColExpression.getType(), udfSpec.getDerivedColumnExpression())));
                // Fail and instruct user to add explicit cast, if the base type itself is different e.g double and decimal / char and varchar / integer and varchar.
                checkState(aliasedDerivedColExpression.getType().getTypeSignature().getBase().equals(derivedColumn.getType().getTypeSignature().getBase()),
                        format("derivedColumn: %s 's Type: %s did not match with return type :%s of the expression :%s, consider adding explicit cast",
                                derivedColumn.getName(), derivedColumn.getType(), aliasedDerivedColExpression.getType(), udfSpec.getDerivedColumnExpression()));
            }
            derivedColumnExpressionToDerivedColumnMap.put(aliasedDerivedColExpression, derivedColumn);
        });

        return new ExpressionRewriteInfoMaps(columnIdentityMap, newAssignmentsMap, columnsMap, derivedColumnExpressionToDerivedColumnMap);
    }

    private record ExpressionRewriteInfoMaps(
            Map<String, ColumnIdentity> columnIdentityMap,
            Map<VariableReferenceExpression, IcebergColumnHandle> newAssignmentsMap,
            Map<String, ColumnMetadata> columnsMap,
            TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap) {}
}
