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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.hive.BaseHiveColumnHandle;
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
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardWarningCode;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.derivedColumns.DerivedColumnSpec;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.filterValues;
import static com.google.common.collect.Maps.transformValues;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * SimplifySubExpressions's plan rewriter - rewrite expressions/subexpressions to their derived column equivalent.
 */
public class SimplifySubExpressionsRewriter
        extends ConnectorPlanRewriter<Set<VariableReferenceExpression>>
{
    private static final Logger LOG = Logger.get(SimplifySubExpressionsRewriter.class);

    private final ConnectorSession session;
    private final SqlParser sqlParser;
    private final VariableAllocator variableAllocator;
    private final StandardFunctionResolution functionResolution;
    private final FunctionMetadataManager functionMetadataManager;
    private final TypeManager typeManager;
    private final IcebergTransactionManager transactionManager;
    private final PlanNodeIdAllocator idAllocator;
    private final BiMap<String, VariableReferenceExpression> derivedColumnAliasMap;
    private final Map<VariableReferenceExpression, IcebergColumnHandle> newAssignmentsMap;

    public SimplifySubExpressionsRewriter(
            StandardFunctionResolution functionResolution,
            FunctionMetadataManager functionMetadataManager,
            TypeManager typeManager,
            IcebergTransactionManager transactionManager,
            PlanNodeIdAllocator idAllocator,
            ConnectorSession session,
            SqlParser sqlParser,
            VariableAllocator variableAllocator)
    {
        this.functionResolution = functionResolution;
        this.functionMetadataManager = functionMetadataManager;
        this.typeManager = typeManager;
        this.transactionManager = transactionManager;
        this.idAllocator = idAllocator;
        this.session = session;
        this.sqlParser = sqlParser;
        this.variableAllocator = variableAllocator;

        // TODO: see if we can do without these global and mutable maps.
        this.derivedColumnAliasMap = HashBiMap.create();
        this.newAssignmentsMap = new HashMap<>();
    }

    private static VariableReferenceExpression colHandleToVariableRef(ColumnHandle columnHandle)
    {
        requireNonNull(columnHandle);
        Type type = ((IcebergColumnHandle) columnHandle).getType();
        String columnName = ((IcebergColumnHandle) columnHandle).getName();
        return new VariableReferenceExpression(Optional.empty(), columnName, type);
    }

    @Override
    public PlanNode visitJoin(JoinNode node, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        PlanNode left = node.getLeft();
        PlanNode right = node.getRight();
        List<VariableReferenceExpression> leftOldOutputVariables = left.getOutputVariables();
        List<VariableReferenceExpression> rightOldOutputVariables = right.getOutputVariables();
        Optional<RowExpression> filterPredicate = node.getFilter();
        ImmutableSet.Builder<VariableReferenceExpression> derivedColumnsAddedBuilder = ImmutableSet.builder();
        if (context.get() != null && !context.get().isEmpty()) {
            derivedColumnsAddedBuilder.addAll(context.get());
        }
        if (filterPredicate.isPresent()) {
            RewrittenRowExpression rewrittenRowExpression = rewriteFilterPredicate(node, filterPredicate.get());
            filterPredicate = Optional.of(rewrittenRowExpression.rewrittenExpression());
            derivedColumnsAddedBuilder.addAll(rewrittenRowExpression.derivedColumnsAdded());
        }
        left = context.rewrite(left, derivedColumnsAddedBuilder.build());
        // Sets difference should be safe because we always add more output variables and never prune.
        checkState(leftOldOutputVariables.size() <= left.getOutputVariables().size(), "Rewrite should not remove output variables.");
        Set<VariableReferenceExpression> leftDiff = Sets.difference(ImmutableSet.copyOf(left.getOutputVariables()), ImmutableSet.copyOf(leftOldOutputVariables));
        LinkedList<VariableReferenceExpression> outputVariables = new LinkedList<>(node.getOutputVariables());
        right = context.rewrite(right, derivedColumnsAddedBuilder.build());
        checkState(rightOldOutputVariables.size() <= right.getOutputVariables().size(), "Rewrite should not remove output variables.");
        Set<VariableReferenceExpression> rightDiff = Sets.difference(ImmutableSet.copyOf(right.getOutputVariables()), ImmutableSet.copyOf(rightOldOutputVariables));
        leftDiff.forEach(outputVariables::addFirst);
        outputVariables.addAll(rightDiff);

        return new JoinNode(
                node.getSourceLocation(),
                idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode(),
                node.getType(),
                left, right,
                node.getCriteria(),
                outputVariables,
                filterPredicate,
                node.getLeftHashVariable(),
                node.getRightHashVariable(),
                node.getDistributionType(),
                node.getDynamicFilters());
    }

    @Override
    public PlanNode visitProject(ProjectNode projectNode, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        Set<TableScanNode> tableScanNodes = projectNode.accept(new FindTableScanNodesPlanVisitor(), null);
        Assignments assignments = projectNode.getAssignments();
        ImmutableSet.Builder<VariableReferenceExpression> derivedColumnsAddedBuilder = ImmutableSet.builder();
        if (context.get() != null && !context.get().isEmpty()) {
            derivedColumnsAddedBuilder.addAll(context.get());
        }
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> assignmentsMapBuilder = ImmutableMap.builder();
        assignmentsMapBuilder.putAll(assignments.getMap());
        for (TableScanNode tableScanNode : tableScanNodes) {
            ConnectorTableMetadata tableMetadata = getConnectorTableMetadata(tableScanNode);
            List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                    .filter(colString -> colString != null && !colString.isBlank()).toList();
            if (!derivedColumns.isEmpty()) {
                TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap = buildExpressionRewriteInfoMaps(tableScanNode);
                derivedColumnsAddedBuilder.build().forEach(col -> {
                    if (derivedColumnExpressionToDerivedColumnMap.containsValue(col)) {
                        assignmentsMapBuilder.put(col, col);
                    }
                });

                Map<VariableReferenceExpression, RewrittenRowExpression> rewrittenAssignments = transformValues(assignmentsMapBuilder.buildKeepingLast(),
                        rowExpression -> rowExpression.accept(new RewriteCommonSubExpression(), derivedColumnExpressionToDerivedColumnMap));
                derivedColumnsAddedBuilder.addAll(rewrittenAssignments.values().stream()
                        .flatMap(rewrittenRowExpression -> rewrittenRowExpression.derivedColumnsAdded().stream()).collect(toImmutableSet()));
                assignmentsMapBuilder.putAll(transformValues(rewrittenAssignments, RewrittenRowExpression::rewrittenExpression));
            }
        }
        // TODO: Wish to avoid buildKeepingLast - there should be a better way!
        ProjectNode newProjectNode = new ProjectNode(projectNode.getSourceLocation(), idAllocator.getNextId(), projectNode.getSource(),
                new Assignments(assignmentsMapBuilder.buildKeepingLast()), projectNode.getLocality());
        return context.defaultRewrite(newProjectNode, derivedColumnsAddedBuilder.build());
    }

    @Override
    public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        final Set<VariableReferenceExpression> derivedColumnsAdded = getApplicableDerivedColumns(tableScan, context);
        if (!derivedColumnsAdded.isEmpty()) {
            TableHandle handle = tableScan.getTable();
            Set<String> unaliasedDerivedColumnsAdded = derivedColumnsAdded.stream().map(newAssignmentsMap::get).map(BaseHiveColumnHandle::getName).collect(toImmutableSet());
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> tableAssignmentsBuilder = ImmutableMap.<VariableReferenceExpression, ColumnHandle>builder()
                    .putAll(filterValues(tableScan.getAssignments(), colHandle -> !unaliasedDerivedColumnsAdded.contains(((IcebergColumnHandle) colHandle).getName())));

            derivedColumnsAdded.forEach(col -> tableAssignmentsBuilder.put(col, newAssignmentsMap.get(col)));
            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            icebergTableLayoutHandle.getDomainPredicate(),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            Optional.of(ImmutableSet.<IcebergColumnHandle>builder().addAll(icebergTableLayoutHandle.getRequestedColumns().orElse(ImmutableSet.of()))
                                    .addAll(derivedColumnsAdded.stream().map(newAssignmentsMap::get).collect(toImmutableSet())).build()),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            icebergTableLayoutHandle.getPartitionColumnPredicate(),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));

            return new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    tableAssignmentsBuilder.build().keySet().stream().toList(),
                    tableAssignmentsBuilder.build(),
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
        }
        return tableScan;
    }

    @Override
    public PlanNode visitFilter(FilterNode filter, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        RewrittenRowExpression rewrittenRowExpression = rewriteFilterPredicate(filter, filter.getPredicate());
        Set<VariableReferenceExpression> derivedColumnsAdded;
        if (context.get() != null && !context.get().isEmpty()) {
            derivedColumnsAdded = ImmutableSet.<VariableReferenceExpression>builder().addAll(context.get()).addAll(rewrittenRowExpression.derivedColumnsAdded()).build();
        }
        else {
            derivedColumnsAdded = rewrittenRowExpression.derivedColumnsAdded();
        }

        RowExpression filterPredicate = rewrittenRowExpression.rewrittenExpression();
        if (!filterPredicate.equals(filter.getPredicate())) {
            filter = new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), filter.getStatsEquivalentPlanNode(), filter.getSource(), filterPredicate);
        }
        return context.defaultRewrite(filter, derivedColumnsAdded);
    }

    @Override
    public PlanNode visitAggregation(AggregationNode node, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        Set<TableScanNode> tableScanNodes = node.accept(new FindTableScanNodesPlanVisitor(), null);
        Set<VariableReferenceExpression> derivedColumnsAdded =
                tableScanNodes.stream().flatMap(tableScanNode -> getApplicableDerivedColumns(tableScanNode, context).stream()).collect(toImmutableSet());
        List<VariableReferenceExpression> newGroupingKeys = ImmutableList.<VariableReferenceExpression>builder().addAll(derivedColumnsAdded).addAll(node.getGroupingKeys()).build();
        AggregationNode.GroupingSetDescriptor groupingSets = new AggregationNode.GroupingSetDescriptor(newGroupingKeys, node.getGroupingSetCount(), node.getGlobalGroupingSets());
        PlanNode source = context.rewrite(node.getSource(), derivedColumnsAdded);
        return new AggregationNode(node.getSourceLocation(), idAllocator.getNextId(),
                node.getStatsEquivalentPlanNode(), source, node.getAggregations(), groupingSets, node.getPreGroupedVariables(),
                node.getStep(), node.getHashVariable(), node.getGroupIdVariable(), node.getAggregationId());
    }

    private TreeMap<RowExpression, RowExpression> buildExpressionRewriteInfoMaps(TableScanNode tableScan)
    {
        ConnectorTableMetadata tableMetadata = getConnectorTableMetadata(tableScan);
        Map<String, ColumnMetadata> columnMetadataMap = tableMetadata.getColumns().stream()
                .collect(toImmutableMap(ColumnMetadata::getName, col -> col));
        List<DerivedColumnSpec> derivedColumnSpecs = IcebergTableProperties.getDerivedColumnSpec(tableMetadata.getProperties()).getDerivedColumnSpecs();
        TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap = new TreeMap<>(new RowExpressionComparator());
        Map<VariableReferenceExpression, VariableReferenceExpression> aliasMap = tableScan.getAssignments().entrySet().stream().collect(
                toImmutableMap(entry -> colHandleToVariableRef(entry.getValue()), Map.Entry::getKey));
        for (DerivedColumnSpec udfSpec : derivedColumnSpecs) {
            RowExpression derivedColumnRowExpression = parseExpressionString(udfSpec.getDerivedColumnExpression(), columnMetadataMap);
            // Apply column aliases, as per the TableScan's assignment map.
            RowExpression aliasedDerivedColExpression = new RowExpressionTreeRewriter<>(new ApplyAliasesRewriter()).rewrite(derivedColumnRowExpression, aliasMap);
            VariableReferenceExpression derivedColumn = getDerivedColumnUniquelyAllocated(udfSpec, columnMetadataMap, tableMetadata);
            newAssignmentsMap.put(derivedColumn, getIcebergColumnHandle(tableScan, udfSpec.getDerivedColumnName()));
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
        }
        return derivedColumnExpressionToDerivedColumnMap;
    }

    private RewrittenRowExpression rewriteFilterPredicate(PlanNode node, RowExpression filterPredicate)
    {
        Set<TableScanNode> tableScanNodes = node.accept(new FindTableScanNodesPlanVisitor(), null);
        RowExpression filterPredicateRewritten = filterPredicate;
        Set<VariableReferenceExpression> derivedColumnsAdded = ImmutableSet.of();
        for (TableScanNode tableScanNode : tableScanNodes) {
            ConnectorTableMetadata tableMetadata = getConnectorTableMetadata(tableScanNode);
            List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                    .filter(colString -> colString != null && !colString.isBlank()).toList();
            if (!derivedColumns.isEmpty()) {
                TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap = buildExpressionRewriteInfoMaps(tableScanNode);
                RewrittenRowExpression rewrittenRowExpression =
                        filterPredicateRewritten.accept(new RewriteCommonSubExpression(), derivedColumnExpressionToDerivedColumnMap);
                derivedColumnsAdded = ImmutableSet.<VariableReferenceExpression>builder()
                        .addAll(rewrittenRowExpression.derivedColumnsAdded())
                        .addAll(derivedColumnsAdded).build();
                // We are incrementally rewriting sub-expr of this filter predicate, inspecting each tablescan and fetching derived column info.
                filterPredicateRewritten = rewrittenRowExpression.rewrittenExpression();
            }
        }
        return new RewrittenRowExpression(filterPredicateRewritten, derivedColumnsAdded);
    }

    // Generate an alias for derived column if more than one table has same name for it's derived column.
    private VariableReferenceExpression getDerivedColumnUniquelyAllocated(DerivedColumnSpec udfSpec, Map<String, ColumnMetadata> columnMetadataMap, ConnectorTableMetadata tableMetadata)
    {
        VariableReferenceExpression derivedColumn = new VariableReferenceExpression(Optional.empty(), udfSpec.getDerivedColumnName(),
                columnMetadataMap.get(udfSpec.getDerivedColumnName()).getType());
        String derivedColumnUniqueId = getDerivedColumnUniqueId(tableMetadata, udfSpec.getDerivedColumnName());
        if (derivedColumnAliasMap.containsKey(derivedColumnUniqueId)) {
            derivedColumn = derivedColumnAliasMap.get(derivedColumnUniqueId);
        }
        else {
            if (derivedColumnAliasMap.containsValue(derivedColumn)) {
                derivedColumn = variableAllocator.newVariable(udfSpec.getDerivedColumnName(), columnMetadataMap.get(udfSpec.getDerivedColumnName()).getType());
            }
            derivedColumnAliasMap.put(derivedColumnUniqueId, derivedColumn);
        }
        return derivedColumn;
    }

    private IcebergAbstractMetadata getIcebergMetadata(TableScanNode tableScan)
    {
        TableHandle handle = tableScan.getTable();
        return (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
    }

    private IcebergColumnHandle getIcebergColumnHandle(TableScanNode tableScan, String colName)
    {
        SchemaTableName schemaTableName = ((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getSchemaTableName();
        Types.NestedField nestedField = IcebergUtil.getIcebergTable(getIcebergMetadata(tableScan), session,
                schemaTableName).schema().caseInsensitiveFindField(colName);
        return IcebergColumnHandle.create(nestedField, typeManager, BaseHiveColumnHandle.ColumnType.REGULAR);
    }

    private static String getDerivedColumnUniqueId(ConnectorTableMetadata tableMetadata, String derivedColumnName)
    {
        return derivedColumnName + "_" + tableMetadata.getTable().toString();
    }

    private Set<VariableReferenceExpression> getApplicableDerivedColumns(TableScanNode tableScan, RewriteContext<Set<VariableReferenceExpression>> context)
    {
        final Set<VariableReferenceExpression> derivedColumnsAdded;
        if (context.get() != null && !context.get().isEmpty()) {
            ConnectorTableMetadata tableMetadata = getConnectorTableMetadata(tableScan);
            List<String> derivedColumnsConfigured = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream()
                    .filter(colString -> colString != null && !colString.isBlank()).toList();
            Set<VariableReferenceExpression> derivedColumnAliased = derivedColumnsConfigured.stream().filter(col -> derivedColumnAliasMap.containsKey(getDerivedColumnUniqueId(tableMetadata, col)))
                    .map(col -> derivedColumnAliasMap.get(getDerivedColumnUniqueId(tableMetadata, col))).collect(toImmutableSet());
            derivedColumnsAdded = Sets.intersection(derivedColumnAliased, context.get());
        }
        else {
            derivedColumnsAdded = ImmutableSet.of();
        }
        return derivedColumnsAdded;
    }

    private ConnectorTableMetadata getConnectorTableMetadata(TableScanNode tableScan)
    {
        TableHandle handle = tableScan.getTable();
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
        return metadata.getTableMetadata(session, tableHandle);
    }

    private RowExpression parseExpressionString(String unprocessedExpressionString, Map<String, ColumnMetadata> columnsMap)
    {
        Expression expression = sqlParser.createExpression(unprocessedExpressionString,
                ParsingOptions.builder().setWarningConsumer(parsingWarning -> {
                    String message = format("derived column expression: %s has parse warnings: %s", unprocessedExpressionString, parsingWarning.getMessage());
                    LOG.warn(message);
                    session.getWarningCollector().add(new PrestoWarning(PARSER_WARNING, message));
                }).setDecimalLiteralTreatment(AS_DECIMAL).build());
        AstExpressionToRowExpression astExpressionToRowExpression = new AstExpressionToRowExpression(functionResolution, functionMetadataManager, typeManager, session.getSqlFunctionProperties());
        // Expression configured on a derived column as RowExpression
        return astExpressionToRowExpression.process(expression, columnsMap);
    }
}
