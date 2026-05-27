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
import com.facebook.presto.spi.derivedColumns.DerivedColumnSpec;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;

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

public class FilterPredicateCSERewriter
        extends ConnectorPlanRewriter<Void>
{
    private static final Logger LOG = Logger.get(FilterPredicateCSERewriter.class);

    private final ConnectorSession session;
    private final SqlParser sqlParser;
    private final IcebergTableProperties tableProperties;
    private final StandardFunctionResolution functionResolution;
    private final TypeManager typeManager;
    private final FunctionMetadataManager functionMetadataManager;
    private final IcebergTransactionManager transactionManager;
    private final PlanNodeIdAllocator idAllocator;

    public FilterPredicateCSERewriter(IcebergTableProperties tableProperties,
            StandardFunctionResolution functionResolution,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager,
            IcebergTransactionManager transactionManager,
            PlanNodeIdAllocator idAllocator,
            ConnectorSession session,
            SqlParser sqlParser)
    {
        this.tableProperties = tableProperties;
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
        this.transactionManager = transactionManager;
        this.idAllocator = idAllocator;
        this.session = session;
        this.sqlParser = sqlParser;
    }

    @Override
    public PlanNode visitFilter(FilterNode filter, RewriteContext<Void> context)
    {
        if (!isDerivedColumnsEnabled(session)) {
            return filter;
        }
        TableScanNode tableScan = (TableScanNode) filter.getSource();
        TableHandle handle = tableScan.getTable();
        IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
        IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        List<String> derivedColumns = IcebergTableProperties.getDerivedColumns(tableMetadata.getProperties()).stream().filter(x -> !x.isBlank()).toList();
        if (derivedColumns.isEmpty()) {
            return filter;
        }

        Map<String, ColumnIdentity> columnIdentityMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                .collect(Collectors.toMap(ColumnMetadata::getName, col ->
                {
                    Types.NestedField nestedField = IcebergUtil.getIcebergTable(metadata, session,
                            ((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getSchemaTableName()).schema().caseInsensitiveFindField(col.getName());
                    return ColumnIdentity.createColumnIdentity(nestedField);
                }));

        Map<String, ColumnMetadata> columnsMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                .collect(Collectors.toMap(ColumnMetadata::getName, col -> col));
        checkState(columnsMap.keySet().containsAll(derivedColumns),
                format("inconsistent derived column definition, derived columns: %s does not exist in table: %s", Joiner.on(',').join(derivedColumns),
                        tableHandle.getIcebergTableName()));
        List<DerivedColumnSpec> derivedColumnSpecs = IcebergTableProperties.getDerivedColumnSpec(tableMetadata.getProperties()).getDerivedColumnSpecs();
        TreeMap<RowExpression, RowExpression> derivedColumnExpressionToDerivedColumnMap = new TreeMap<>(new RowExpressionComparator());

        derivedColumnSpecs.forEach(udfSpec -> {
            Expression expression = sqlParser.createExpression(udfSpec.getDerivedColumnExpression(),
                    ParsingOptions.builder().setWarningConsumer(parsingWarning -> {
                        String message = format("derived column expression: %s has parse warnings: %s", udfSpec.getDerivedColumnExpression(), parsingWarning.getMessage());
                        LOG.warn(message);
                        session.getWarningCollector().add(new PrestoWarning(PARSER_WARNING, message));
                    }).setDecimalLiteralTreatment(AS_DECIMAL).build());
            AstExpressionToRowExpression astExpressionToRowExpression = new AstExpressionToRowExpression(functionResolution, typeManager);
            // Expression configured on a derived column
            RowExpression derivedColumnRowExpression = astExpressionToRowExpression.process(expression, columnsMap);
            VariableReferenceExpression derivedColumn = new VariableReferenceExpression(Optional.empty(), udfSpec.getDerivedColumnName(),
                    columnsMap.get(udfSpec.getDerivedColumnName()).getType());
            if (!derivedColumnRowExpression.getType().equals(derivedColumn.getType())) {
                session.getWarningCollector().add(new PrestoWarning(StandardWarningCode.PERFORMANCE_WARNING,
                        format("derivedColumn: %s 's Type: %s did not match with return type :%s of the expression :%s",
                                derivedColumn.getName(), derivedColumn.getType(), derivedColumnRowExpression.getType(), udfSpec.getDerivedColumnExpression())));
                checkState(derivedColumnRowExpression.getType().getTypeSignature().getBase().equals(derivedColumn.getType().getTypeSignature().getBase()),
                        format("derivedColumn: %s 's Type: %s did not match with return type :%s of the expression :%s",
                                derivedColumn.getName(), derivedColumn.getType(), derivedColumnRowExpression.getType(), udfSpec.getDerivedColumnExpression()));
            }

            derivedColumnExpressionToDerivedColumnMap.put(derivedColumnRowExpression, derivedColumn);
        });
        RewrittenExpressionMetadata rewrittenExpressionMetadata = filter.getPredicate().accept(new RewriteCommonSubExpression(), derivedColumnExpressionToDerivedColumnMap);
        if (!rewrittenExpressionMetadata.derivedColumnsAdded().isEmpty()) {
            Set<VariableReferenceExpression> outputVariables = new HashSet<>(tableScan.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> tableAssignments = new HashMap<>(tableScan.getAssignments());
            RowExpression rewrittenFilterPredicate = rewrittenExpressionMetadata.rewrittenExpression();
            Function<VariableReferenceExpression, IcebergColumnHandle> derivedColumnHandle = varRef -> new IcebergColumnHandle(
                    columnIdentityMap.get(varRef.getName()),
                    columnsMap.get(varRef.getName()).getType(),
                    Optional.of("derived column"),
                    BaseHiveColumnHandle.ColumnType.REGULAR);
            if (!outputVariables.containsAll(rewrittenExpressionMetadata.derivedColumnsAdded())) {
                outputVariables.addAll(rewrittenExpressionMetadata.derivedColumnsAdded());
                tableAssignments.putAll(rewrittenExpressionMetadata.derivedColumnsAdded().stream()
                        .collect(Collectors.toMap(k -> k, derivedColumnHandle)));
            }
            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            icebergTableLayoutHandle.getDomainPredicate(),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            Optional.of(ImmutableSet.<IcebergColumnHandle>builder().addAll(icebergTableLayoutHandle.getRequestedColumns().orElse(ImmutableSet.of()))
                                    .addAll(rewrittenExpressionMetadata.derivedColumnsAdded().stream().map(derivedColumnHandle).collect(Collectors.toSet())).build()),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            icebergTableLayoutHandle.getPartitionColumnPredicate(),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));

            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    outputVariables.stream().toList(),
                    tableAssignments,
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
            return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), filter.getStatsEquivalentPlanNode(), newTableScan, rewrittenFilterPredicate);
        }
        return filter;
    }
}
