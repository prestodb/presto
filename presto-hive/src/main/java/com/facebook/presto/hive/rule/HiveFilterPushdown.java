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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.hive.HiveBucketHandle;
import com.facebook.presto.hive.HiveBucketing;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HivePartitionResult;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.HiveTransactionManager;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Functions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.expressions.DynamicFilters.isDynamicFilter;
import static com.facebook.presto.expressions.DynamicFilters.removeNestedDynamicFilters;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.expressions.RowExpressionNodeInliner.replaceExpression;
import static com.facebook.presto.hive.HiveSessionProperties.isParquetPushdownFilterEnabled;
import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.hive.HiveWarningCode.HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getMetastoreHeaders;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isUserDefinedTypeEncodingEnabled;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableBiMap.toImmutableBiMap;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Runs during both logical and physical phases of connector-aided plan optimization.
 * In most cases filter pushdown will occur during logical phase. However, in cases
 * when new filter is added between logical and physical phases, e.g. a filter on a join
 * key from one side of a join is added to the other side, the new filter will get
 * merged with the one already pushed down.
 */
public class HiveFilterPushdown
        implements ConnectorPlanOptimizer
{
    private static final ConnectorTableLayout EMPTY_TABLE_LAYOUT = new ConnectorTableLayout(
            new ConnectorTableLayoutHandle() {},
            Optional.empty(),
            TupleDomain.none(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            emptyList());

    protected final HiveTransactionManager transactionManager;
    protected final RowExpressionService rowExpressionService;
    protected final StandardFunctionResolution functionResolution;
    protected final HivePartitionManager partitionManager;
    protected final FunctionMetadataManager functionMetadataManager;

    public HiveFilterPushdown(
            HiveTransactionManager transactionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution,
            HivePartitionManager partitionManager,
            FunctionMetadataManager functionMetadataManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "functionMetadataManager is null");
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new Rewriter(session, idAllocator), maxSubplan);
    }

    /**
     * If this method returns true, the expression will not be pushed inside the TableScan node.
     * This is exposed for dynamic or computed column functionality support.
     * Consider the following case:
     * Read a base row from the file. Modify the row to add a new column or update an existing column
     * all inside the TableScan. If filters are pushed down inside the TableScan, it would try to apply
     * it on base row. In these cases, override this method and return true. This will prevent the
     * expression from being pushed into the TableScan but will wrap the TableScanNode in a FilterNode.
     * @param expression expression to be evaluated.
     * @param tableHandle tableHandler where the expression to be evaluated.
     * @param columnHandleMap column name to column handle Map for all columns in the table.
     * @return true, if this expression should not be pushed inside the table scan, else false.
     */
    protected boolean useDynamicFilter(RowExpression expression, ConnectorTableHandle tableHandle, Map<String, ColumnHandle> columnHandleMap)
    {
        return false;
    }

    protected ConnectorPushdownFilterResult pushdownFilter(
            ConnectorSession session,
            HiveMetadata metadata,
            ConnectorTableHandle tableHandle,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        return pushdownFilter(
                session,
                metadata,
                metadata.getMetastore(),
                tableHandle,
                filter,
                currentLayoutHandle);
    }

    public ConnectorPushdownFilterResult pushdownFilter(
            ConnectorSession session,
            ConnectorMetadata metadata,
            SemiTransactionalHiveMetastore metastore,
            ConnectorTableHandle tableHandle,
            RowExpression filter,
            Optional<ConnectorTableLayoutHandle> currentLayoutHandle)
    {
        checkArgument(!FALSE_CONSTANT.equals(filter), "Cannot pushdown filter that is always false");
        if (TRUE_CONSTANT.equals(filter) && currentLayoutHandle.isPresent()) {
            return new ConnectorPushdownFilterResult(metadata.getTableLayout(session, currentLayoutHandle.get()), TRUE_CONSTANT);
        }

        // Split the filter into 3 groups of conjuncts:
        //  - range filters that apply to entire columns,
        //  - range filters that apply to subfields,
        //  - the rest. Intersect these with possibly pre-existing filters.
        DomainTranslator.ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                .fromPredicate(session, filter, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session).toColumnExtractor());
        if (currentLayoutHandle.isPresent()) {
            HiveTableLayoutHandle currentHiveLayout = (HiveTableLayoutHandle) currentLayoutHandle.get();
            decomposedFilter = intersectExtractionResult(new DomainTranslator.ExtractionResult(currentHiveLayout.getDomainPredicate(), currentHiveLayout.getRemainingPredicate()), decomposedFilter);
        }

        if (decomposedFilter.getTupleDomain().isNone()) {
            return new ConnectorPushdownFilterResult(EMPTY_TABLE_LAYOUT, FALSE_CONSTANT);
        }

        RowExpression optimizedRemainingExpression = rowExpressionService.getExpressionOptimizer().optimize(decomposedFilter.getRemainingExpression(), OPTIMIZED, session);
        if (optimizedRemainingExpression instanceof ConstantExpression) {
            ConstantExpression constantExpression = (ConstantExpression) optimizedRemainingExpression;
            if (FALSE_CONSTANT.equals(constantExpression) || constantExpression.getValue() == null) {
                return new ConnectorPushdownFilterResult(EMPTY_TABLE_LAYOUT, FALSE_CONSTANT);
            }
        }
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        TupleDomain<ColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(columnHandles::get);
        if (currentLayoutHandle.isPresent()) {
            entireColumnDomain = entireColumnDomain.intersect(((HiveTableLayoutHandle) (currentLayoutHandle.get())).getPartitionColumnPredicate());
        }

        Constraint<ColumnHandle> constraint = new Constraint<>(entireColumnDomain);

        // Extract deterministic conjuncts that apply to partition columns and specify these as Constraint#predicate
        if (!TRUE_CONSTANT.equals(decomposedFilter.getRemainingExpression())) {
            LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(rowExpressionService.getDeterminismEvaluator(), functionResolution, functionMetadataManager);
            RowExpression deterministicPredicate = logicalRowExpressions.filterDeterministicConjuncts(decomposedFilter.getRemainingExpression());
            if (!TRUE_CONSTANT.equals(deterministicPredicate)) {
                ConstraintEvaluator evaluator = new ConstraintEvaluator(rowExpressionService, session, columnHandles, deterministicPredicate);
                constraint = new Constraint<>(entireColumnDomain, evaluator::isCandidate);
            }
        }

        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(metastore, tableHandle, constraint, session);

        TupleDomain<Subfield> domainPredicate = withColumnDomains(ImmutableMap.<Subfield, Domain>builder()
                .putAll(hivePartitionResult.getUnenforcedConstraint()
                        .transform(HiveFilterPushdown::toSubfield)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .putAll(decomposedFilter.getTupleDomain()
                        .transform(subfield -> !isEntireColumn(subfield) ? subfield : null)
                        .getDomains()
                        .orElse(ImmutableMap.of()))
                .build());

        Set<String> predicateColumnNames = new HashSet<>();
        domainPredicate.getDomains().get().keySet().stream()
                .map(Subfield::getRootName)
                .forEach(predicateColumnNames::add);
        // Include only columns referenced in the optimized expression. Although the expression is sent to the worker node
        // unoptimized, the worker is expected to optimize the expression before executing.
        extractVariableExpressions(optimizedRemainingExpression).stream()
                .map(VariableReferenceExpression::getName)
                .forEach(predicateColumnNames::add);

        Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();

        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(rowExpressionService.getDeterminismEvaluator(), functionResolution, functionMetadataManager);
        List<RowExpression> conjuncts = extractConjuncts(decomposedFilter.getRemainingExpression());
        ImmutableList.Builder<RowExpression> dynamicConjuncts = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> staticConjuncts = ImmutableList.builder();
        for (RowExpression conjunct : conjuncts) {
            if (isDynamicFilter(conjunct) || useDynamicFilter(conjunct, tableHandle, columnHandles)) {
                dynamicConjuncts.add(conjunct);
            }
            else {
                staticConjuncts.add(conjunct);
            }
        }
        RowExpression dynamicFilterExpression = logicalRowExpressions.combineConjuncts(dynamicConjuncts.build());
        RowExpression remainingExpression = logicalRowExpressions.combineConjuncts(staticConjuncts.build());
        remainingExpression = removeNestedDynamicFilters(remainingExpression);

        MetastoreContext context = new MetastoreContext(session.getIdentity(), session.getQueryId(), session.getClientInfo(), session.getSource(), getMetastoreHeaders(session), isUserDefinedTypeEncodingEnabled(session), metastore.getColumnConverterProvider());
        Table table = metastore.getTable(context, hiveTableHandle)
                .orElseThrow(() -> new TableNotFoundException(tableName));
        String layoutString = createTableLayoutString(
                session,
                rowExpressionService,
                tableName,
                hivePartitionResult.getBucketHandle(),
                hivePartitionResult.getBucketFilter(),
                remainingExpression,
                domainPredicate);

        Optional<Set<HiveColumnHandle>> requestedColumns = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).getRequestedColumns()).orElse(Optional.empty());

        boolean appendRowNumbereEnabled = currentLayoutHandle.map(layout -> ((HiveTableLayoutHandle) layout).isAppendRowNumberEnabled()).orElse(false);
        return new ConnectorPushdownFilterResult(
                metadata.getTableLayout(
                        session,
                        new HiveTableLayoutHandle.Builder()
                                .setSchemaTableName(tableName)
                                .setTablePath(table.getStorage().getLocation())
                                .setPartitionColumns(hivePartitionResult.getPartitionColumns())
                                .setDataColumns(pruneColumnComments(hivePartitionResult.getDataColumns()))
                                .setTableParameters(hivePartitionResult.getTableParameters())
                                .setDomainPredicate(domainPredicate)
                                .setRemainingPredicate(remainingExpression)
                                .setPredicateColumns(predicateColumns)
                                .setPartitionColumnPredicate(hivePartitionResult.getEnforcedConstraint())
                                .setPartitions(hivePartitionResult.getPartitions())
                                .setBucketHandle(hivePartitionResult.getBucketHandle())
                                .setBucketFilter(hivePartitionResult.getBucketFilter())
                                .setPushdownFilterEnabled(true)
                                .setLayoutString(layoutString)
                                .setRequestedColumns(requestedColumns)
                                .setPartialAggregationsPushedDown(false)
                                .setAppendRowNumberEnabled(appendRowNumbereEnabled)
                                .setHiveTableHandle(hiveTableHandle)
                                .build()),
                dynamicFilterExpression);
    }

    public static class ConnectorPushdownFilterResult
    {
        private final ConnectorTableLayout layout;
        private final RowExpression unenforcedConstraint;

        public ConnectorPushdownFilterResult(ConnectorTableLayout layout, RowExpression unenforcedConstraint)
        {
            this.layout = requireNonNull(layout, "layout is null");
            this.unenforcedConstraint = requireNonNull(unenforcedConstraint, "unenforcedConstraint is null");
        }

        public ConnectorTableLayout getLayout()
        {
            return layout;
        }

        public RowExpression getUnenforcedConstraint()
        {
            return unenforcedConstraint;
        }
    }

    private class Rewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        Rewriter(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, RewriteContext<Void> context)
        {
            if (!(filter.getSource() instanceof TableScanNode)) {
                return visitPlan(filter, context);
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();
            if (!isPushdownFilterSupported(session, tableScan.getTable())) {
                return filter;
            }

            RowExpression expression = filter.getPredicate();
            TableHandle handle = tableScan.getTable();
            HiveMetadata hiveMetadata = getMetadata(handle);

            BiMap<VariableReferenceExpression, VariableReferenceExpression> symbolToColumnMapping = tableScan.getAssignments().entrySet().stream()
                    .collect(toImmutableBiMap(
                            Map.Entry::getKey,
                            entry -> new VariableReferenceExpression(Optional.empty(), getColumnName(session, hiveMetadata, handle.getConnectorHandle(), entry.getValue()), entry.getKey().getType())));

            RowExpression replacedExpression = replaceExpression(expression, symbolToColumnMapping);
            // replaceExpression() may further optimize the expression; if the resulting expression is always false, then return empty Values node
            if (FALSE_CONSTANT.equals(replacedExpression)) {
                session.getWarningCollector().add((new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    hiveMetadata,
                    handle.getConnectorHandle(),
                    replacedExpression,
                    handle.getLayout());

            ConnectorTableLayout layout = pushdownFilterResult.getLayout();
            if (layout.getPredicate().isNone()) {
                session.getWarningCollector().add((new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName()))));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            TableScanNode node = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), Optional.of(pushdownFilterResult.getLayout().getHandle())),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getTableConstraints(),
                    layout.getPredicate(),
                    TupleDomain.all());

            RowExpression unenforcedFilter = pushdownFilterResult.getUnenforcedConstraint();
            if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
                return new FilterNode(tableScan.getSourceLocation(), idAllocator.getNextId(), node, replaceExpression(unenforcedFilter, symbolToColumnMapping.inverse()));
            }

            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScan, RewriteContext<Void> context)
        {
            if (!isPushdownFilterSupported(session, tableScan.getTable())) {
                return tableScan;
            }
            TableHandle handle = tableScan.getTable();
            HiveMetadata hiveMetadata = getMetadata(handle);
            ConnectorPushdownFilterResult pushdownFilterResult = pushdownFilter(
                    session,
                    hiveMetadata,
                    handle.getConnectorHandle(),
                    TRUE_CONSTANT,
                    handle.getLayout());
            if (pushdownFilterResult.getLayout().getPredicate().isNone()) {
                session.getWarningCollector().add(new PrestoWarning(HIVE_TABLESCAN_CONVERTED_TO_VALUESNODE, format("Table '%s' returns 0 rows, and is converted to an empty %s by %s", tableScan.getTable().getConnectorHandle(), ValuesNode.class.getSimpleName(), HiveFilterPushdown.class.getSimpleName())));
                return new ValuesNode(tableScan.getSourceLocation(), idAllocator.getNextId(), tableScan.getOutputVariables(), ImmutableList.of(), Optional.of(tableScan.getTable().getConnectorHandle().toString()));
            }

            TableScanNode node = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), Optional.of(pushdownFilterResult.getLayout().getHandle())),
                    tableScan.getOutputVariables(),
                    tableScan.getAssignments(),
                    tableScan.getTableConstraints(),
                    pushdownFilterResult.getLayout().getPredicate(),
                    TupleDomain.all());

            RowExpression unenforcedFilter = pushdownFilterResult.getUnenforcedConstraint();
            if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Unenforced filter found %s but not handled", unenforcedFilter));
            }
            return node;
        }
    }

    private static class ConstraintEvaluator
    {
        private final Map<String, ColumnHandle> assignments;
        private final RowExpressionService evaluator;
        private final ConnectorSession session;
        private final RowExpression expression;
        private final Set<ColumnHandle> arguments;

        public ConstraintEvaluator(RowExpressionService evaluator, ConnectorSession session, Map<String, ColumnHandle> assignments, RowExpression expression)
        {
            this.assignments = assignments;
            this.evaluator = evaluator;
            this.session = session;
            this.expression = expression;

            arguments = ImmutableSet.copyOf(extractVariableExpressions(expression)).stream()
                    .map(VariableReferenceExpression::getName)
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }

            Function<VariableReferenceExpression, Object> variableResolver = variable -> {
                ColumnHandle column = assignments.get(variable.getName());
                checkArgument(column != null, "Missing column assignment for %s", variable);

                if (!bindings.containsKey(column)) {
                    return variable;
                }

                return bindings.get(column).getValue();
            };

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = null;
            try {
                optimized = evaluator.getExpressionOptimizer().optimize(expression, OPTIMIZED, session, variableResolver);
            }
            catch (PrestoException e) {
                propagateIfUnhandled(e);
                return true;
            }

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null && (!(optimized instanceof ConstantExpression) || !((ConstantExpression) optimized).isNull());
        }

        private static void propagateIfUnhandled(PrestoException e)
                throws PrestoException
        {
            int errorCode = e.getErrorCode().getCode();
            if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                    || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                    || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                    || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
                return;
            }

            throw e;
        }
    }

    protected HiveMetadata getMetadata(TableHandle tableHandle)
    {
        ConnectorMetadata metadata = transactionManager.get(tableHandle.getTransaction());
        checkState(metadata instanceof HiveMetadata, "metadata must be HiveMetadata");
        return (HiveMetadata) metadata;
    }

    private static String getColumnName(ConnectorSession session, HiveMetadata metadata, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return metadata.getColumnMetadata(session, tableHandle, columnHandle).getName();
    }

    protected boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorHandle() instanceof HiveTableHandle, "pushdownFilter is never supported on a non-hive TableHandle");
        if (((HiveTableHandle) tableHandle.getConnectorHandle()).getAnalyzePartitionValues().isPresent()) {
            return false;
        }

        boolean pushdownFilterEnabled = HiveSessionProperties.isPushdownFilterEnabled(session);
        if (pushdownFilterEnabled) {
            HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(getMetadata(tableHandle).getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
            if (hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.DWRF || hiveStorageFormat == HiveStorageFormat.PARQUET && isParquetPushdownFilterEnabled(session)) {
                return true;
            }
        }
        return false;
    }

    private static DomainTranslator.ExtractionResult intersectExtractionResult(DomainTranslator.ExtractionResult left, DomainTranslator.ExtractionResult right)
    {
        RowExpression newRemainingExpression;
        if (right.getRemainingExpression().equals(TRUE_CONSTANT)) {
            newRemainingExpression = left.getRemainingExpression();
        }
        else if (left.getRemainingExpression().equals(TRUE_CONSTANT)) {
            newRemainingExpression = right.getRemainingExpression();
        }
        else {
            newRemainingExpression = and(left.getRemainingExpression(), right.getRemainingExpression());
        }
        return new DomainTranslator.ExtractionResult(left.getTupleDomain().intersect(right.getTupleDomain()), newRemainingExpression);
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static List<Column> pruneColumnComments(List<Column> columns)
    {
        return columns.stream()
                .map(column -> new Column(column.getName(), column.getType(), Optional.empty(), column.getTypeMetadata()))
                .collect(toImmutableList());
    }

    private static String createTableLayoutString(
            ConnectorSession session,
            RowExpressionService rowExpressionService,
            SchemaTableName tableName,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketing.HiveBucketFilter> bucketFilter,
            RowExpression remainingPredicate,
            TupleDomain<Subfield> domainPredicate)
    {
        return toStringHelper(tableName.toString())
                .omitNullValues()
                .add("buckets", bucketHandle.map(HiveBucketHandle::getReadBucketCount).orElse(null))
                .add("bucketsToKeep", bucketFilter.map(HiveBucketing.HiveBucketFilter::getBucketsToKeep).orElse(null))
                .add("filter", TRUE_CONSTANT.equals(remainingPredicate) ? null : rowExpressionService.formatRowExpression(session, remainingPredicate))
                .add("domains", domainPredicate.isAll() ? null : domainPredicate.toString(session.getSqlFunctionProperties()))
                .toString();
    }

    public static Set<VariableReferenceExpression> extractVariableExpressions(RowExpression expression)
    {
        ImmutableSet.Builder<VariableReferenceExpression> builder = ImmutableSet.builder();
        expression.accept(new VariableReferenceBuilderVisitor(), builder);
        return builder.build();
    }

    private static class VariableReferenceBuilderVisitor
            extends DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<VariableReferenceExpression>>
    {
        @Override
        public Void visitVariableReference(VariableReferenceExpression variable, ImmutableSet.Builder<VariableReferenceExpression> builder)
        {
            builder.add(variable);
            return null;
        }
    }

    private static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((HiveColumnHandle) columnHandle).getName(), ImmutableList.of());
    }
}
