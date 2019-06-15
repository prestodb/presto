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
package com.facebook.presto.hive.optimization;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveMetadata;
import com.facebook.presto.hive.HivePartitionManager;
import com.facebook.presto.hive.HivePartitionResult;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.hive.HiveStorageFormat;
import com.facebook.presto.hive.HiveTableHandle;
import com.facebook.presto.hive.HiveTableLayoutHandle;
import com.facebook.presto.hive.SubfieldExtractor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveTableProperties.getHiveStorageFormat;
import static com.facebook.presto.spi.predicate.TupleDomain.withColumnDomains;
import static com.facebook.presto.spi.relation.LogicalRowExpressions.TRUE_CONSTANT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;

public class FilterPushdown
        implements ConnectorPlanOptimizer
{
    private final Function<ConnectorTransactionHandle, ConnectorMetadata> hiveMetadataProvider;
    private final HivePartitionManager partitionManager;
    private final RowExpressionService rowExpressionService;
    private final StandardFunctionResolution functionResolution;

    public FilterPushdown(
            Function<ConnectorTransactionHandle, ConnectorMetadata> hiveMetadataProvider,
            HivePartitionManager partitionManager,
            RowExpressionService rowExpressionService,
            StandardFunctionResolution functionResolution)
    {
        this.hiveMetadataProvider = requireNonNull(hiveMetadataProvider, "hiveMetadataProvider is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.rowExpressionService = requireNonNull(rowExpressionService, "rowExpressionService is null");
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public PlanNode optimize(
            PlanNode maxSubplan,
            ConnectorSession session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        if (!HiveSessionProperties.isPushdownFilterEnabled(session)) {
            return maxSubplan;
        }

        return maxSubplan.accept(new Visitor(session, idAllocator), null);
    }

    private boolean isPushdownFilterSupported(ConnectorSession session, TableHandle tableHandle)
    {
        if (((HiveTableHandle) tableHandle.getConnectorHandle()).getAnalyzePartitionValues().isPresent()) {
            return false;
        }

        ConnectorMetadata metadata = hiveMetadataProvider.apply(tableHandle.getTransaction());
        HiveStorageFormat hiveStorageFormat = getHiveStorageFormat(metadata.getTableMetadata(session, tableHandle.getConnectorHandle()).getProperties());
        return hiveStorageFormat == HiveStorageFormat.ORC || hiveStorageFormat == HiveStorageFormat.DWRF;
    }

    private PushdownFilterResult pushdownFilter(
            ConnectorSession session,
            TableHandle tableHandle,
            RowExpression filter,
            Map<String, String> symbolToColumnMapping)
    {
        HiveMetadata metadata = (HiveMetadata) hiveMetadataProvider.apply(tableHandle.getTransaction());

        if (TRUE_CONSTANT.equals(filter) && tableHandle.getLayout().isPresent()) {
            return new PushdownFilterResult(metadata.getTableLayout(session, tableHandle.getLayout().get()), TRUE_CONSTANT);
        }

        if (tableHandle.getLayout().isPresent()) {
            // TODO: do we wanna override existing table handle?
            // throw new UnsupportedOperationException("Partial filter pushdown is not supported");
        }

        // Split the filter into 3 groups of conjuncts:
        //  - range filters that apply to entire columns,
        //  - range filters that apply to subfields,
        //  - the rest
        DomainTranslator.ExtractionResult<Subfield> decomposedFilter = rowExpressionService.getDomainTranslator()
                .fromPredicate(session, filter, new SubfieldExtractor(functionResolution, rowExpressionService.getExpressionOptimizer(), session));

        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle.getConnectorHandle());
        TupleDomain<ColumnHandle> entireColumnDomain = decomposedFilter.getTupleDomain()
                .transform(subfield -> isEntireColumn(subfield) ? subfield.getRootName() : null)
                .transform(columnHandles::get);
        // TODO Extract deterministic conjuncts that apply to partition columns and specify these as Contraint#predicate
        HivePartitionResult hivePartitionResult = partitionManager.getPartitions(
                metadata.getMetastore(),
                tableHandle.getConnectorHandle(),
                new Constraint<>(entireColumnDomain),
                session);

        TupleDomain<Subfield> domainPredicate = withColumnDomains(ImmutableMap.<Subfield, Domain>builder()
                .putAll(hivePartitionResult.getUnenforcedConstraint()
                        .transform(FilterPushdown::toSubfield)
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
                .map(symbolToColumnMapping::get)
                .forEach(predicateColumnNames::add);
        extractAll(decomposedFilter.getRemainingExpression()).stream()
                .map(VariableReferenceExpression::getName)
                .forEach(predicateColumnNames::add);

        Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(HiveColumnHandle.class::cast)
                .collect(toImmutableMap(HiveColumnHandle::getName, Functions.identity()));

        return new PushdownFilterResult(
                metadata.getTableLayout(
                        session,
                        new HiveTableLayoutHandle(
                                ((HiveTableHandle) tableHandle.getConnectorHandle()).getSchemaTableName(),
                                ImmutableList.copyOf(hivePartitionResult.getPartitionColumns()),
                                metadata.getPartitionsAsList(hivePartitionResult),
                                domainPredicate,
                                decomposedFilter.getRemainingExpression(),
                                predicateColumns,
                                hivePartitionResult.getEnforcedConstraint(),
                                hivePartitionResult.getBucketHandle(),
                                hivePartitionResult.getBucketFilter())),
                TRUE_CONSTANT);
    }

    private static boolean isEntireColumn(Subfield subfield)
    {
        return subfield.getPath().isEmpty();
    }

    private static Subfield toSubfield(ColumnHandle columnHandle)
    {
        return new Subfield(((HiveColumnHandle) columnHandle).getName(), ImmutableList.of());
    }

    private static Set<VariableReferenceExpression> extractAll(RowExpression expression)
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

    private String getColumnName(ConnectorSession session, TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return hiveMetadataProvider.apply(tableHandle.getTransaction()).getColumnMetadata(session, tableHandle.getConnectorHandle(), columnHandle).getName();
    }

    private class Visitor
            extends PlanVisitor<PlanNode, Void>
    {
        private final ConnectorSession session;
        private final PlanNodeIdAllocator idAllocator;

        Visitor(ConnectorSession session, PlanNodeIdAllocator idAllocator)
        {
            this.session = requireNonNull(session, "session is null");
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
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) node.getSource();
                TableHandle handle = tableScanNode.getTable();

                if (!isPushdownFilterSupported(session, handle)) {
                    return node;
                }

                Map<String, String> symbolToColumnMapping = tableScanNode.getAssignments().entrySet().stream()
                        .collect(toImmutableMap(
                                entry -> entry.getKey().getName(),
                                entry -> getColumnName(session, tableScanNode.getTable(), entry.getValue())));

                PushdownFilterResult result = pushdownFilter(session, handle, node.getPredicate(), symbolToColumnMapping);

                if (result.getLayout().getPredicate().isNone()) {
                    return new ValuesNode(idAllocator.getNextId(), node.getOutputVariables(), ImmutableList.of());
                }

                TableScanNode tableScan = new TableScanNode(
                        node.getId(),
                        handle,
                        tableScanNode.getOutputVariables(),
                        tableScanNode.getAssignments(),
                        result.getLayout().getPredicate(),
                        TupleDomain.all());

                RowExpression unenforcedFilter = result.getUnenforcedConstraint();
                if (!TRUE_CONSTANT.equals(unenforcedFilter)) {
                    return new FilterNode(idAllocator.getNextId(), tableScan, unenforcedFilter);
                }

                return tableScan;
            }
            return node;
        }
    }

    private static class PushdownFilterResult
    {
        private final ConnectorTableLayout layout;
        private final RowExpression unenforcedConstraint;

        public PushdownFilterResult(ConnectorTableLayout layout, RowExpression unenforcedConstraint)
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
}
