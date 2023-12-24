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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.PartitioningMetadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.CTEInformation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.BasePlanFragmenter;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getCteMaterializationStrategy;
import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.SystemSessionProperties.getPartitioningProviderCatalog;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.TemporaryTableUtil.assignPartitioningVariables;
import static com.facebook.presto.sql.TemporaryTableUtil.assignTemporaryTableColumnNames;
import static com.facebook.presto.sql.TemporaryTableUtil.createTemporaryTableScan;
import static com.facebook.presto.sql.TemporaryTableUtil.createTemporaryTableWriteWithoutExchanges;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.CteMaterializationStrategy.ALL;
import static com.facebook.presto.sql.planner.optimizations.CteUtils.getCtePartitionIndex;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/*
 * PhysicalCteOptimizer Transformation:
 * This optimizer modifies the logical plan by transforming CTE producers into table writes
 * and CTE consumers into table scans.
 *
 * Example:
 * Before Transformation:
 *   CTEProducer(cteX)
 *   |-- SomeOperation
 *   `-- CTEConsumer(cteX)
 *
 * After Transformation:
 *   TableWrite(cteX)
 *   |-- SomeOperation
 *   `-- TableScan(cteX) *
 */
public class PhysicalCteOptimizer
        implements PlanOptimizer
{
    private final Metadata metadata;

    public PhysicalCteOptimizer(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanOptimizerResult optimize(PlanNode plan, Session session, TypeProvider types, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(warningCollector, "warningCollector is null");
        if (!getCteMaterializationStrategy(session).equals(ALL)
                || session.getCteInformationCollector().getCTEInformationList().stream().noneMatch(CTEInformation::isMaterialized)) {
            return PlanOptimizerResult.optimizerResult(plan, false);
        }
        PhysicalCteTransformerContext context = new PhysicalCteTransformerContext();
        CteProducerRewriter cteProducerRewriter = new CteProducerRewriter(session, idAllocator, variableAllocator);
        CteConsumerRewriter cteConsumerRewriter = new CteConsumerRewriter(session, idAllocator, variableAllocator);
        PlanNode producerReplaced = SimplePlanRewriter.rewriteWith(cteProducerRewriter, plan, context);
        PlanNode rewrittenPlan = SimplePlanRewriter.rewriteWith(cteConsumerRewriter, producerReplaced, context);
        return PlanOptimizerResult.optimizerResult(rewrittenPlan,
                cteConsumerRewriter.isPlanRewritten() || cteProducerRewriter.isPlanRewritten());
    }

    public class CteProducerRewriter
            extends SimplePlanRewriter<PhysicalCteTransformerContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        private final Session session;

        private boolean isPlanRewritten;

        public CteProducerRewriter(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
            this.session = requireNonNull(session, "session must not be null");
        }

        @Override
        public PlanNode visitCteProducer(CteProducerNode node, RewriteContext<PhysicalCteTransformerContext> context)
        {
            isPlanRewritten = true;
            // Create Table Metadata
            PlanNode actualSource = node.getSource();
            VariableReferenceExpression partitionVariable = actualSource.getOutputVariables()
                    .get(getCtePartitionIndex(actualSource.getOutputVariables()));
            List<Type> partitioningTypes = Arrays.asList(partitionVariable.getType());
            String partitioningProviderCatalog = getPartitioningProviderCatalog(session);
            // First column is taken as the partitioning column
            Partitioning partitioning = Partitioning.create(
                    metadata.getPartitioningHandleForExchange(session, partitioningProviderCatalog,
                            getHashPartitionCount(session), partitioningTypes),
                    Arrays.asList(partitionVariable));
            BasePlanFragmenter.PartitioningVariableAssignments partitioningVariableAssignments
                    = assignPartitioningVariables(variableAllocator, partitioning);
            Map<VariableReferenceExpression, ColumnMetadata> variableToColumnMap =
                    assignTemporaryTableColumnNames(actualSource.getOutputVariables(),
                            partitioningVariableAssignments.getConstants().keySet());
            List<VariableReferenceExpression> partitioningVariables = partitioningVariableAssignments.getVariables();
            List<String> partitionColumns = partitioningVariables.stream()
                    .map(variable -> variableToColumnMap.get(variable).getName())
                    .collect(toImmutableList());
            PartitioningMetadata partitioningMetadata = new PartitioningMetadata(partitioning.getHandle(), partitionColumns);

            TableHandle temporaryTableHandle;
            try {
                temporaryTableHandle = metadata.createTemporaryTable(
                        session,
                        partitioningProviderCatalog,
                        ImmutableList.copyOf(variableToColumnMap.values()),
                        Optional.of(partitioningMetadata));
                context.get().put(node.getCteName(),
                        new PhysicalCteTransformerContext.TemporaryTableInfo(
                                createTemporaryTableScan(
                                        metadata,
                                        session,
                                        idAllocator,
                                        node.getSourceLocation(),
                                        temporaryTableHandle,
                                        actualSource.getOutputVariables(),
                                        variableToColumnMap,
                                        partitioningMetadata), node.getOutputVariables()));
            }
            catch (PrestoException e) {
                if (e.getErrorCode().equals(NOT_SUPPORTED.toErrorCode())) {
                    throw new PrestoException(
                            NOT_SUPPORTED,
                            format("Temporary table cannot be created in catalog \"%s\": %s", partitioningProviderCatalog, e.getMessage()),
                            e);
                }
                throw e;
            }
            // Create the writer
            return createTemporaryTableWriteWithoutExchanges(
                    metadata,
                    session,
                    idAllocator,
                    variableAllocator,
                    actualSource,
                    temporaryTableHandle,
                    actualSource.getOutputVariables(),
                    variableToColumnMap,
                    partitioningMetadata,
                    node.getRowCountVariable());
        }

        public boolean isPlanRewritten()
        {
            return isPlanRewritten;
        }
    }

    public class CteConsumerRewriter
            extends SimplePlanRewriter<PhysicalCteTransformerContext>
    {
        private final PlanNodeIdAllocator idAllocator;

        private final VariableAllocator variableAllocator;

        private final Session session;

        private boolean isPlanRewritten;

        public CteConsumerRewriter(Session session, PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator must not be null");
            this.variableAllocator = requireNonNull(variableAllocator, "variableAllocator must not be null");
            this.session = requireNonNull(session, "session must not be null");
        }

        @Override
        public PlanNode visitCteConsumer(CteConsumerNode node, RewriteContext<PhysicalCteTransformerContext> context)
        {
            isPlanRewritten = true;
            // Create Table Metadata
            PhysicalCteTransformerContext.TemporaryTableInfo tableInfo = context.get().getTableInfo(node.getCteName());
            TableScanNode tempScan = tableInfo.getTableScanNode();

            // Need to create new Variables for temp table scans to avoid duplicate references
            List<VariableReferenceExpression> newOutputVariables = new ArrayList<>();
            Map<VariableReferenceExpression, ColumnHandle> newColumnAssignmentsMap = new HashMap<>();
            for (VariableReferenceExpression oldVariable : tempScan.getOutputVariables()) {
                VariableReferenceExpression newVariable = variableAllocator.newVariable(oldVariable);
                newOutputVariables.add(newVariable);
                newColumnAssignmentsMap.put(newVariable, tempScan.getAssignments().get(oldVariable));
            }

            TableScanNode tableScanNode = new TableScanNode(
                    Optional.empty(),
                    idAllocator.getNextId(),
                    tempScan.getTable(),
                    newOutputVariables,
                    newColumnAssignmentsMap,
                    tempScan.getCurrentConstraint(),
                    tempScan.getEnforcedConstraint());

            // The temporary table scan might have columns removed by the UnaliasSymbolReferences and other optimizers (its a plan tree after all),
            // use originalOutputVariables (which are also canonicalized and maintained) and add them back
            Map<VariableReferenceExpression, VariableReferenceExpression> intermediateReferenceMap = new HashMap<>();
            for (int i = 0; i < tempScan.getOutputVariables().size(); i++) {
                intermediateReferenceMap.put(tempScan.getOutputVariables().get(i), newOutputVariables.get(i));
            }

            Assignments.Builder assignments = Assignments.builder();
            for (int i = 0; i < tableInfo.getOriginalOutputVariables().size(); i++) {
                assignments.put(node.getOutputVariables().get(i), intermediateReferenceMap.get(tableInfo.getOriginalOutputVariables().get(i)));
            }
            return new ProjectNode(Optional.empty(), idAllocator.getNextId(), Optional.empty(),
                    tableScanNode, assignments.build(), ProjectNode.Locality.LOCAL);
        }

        public boolean isPlanRewritten()
        {
            return isPlanRewritten;
        }
    }

    public static class PhysicalCteTransformerContext
    {
        private Map<String, TemporaryTableInfo> cteNameToTableInfo = new HashMap<>();

        public PhysicalCteTransformerContext()
        {
            cteNameToTableInfo = new HashMap<>();
        }

        public void put(String cteName, TemporaryTableInfo handle)
        {
            cteNameToTableInfo.put(cteName, handle);
        }

        public TemporaryTableInfo getTableInfo(String cteName)
        {
            return cteNameToTableInfo.get(cteName);
        }

        public static class TemporaryTableInfo
        {
            private final TableScanNode tableScanNode;
            private final List<VariableReferenceExpression> originalOutputVariables;

            public TemporaryTableInfo(TableScanNode tableScanNode, List<VariableReferenceExpression> originalOutputVariables)
            {
                this.tableScanNode = requireNonNull(tableScanNode, "tableScanNode must not be null");
                this.originalOutputVariables = requireNonNull(originalOutputVariables, "originalOutputVariables must not be null");
            }

            public List<VariableReferenceExpression> getOriginalOutputVariables()
            {
                return originalOutputVariables;
            }

            public TableScanNode getTableScanNode()
            {
                return tableScanNode;
            }
        }
    }
}
