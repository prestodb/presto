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
package com.facebook.presto.sql;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.metadata.PartitioningMetadata;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningHandle;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.TableStatisticsMetadata;
import com.facebook.presto.sql.planner.BasePlanFragmenter;
import com.facebook.presto.sql.planner.StatisticsAggregationPlanner;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.StatisticAggregations;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableWriterMergeNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.getTaskPartitionedWriterCount;
import static com.facebook.presto.SystemSessionProperties.isTableWriterMergeOperatorEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.ensureSourceOrderingGatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.partitionedExchange;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.function.Function.identity;

// Planner Util for creating temporary tables
public class TemporaryTableUtil
{
    private TemporaryTableUtil()
    {
    }

    public static TableScanNode createTemporaryTableScan(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            Optional<SourceLocation> sourceLocation,
            TableHandle tableHandle,
            List<VariableReferenceExpression> outputVariables,
            Map<VariableReferenceExpression, ColumnMetadata> variableToColumnMap,
            Optional<PartitioningMetadata> expectedPartitioningMetadata)
    {
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        Map<VariableReferenceExpression, ColumnMetadata> outputColumns = outputVariables.stream()
                .collect(toImmutableMap(identity(), variableToColumnMap::get));
        Set<ColumnHandle> outputColumnHandles = outputColumns.values().stream()
                .map(ColumnMetadata::getName)
                .map(columnHandles::get)
                .collect(toImmutableSet());

        TableLayoutResult selectedLayout = metadata.getLayout(session, tableHandle, Constraint.alwaysTrue(), Optional.of(outputColumnHandles));
        verify(selectedLayout.getUnenforcedConstraint().equals(TupleDomain.all()), "temporary table layout shouldn't enforce any constraints");
        verify(!selectedLayout.getLayout().getColumns().isPresent(), "temporary table layout must provide all the columns");
        if (expectedPartitioningMetadata.isPresent()) {
            TableLayout.TablePartitioning expectedPartitioning = new TableLayout.TablePartitioning(
                    expectedPartitioningMetadata.get().getPartitioningHandle(),
                    expectedPartitioningMetadata.get().getPartitionColumns().stream()
                            .map(columnHandles::get)
                            .collect(toImmutableList()));
            verify(selectedLayout.getLayout().getTablePartitioning().equals(Optional.of(expectedPartitioning)), "invalid temporary table partitioning");
        }
        Map<VariableReferenceExpression, ColumnHandle> assignments = outputVariables.stream()
                .collect(toImmutableMap(identity(), variable -> columnHandles.get(outputColumns.get(variable).getName())));

        return new TableScanNode(
                sourceLocation,
                idAllocator.getNextId(),
                selectedLayout.getLayout().getNewTableHandle(),
                outputVariables,
                assignments,
                TupleDomain.all(),
                TupleDomain.all());
    }

    public static Map<VariableReferenceExpression, ColumnMetadata> assignTemporaryTableColumnNames(Collection<VariableReferenceExpression> outputVariables,
            Collection<VariableReferenceExpression> constantPartitioningVariables)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ColumnMetadata> result = ImmutableMap.builder();
        int column = 0;
        for (VariableReferenceExpression outputVariable : concat(outputVariables, constantPartitioningVariables)) {
            String columnName = format("_c%d_%s", column, outputVariable.getName());
            result.put(outputVariable, new ColumnMetadata(columnName, outputVariable.getType()));
            column++;
        }
        return result.build();
    }

    public static Map<VariableReferenceExpression, ColumnMetadata> assignTemporaryTableColumnNames(Collection<VariableReferenceExpression> outputVariables)
    {
        return assignTemporaryTableColumnNames(outputVariables, Collections.emptyList());
    }

    public static BasePlanFragmenter.PartitioningVariableAssignments assignPartitioningVariables(VariableAllocator variableAllocator,
            Partitioning partitioning)
    {
        ImmutableList.Builder<VariableReferenceExpression> variables = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> constants = ImmutableMap.builder();
        for (RowExpression argument : partitioning.getArguments()) {
            checkArgument(argument instanceof ConstantExpression || argument instanceof VariableReferenceExpression,
                    format("Expect argument to be ConstantExpression or VariableReferenceExpression, got %s (%s)", argument.getClass(), argument));
            VariableReferenceExpression variable;
            if (argument instanceof ConstantExpression) {
                variable = variableAllocator.newVariable(argument.getSourceLocation(), "constant_partition", argument.getType());
                constants.put(variable, argument);
            }
            else {
                variable = (VariableReferenceExpression) argument;
            }
            variables.add(variable);
        }
        return new BasePlanFragmenter.PartitioningVariableAssignments(variables.build(), constants.build());
    }

    public static TableFinishNode createTemporaryTableWriteWithoutExchanges(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            PlanNode source,
            TableHandle tableHandle,
            List<VariableReferenceExpression> outputs,
            Map<VariableReferenceExpression, ColumnMetadata> variableToColumnMap,
            VariableReferenceExpression outputVar)
    {
        SchemaTableName schemaTableName = metadata.getTableMetadata(session, tableHandle).getTable();
        TableWriterNode.InsertReference insertReference = new TableWriterNode.InsertReference(tableHandle, schemaTableName);
        List<String> outputColumnNames = outputs.stream()
                .map(variableToColumnMap::get)
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
        Set<VariableReferenceExpression> outputNotNullColumnVariables = outputs.stream()
                .filter(variable -> variableToColumnMap.get(variable) != null && !(variableToColumnMap.get(variable).isNullable()))
                .collect(Collectors.toSet());
        Map<String, VariableReferenceExpression> columnNameToVariable = variableToColumnMap.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getValue().getName(), Map.Entry::getKey));
        return new TableFinishNode(
                source.getSourceLocation(),
                idAllocator.getNextId(),
                new TableWriterNode(
                        source.getSourceLocation(),
                        idAllocator.getNextId(),
                        source,
                        Optional.of(insertReference),
                        variableAllocator.newVariable("rows", BIGINT),
                        variableAllocator.newVariable("fragments", VARBINARY),
                        variableAllocator.newVariable("commitcontext", VARBINARY),
                        outputs,
                        outputColumnNames,
                        outputNotNullColumnVariables,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(Boolean.TRUE)),
                Optional.of(insertReference),
                outputVar,
                Optional.empty(),
                Optional.empty());
    }

    public static TableFinishNode createTemporaryTableWriteWithExchanges(
            Metadata metadata,
            Session session,
            PlanNodeIdAllocator idAllocator,
            VariableAllocator variableAllocator,
            StatisticsAggregationPlanner statisticsAggregationPlanner,
            Optional<SourceLocation> sourceLocation,
            TableHandle tableHandle,
            Map<VariableReferenceExpression, ColumnMetadata> variableToColumnMap,
            List<VariableReferenceExpression> outputs,
            List<List<VariableReferenceExpression>> inputs,
            List<PlanNode> sources,
            Map<VariableReferenceExpression, RowExpression> constantExpressions,
            PartitioningMetadata partitioningMetadata)
    {
        if (!constantExpressions.isEmpty()) {
            List<VariableReferenceExpression> constantVariables = ImmutableList.copyOf(constantExpressions.keySet());
            outputs = ImmutableList.<VariableReferenceExpression>builder()
                    .addAll(outputs)
                    .addAll(constantVariables)
                    .build();
            inputs = inputs.stream()
                    .map(input -> ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(input)
                            .addAll(constantVariables)
                            .build())
                    .collect(toImmutableList());

            // update sources
            sources = sources.stream()
                    .map(source -> {
                        Assignments.Builder assignments = Assignments.builder();
                        source.getOutputVariables().forEach(variable -> assignments.put(variable, new VariableReferenceExpression(variable.getSourceLocation(), variable.getName(), variable.getType())));
                        constantVariables.forEach(variable -> assignments.put(variable, constantExpressions.get(variable)));
                        return new ProjectNode(source.getSourceLocation(), idAllocator.getNextId(), source, assignments.build(), ProjectNode.Locality.LOCAL);
                    })
                    .collect(toImmutableList());
        }

        NewTableLayout insertLayout = metadata.getInsertLayout(session, tableHandle)
                // TODO: support insert into non partitioned table
                .orElseThrow(() -> new IllegalArgumentException("insertLayout for the temporary table must be present"));

        PartitioningHandle partitioningHandle = partitioningMetadata.getPartitioningHandle();
        List<String> partitionColumns = partitioningMetadata.getPartitionColumns();
        ConnectorNewTableLayout expectedNewTableLayout = new ConnectorNewTableLayout(partitioningHandle.getConnectorHandle(), partitionColumns);
        verify(insertLayout.getLayout().equals(expectedNewTableLayout), "unexpected new table layout");

        Map<String, VariableReferenceExpression> columnNameToVariable = variableToColumnMap.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getValue().getName(), Map.Entry::getKey));
        List<VariableReferenceExpression> partitioningVariables = partitionColumns.stream()
                .map(columnNameToVariable::get)
                .collect(toImmutableList());

        List<String> outputColumnNames = outputs.stream()
                .map(variableToColumnMap::get)
                .map(ColumnMetadata::getName)
                .collect(toImmutableList());
        Set<VariableReferenceExpression> outputNotNullColumnVariables = outputs.stream()
                .filter(variable -> variableToColumnMap.get(variable) != null && !(variableToColumnMap.get(variable).isNullable()))
                .collect(Collectors.toSet());

        SchemaTableName schemaTableName = metadata.getTableMetadata(session, tableHandle).getTable();
        TableWriterNode.InsertReference insertReference = new TableWriterNode.InsertReference(tableHandle, schemaTableName);

        PartitioningScheme partitioningScheme = new PartitioningScheme(
                Partitioning.create(partitioningHandle, partitioningVariables),
                outputs,
                Optional.empty(),
                false,
                Optional.empty());

        ExchangeNode writerRemoteSource = new ExchangeNode(
                sourceLocation,
                idAllocator.getNextId(),
                REPARTITION,
                REMOTE_STREAMING,
                partitioningScheme,
                sources,
                inputs,
                false,
                Optional.empty());

        ExchangeNode writerSource;
        if (getTaskPartitionedWriterCount(session) == 1) {
            writerSource = gatheringExchange(
                    idAllocator.getNextId(),
                    LOCAL,
                    writerRemoteSource);
        }
        else {
            writerSource = partitionedExchange(
                    idAllocator.getNextId(),
                    LOCAL,
                    writerRemoteSource,
                    partitioningScheme);
        }

        String catalogName = tableHandle.getConnectorId().getCatalogName();
        TableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
        TableStatisticsMetadata statisticsMetadata = metadata.getStatisticsCollectionMetadataForWrite(session, catalogName, tableMetadata.getMetadata());
        StatisticsAggregationPlanner.TableStatisticAggregation statisticsResult = statisticsAggregationPlanner.createStatisticsAggregation(statisticsMetadata, columnNameToVariable);
        StatisticAggregations.Parts aggregations = statisticsResult.getAggregations().splitIntoPartialAndFinal(variableAllocator, metadata.getFunctionAndTypeManager());
        PlanNode tableWriterMerge;

        // Disabled by default. Enable when the column statistics are essential for future runtime adaptive plan optimizations
        boolean enableStatsCollectionForTemporaryTable = SystemSessionProperties.isEnableStatsCollectionForTemporaryTable(session);

        if (isTableWriterMergeOperatorEnabled(session)) {
            StatisticAggregations.Parts localAggregations = aggregations.getPartialAggregation().splitIntoPartialAndIntermediate(variableAllocator, metadata.getFunctionAndTypeManager());
            tableWriterMerge = new TableWriterMergeNode(
                    sourceLocation,
                    idAllocator.getNextId(),
                    gatheringExchange(
                            idAllocator.getNextId(),
                            LOCAL,
                            new TableWriterNode(
                                    sourceLocation,
                                    idAllocator.getNextId(),
                                    writerSource,
                                    Optional.of(insertReference),
                                    variableAllocator.newVariable("partialrows", BIGINT),
                                    variableAllocator.newVariable("partialfragments", VARBINARY),
                                    variableAllocator.newVariable("partialtablecommitcontext", VARBINARY),
                                    outputs,
                                    outputColumnNames,
                                    outputNotNullColumnVariables,
                                    Optional.of(partitioningScheme),
                                    Optional.empty(),
                                    enableStatsCollectionForTemporaryTable ? Optional.of(localAggregations.getPartialAggregation()) : Optional.empty(),
                                    Optional.empty(),
                                    Optional.of(Boolean.TRUE))),
                    variableAllocator.newVariable("intermediaterows", BIGINT),
                    variableAllocator.newVariable("intermediatefragments", VARBINARY),
                    variableAllocator.newVariable("intermediatetablecommitcontext", VARBINARY),
                    enableStatsCollectionForTemporaryTable ? Optional.of(localAggregations.getIntermediateAggregation()) : Optional.empty());
        }
        else {
            tableWriterMerge = new TableWriterNode(
                    sourceLocation,
                    idAllocator.getNextId(),
                    writerSource,
                    Optional.of(insertReference),
                    variableAllocator.newVariable("partialrows", BIGINT),
                    variableAllocator.newVariable("partialfragments", VARBINARY),
                    variableAllocator.newVariable("partialtablecommitcontext", VARBINARY),
                    outputs,
                    outputColumnNames,
                    outputNotNullColumnVariables,
                    Optional.of(partitioningScheme),
                    Optional.empty(),
                    enableStatsCollectionForTemporaryTable ? Optional.of(aggregations.getPartialAggregation()) : Optional.empty(),
                    Optional.empty(),
                    Optional.of(Boolean.TRUE));
        }

        return new TableFinishNode(
                sourceLocation,
                idAllocator.getNextId(),
                ensureSourceOrderingGatheringExchange(
                        idAllocator.getNextId(),
                        REMOTE_STREAMING,
                        tableWriterMerge),
                Optional.of(insertReference),
                variableAllocator.newVariable("rows", BIGINT),
                enableStatsCollectionForTemporaryTable ? Optional.of(aggregations.getFinalAggregation()) : Optional.empty(),
                enableStatsCollectionForTemporaryTable ? Optional.of(statisticsResult.getDescriptor()) : Optional.empty());
    }
}
