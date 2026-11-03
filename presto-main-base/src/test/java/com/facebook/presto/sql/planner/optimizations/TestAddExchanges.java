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
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConstantProperty;
import com.facebook.presto.spi.GroupingProperty;
import com.facebook.presto.spi.SortingProperty;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PartitioningProviderManager;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.optimizations.ActualProperties.Global;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.AGGREGATION_PARTITIONING_MERGING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PRECISION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.LEGACY;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.AggregationPartitioningMergingStrategy.TOP_DOWN;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartitioningPrecisionStrategy.AUTOMATIC;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static com.facebook.presto.sql.planner.optimizations.ActualProperties.builder;
import static com.facebook.presto.sql.planner.optimizations.AddExchanges.streamingExecutionPreference;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * These are unit test for the internal logic in AddExchanges.
 * For plan tests see {@link TestAddExchangesPlans}
 */
public class TestAddExchanges
{
    @Test
    public void testTopDownAggregationAdoptsTableWriterPartitioning()
    {
        TableWriterNode optimized = optimizeTableWriterOverAggregation(TOP_DOWN.name());
        Partitioning writerPartitioning = optimized.getTablePartitioningScheme().get().getPartitioning();

        PlanNode writerSource = optimized.getSource();
        assertTrue(writerSource instanceof AggregationNode);
        PlanNode aggregationSource = ((AggregationNode) writerSource).getSource();
        assertTrue(aggregationSource instanceof ExchangeNode);
        assertEquals(((ExchangeNode) aggregationSource).getPartitioningScheme().getPartitioning(), writerPartitioning);
    }

    @Test
    public void testLegacyAggregationDoesNotAdoptTableWriterPartitioning()
    {
        TableWriterNode optimized = optimizeTableWriterOverAggregation(LEGACY.name());

        assertTrue(optimized.getSource() instanceof ExchangeNode);
        ExchangeNode writerExchange = (ExchangeNode) optimized.getSource();
        assertEquals(writerExchange.getPartitioningScheme().getPartitioning().getArguments().size(), 1);
        assertTrue(writerExchange.getSources().get(0) instanceof AggregationNode);
        PlanNode aggregationSource = ((AggregationNode) writerExchange.getSources().get(0)).getSource();
        assertTrue(aggregationSource instanceof ExchangeNode);
        assertEquals(((ExchangeNode) aggregationSource).getPartitioningScheme().getPartitioning().getArguments().size(), 2);
    }

    private static TableWriterNode optimizeTableWriterOverAggregation(String partitioningMergingStrategy)
    {
        Metadata metadata = MetadataManager.createTestMetadataManager();
        Session session = Session.builder(testSessionBuilder().build())
                .setSystemProperty(AGGREGATION_PARTITIONING_MERGING_STRATEGY, partitioningMergingStrategy)
                .setSystemProperty(PARTITIONING_PRECISION_STRATEGY, AUTOMATIC.name())
                .setSystemProperty(SPARK_ASSIGN_BUCKET_TO_PARTITION_FOR_PARTITIONED_TABLE_WRITE_ENABLED, "false")
                .build();
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder planBuilder = new PlanBuilder(session, idAllocator, metadata);
        VariableReferenceExpression shard = planBuilder.variable("shard", BIGINT);
        VariableReferenceExpression document = planBuilder.variable("document", BIGINT);

        PlanNode distributedSource = ExchangeNode.roundRobinExchange(
                idAllocator.getNextId(),
                REMOTE_STREAMING,
                planBuilder.values(shard, document));
        AggregationNode aggregation = planBuilder.aggregation(builder -> builder
                .singleGroupingSet(shard, document)
                .source(distributedSource));
        Partitioning writerPartitioning = Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of(shard));
        TableWriterNode tableWriter = new TableWriterNode(
                Optional.empty(),
                idAllocator.getNextId(),
                aggregation,
                Optional.empty(),
                planBuilder.variable("partialrows", BIGINT),
                planBuilder.variable("fragment", VARBINARY),
                planBuilder.variable("tablecommitcontext", VARBINARY),
                ImmutableList.of(shard, document),
                ImmutableList.of("shard", "document"),
                ImmutableSet.of(),
                Optional.of(new PartitioningScheme(writerPartitioning, aggregation.getOutputVariables())),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanNode optimized = new AddExchanges(metadata, new PartitioningProviderManager(), false)
                .optimize(
                        tableWriter,
                        session,
                        planBuilder.getTypes(),
                        new VariableAllocator(planBuilder.getTypes().allVariables()),
                        idAllocator,
                        WarningCollector.NOOP)
                .getPlanNode();

        assertTrue(optimized instanceof TableWriterNode);
        return (TableWriterNode) optimized;
    }

    @Test
    public void testPickLayoutAnyPreference()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.any());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a", "b"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        // Given no preferences, the original input order should be maintained
        assertEquals(stableSort(input, preference), input);
    }

    @Test
    public void testPickLayoutPartitionedPreference()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.distributed());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutUnpartitionedPreference()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(PreferredProperties.undistributed());

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedOnSingle()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(
                PreferredProperties.partitioned(ImmutableSet.of(variable("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedOnMultiple()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference(
                PreferredProperties.partitioned(ImmutableSet.of(variable("a"), variable("b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGrouped()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedMultiple()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a", "b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedMultipleProperties()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"), grouped("b"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutGroupedWithSort()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.local(ImmutableList.of(grouped("a"), sorted("b", ASC_NULLS_FIRST))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutUnpartitionedWithGroupAndSort()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.undistributedWithLocal(ImmutableList.of(grouped("a"), sorted("b", ASC_NULLS_FIRST))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    @Test
    public void testPickLayoutPartitionedWithGroup()
    {
        Comparator<ActualProperties> preference = streamingExecutionPreference
                (PreferredProperties.partitionedWithLocal(
                        ImmutableSet.of(variable("a")),
                        ImmutableList.of(grouped("a"))));

        List<ActualProperties> input = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .build();

        List<ActualProperties> expected = ImmutableList.<ActualProperties>builder()
                .add(builder()
                        .global(singleStream())
                        .local(ImmutableList.of(constant("a"), sorted("b", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .local(ImmutableList.of(sorted("a", ASC_NULLS_FIRST)))
                        .build())
                .add(builder()
                        .global(streamPartitionedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(hashDistributedOn("a"))
                        .build())
                .add(builder()
                        .global(singleStreamPartition())
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .local(ImmutableList.of(grouped("a", "b")))
                        .build())
                .add(builder()
                        .global(arbitraryPartition())
                        .build())
                .build();
        assertEquals(stableSort(input, preference), expected);
    }

    private static <T> List<T> stableSort(List<T> list, Comparator<T> comparator)
    {
        ArrayList<T> copy = Lists.newArrayList(list);
        Collections.sort(copy, comparator);
        return copy;
    }

    private static Global hashDistributedOn(String... columnNames)
    {
        return partitionedOn(FIXED_HASH_DISTRIBUTION, arguments(columnNames), Optional.of(arguments(columnNames)));
    }

    public static Global singleStream()
    {
        return Global.streamPartitionedOn(ImmutableList.of());
    }

    private static Global streamPartitionedOn(String... columnNames)
    {
        return Global.streamPartitionedOn(arguments(columnNames));
    }

    private static ConstantProperty<VariableReferenceExpression> constant(String column)
    {
        return new ConstantProperty<>(variable(column));
    }

    private static GroupingProperty<VariableReferenceExpression> grouped(String... columns)
    {
        return new GroupingProperty<>(Lists.transform(Arrays.asList(columns), column -> new VariableReferenceExpression(Optional.empty(), column, BIGINT)));
    }

    private static SortingProperty<VariableReferenceExpression> sorted(String column, SortOrder order)
    {
        return new SortingProperty<>(variable(column), order);
    }

    private static VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(Optional.empty(), name, BIGINT);
    }

    private static List<VariableReferenceExpression> arguments(String[] columnNames)
    {
        return Arrays.asList(columnNames).stream()
                .map(column -> new VariableReferenceExpression(Optional.empty(), column, BIGINT))
                .collect(toImmutableList());
    }
}
