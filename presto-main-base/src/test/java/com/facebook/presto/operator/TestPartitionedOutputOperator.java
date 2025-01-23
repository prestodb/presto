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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.operator.repartition.PartitionedOutputOperator;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestPartitionedOutputOperator
{
    private static final int PAGE_COUNT = 100;
    private static final int POSITIONS_PER_PAGE = 1000;

    private static final int PARTITION_COUNT = 512;
    private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
    private static final DataSize PARTITION_MAX_MEMORY = new DataSize(5, MEGABYTE);
    private static final List<Type> TYPES = ImmutableList.of(BIGINT);
    private static final List<Type> REPLICATION_TYPES = ImmutableList.of(BIGINT, BIGINT);
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));

    private static final Block NULL_BLOCK = new RunLengthEncodedBlock(BIGINT.createBlockBuilder(null, 1).appendNull().build(), POSITIONS_PER_PAGE);
    private static final Block TESTING_BLOCK = createLongSequenceBlock(0, POSITIONS_PER_PAGE);
    private static final Block TESTING_DICTIONARY_BLOCK = createLongDictionaryBlock(0, POSITIONS_PER_PAGE);
    private static final Block TESTING_RLE_BLOCK = createRLEBlock(new Random(0).nextLong(), POSITIONS_PER_PAGE);
    private static final Page TESTING_PAGE = new Page(TESTING_BLOCK);
    private static final Page TESTING_PAGE_WITH_NULL_BLOCK = new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_BLOCK);

    @Test
    public void testOutputForSimplePage()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(TESTING_PAGE);
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForPageWithDictionary()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(TESTING_DICTIONARY_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLength()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(false);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(TESTING_RLE_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputForSimplePageAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithDictionaryAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_DICTIONARY_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLengthAndReplication()
    {
        PartitionedOutputOperator partitionedOutputOperator = createPartitionedOutputOperator(true);
        for (int i = 0; i < PAGE_COUNT; i++) {
            partitionedOutputOperator.addInput(new Page(POSITIONS_PER_PAGE, NULL_BLOCK, TESTING_RLE_BLOCK));
        }
        partitionedOutputOperator.finish();

        OperatorContext operatorContext = partitionedOutputOperator.getOperatorContext();
        assertEquals(operatorContext.getOutputDataSize().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getSizeInBytes());
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), PAGE_COUNT * PARTITION_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    private static PartitionedOutputOperator createPartitionedOutputOperator(boolean shouldReplicate)
    {
        PartitionFunction partitionFunction = new LocalPartitionGenerator(new InterpretedHashGenerator(ImmutableList.of(BIGINT), new int[] {0}), PARTITION_COUNT);
        OutputPartitioning outputPartitioning;
        if (shouldReplicate) {
            outputPartitioning = new OutputPartitioning(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty()),
                    true,
                    OptionalInt.of(0));
        }
        else {
            outputPartitioning = new OutputPartitioning(
                    partitionFunction,
                    ImmutableList.of(0),
                    ImmutableList.of(Optional.empty(), Optional.empty()),
                    false,
                    OptionalInt.empty());
        }
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(), false);

        DriverContext driverContext = TestingTaskContext.builder(EXECUTOR, SCHEDULER, TEST_SESSION)
                .setMemoryPoolSize(MAX_MEMORY)
                .setQueryMaxTotalMemory(MAX_MEMORY)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
        }
        PartitionedOutputBuffer buffer = new PartitionedOutputBuffer(
                "task-instance-id",
                new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                buffers.withNoMoreBufferIds(),
                new DataSize(Long.MAX_VALUE, BYTE),
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                SCHEDULER);
        buffer.registerLifespanCompletionCallback(ignore -> {});

        PartitionedOutputOperator.PartitionedOutputFactory operatorFactory;
        if (shouldReplicate) {
            operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(buffer, PARTITION_MAX_MEMORY);
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), REPLICATION_TYPES, Function.identity(), Optional.of(outputPartitioning), serdeFactory)
                    .createOperator(driverContext);
        }
        else {
            operatorFactory = new PartitionedOutputOperator.PartitionedOutputFactory(

                    buffer,
                    PARTITION_MAX_MEMORY);
            return (PartitionedOutputOperator) operatorFactory
                    .createOutputOperator(0, new PlanNodeId("plan-node-0"), TYPES, Function.identity(), Optional.of(outputPartitioning), serdeFactory)
                    .createOperator(driverContext);
        }
    }
}
