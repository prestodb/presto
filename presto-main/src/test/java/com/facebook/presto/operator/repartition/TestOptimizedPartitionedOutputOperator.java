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
package com.facebook.presto.operator.repartition;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingManager;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.PartitionedOutputBuffer;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.PageAssertions;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.PrecomputedHashGenerator;
import com.facebook.presto.operator.exchange.LocalPartitionGenerator;
import com.facebook.presto.operator.repartition.OptimizedPartitionedOutputOperator.OptimizedPartitionedOutputFactory;
import com.facebook.presto.spi.page.PagesSerde;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.OutputPartitioning;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertBetweenInclusive;
import static com.facebook.presto.block.BlockAssertions.Encoding.DICTIONARY;
import static com.facebook.presto.block.BlockAssertions.Encoding.RUN_LENGTH;
import static com.facebook.presto.block.BlockAssertions.createLongDictionaryBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createMapType;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRandomStringBlock;
import static com.facebook.presto.block.BlockAssertions.wrapBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.RowType.withDefaultFieldNames;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.buffer.BufferState.OPEN;
import static com.facebook.presto.execution.buffer.BufferState.TERMINAL_BUFFER_STATES;
import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.PageAssertions.mergePages;
import static com.facebook.presto.operator.PageAssertions.updateBlockTypesWithHashBlockAndNullBlock;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestOptimizedPartitionedOutputOperator
{
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-EXECUTOR-%s"));
    private static final ScheduledExecutorService SCHEDULER = newScheduledThreadPool(1, daemonThreadsNamed("test-%s"));
    private static final DataSize MAX_MEMORY = new DataSize(1, GIGABYTE);
    private static final PagesSerde PAGES_SERDE = new PagesSerdeFactory(new BlockEncodingManager(), false).createPagesSerde(); //testingPagesSerde();

    private static final int PARTITION_COUNT = 16;
    private static final int PAGE_COUNT = 50;
    private static final int POSITION_COUNT = 100;

    private static final Random RANDOM = new Random(0);

    private static final Block NULL_BLOCK = new RunLengthEncodedBlock(BIGINT.createBlockBuilder(null, 1).appendNull().build(), POSITION_COUNT);
    private static final Block TESTING_BLOCK = createLongSequenceBlock(0, POSITION_COUNT);
    private static final Block TESTING_DICTIONARY_BLOCK = createLongDictionaryBlock(0, POSITION_COUNT);
    private static final Block TESTING_RLE_BLOCK = createRLEBlock(new Random(0).nextLong(), POSITION_COUNT);
    private static final Page TESTING_PAGE = new Page(TESTING_BLOCK);
    private static final Page TESTING_PAGE_WITH_DICTIONARY_BLOCK = new Page(TESTING_DICTIONARY_BLOCK);
    private static final Page TESTING_PAGE_WITH_RLE_BLOCK = new Page(TESTING_RLE_BLOCK);
    private static final Page TESTING_PAGE_WITH_NULL_BLOCK = new Page(POSITION_COUNT, TESTING_BLOCK, NULL_BLOCK);
    private static final Page TESTING_PAGE_WITH_NULL_AND_DICTIONARY_BLOCK = new Page(POSITION_COUNT, TESTING_DICTIONARY_BLOCK, NULL_BLOCK);
    private static final Page TESTING_PAGE_WITH_NULL_AND_RLE_BLOCK = new Page(POSITION_COUNT, TESTING_RLE_BLOCK, NULL_BLOCK);

    private static final double OUTPUT_SIZE_ESTIMATION_ERROR_ALLOWANCE = 1.2;

    @Test
    public void testPartitionedSinglePagePrimitiveTypes()
    {
        testPartitionedSinglePage(ImmutableList.of(BIGINT));
        testPartitionedSinglePage(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testPartitionedSinglePage(ImmutableList.of(SMALLINT));
        testPartitionedSinglePage(ImmutableList.of(INTEGER));
        testPartitionedSinglePage(ImmutableList.of(REAL));
        testPartitionedSinglePage(ImmutableList.of(BOOLEAN));
        testPartitionedSinglePage(ImmutableList.of(VARCHAR));

        testPartitionedSinglePage(ImmutableList.of(BIGINT, BIGINT));
        testPartitionedSinglePage(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION - 1)));
        testPartitionedSinglePage(ImmutableList.of(SMALLINT, SMALLINT));
        testPartitionedSinglePage(ImmutableList.of(INTEGER, INTEGER));
        testPartitionedSinglePage(ImmutableList.of(REAL, REAL));
        testPartitionedSinglePage(ImmutableList.of(BOOLEAN, BOOLEAN));
        testPartitionedSinglePage(ImmutableList.of(VARCHAR, VARCHAR));
    }

    @Test
    public void testPartitionedSinglePageForArray()
    {
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType((createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)));

        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(BIGINT))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(SMALLINT))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(INTEGER))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(BOOLEAN))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(VARCHAR))));

        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BIGINT)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(SMALLINT)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(INTEGER)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BOOLEAN)))));
        testPartitionedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(VARCHAR)))));
    }

    @Test
    public void testPartitionedSinglePageForMap()
    {
        testPartitionedSinglePage(ImmutableList.of(createMapType(BIGINT, BIGINT)));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(SMALLINT, SMALLINT)));
        testPartitionedSinglePage(ImmutableList.of(createMapType(INTEGER, INTEGER)));
        testPartitionedSinglePage(ImmutableList.of(createMapType(BOOLEAN, BOOLEAN)));
        testPartitionedSinglePage(ImmutableList.of(createMapType(VARCHAR, VARCHAR)));

        testPartitionedSinglePage(ImmutableList.of(createMapType(BIGINT, createMapType(BIGINT, BIGINT))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(INTEGER, createMapType(INTEGER, INTEGER))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR))));

        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(BIGINT, BIGINT), new ArrayType(createMapType(BIGINT, BIGINT)))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(SMALLINT, SMALLINT), new ArrayType(createMapType(SMALLINT, SMALLINT)))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testPartitionedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
    }

    @Test
    public void testPartitionedSinglePageForRow()
    {
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER, INTEGER))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));

        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))))));

        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))));

        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, BIGINT)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, INTEGER)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, SMALLINT)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, BOOLEAN)))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, VARCHAR)))));

        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER)))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT)))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN)))))));
        testPartitionedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR)))))));
    }

    @Test
    public void testPartitionedMultiplePagesPrimitiveTypes()
    {
        testPartitionedMultiplePages(ImmutableList.of(BIGINT));
        testPartitionedMultiplePages(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testPartitionedMultiplePages(ImmutableList.of(SMALLINT));
        testPartitionedMultiplePages(ImmutableList.of(INTEGER));
        testPartitionedMultiplePages(ImmutableList.of(REAL));
        testPartitionedMultiplePages(ImmutableList.of(BOOLEAN));
        testPartitionedMultiplePages(ImmutableList.of(VARCHAR));

        testPartitionedMultiplePages(ImmutableList.of(BIGINT, BIGINT));
        testPartitionedMultiplePages(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION - 1)));
        testPartitionedMultiplePages(ImmutableList.of(SMALLINT, SMALLINT));
        testPartitionedMultiplePages(ImmutableList.of(INTEGER, INTEGER));
        testPartitionedMultiplePages(ImmutableList.of(REAL, REAL));
        testPartitionedMultiplePages(ImmutableList.of(BOOLEAN, BOOLEAN));
        testPartitionedMultiplePages(ImmutableList.of(VARCHAR, VARCHAR));
    }

    @Test
    public void testPartitionedMultiplePagesForArray()
    {
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType((createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)));

        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(BIGINT))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(SMALLINT))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(INTEGER))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(BOOLEAN))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(VARCHAR))));

        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BIGINT)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(SMALLINT)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(INTEGER)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BOOLEAN)))));
        testPartitionedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(VARCHAR)))));
    }

    @Test
    public void testPartitionedMultiplePagesForMap()
    {
        testPartitionedMultiplePages(ImmutableList.of(createMapType(BIGINT, BIGINT)));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(SMALLINT, SMALLINT)));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(INTEGER, INTEGER)));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(BOOLEAN, BOOLEAN)));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(VARCHAR, VARCHAR)));

        testPartitionedMultiplePages(ImmutableList.of(createMapType(BIGINT, createMapType(BIGINT, BIGINT))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(INTEGER, createMapType(INTEGER, INTEGER))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR))));

        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(BIGINT, BIGINT), new ArrayType(createMapType(BIGINT, BIGINT)))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(SMALLINT, SMALLINT), new ArrayType(createMapType(SMALLINT, SMALLINT)))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testPartitionedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
    }

    @Test
    public void testPartitionedMultiplePagesForRow()
    {
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER, INTEGER))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));

        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))))));

        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))));

        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, BIGINT)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, INTEGER)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, SMALLINT)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, BOOLEAN)))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, VARCHAR)))));

        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER)))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT)))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN)))))));
        testPartitionedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR)))))));
    }

    @Test
    public void testReplicatedSinglePagePrimitiveTypes()
    {
        testReplicatedSinglePage(ImmutableList.of(BIGINT));
        testReplicatedSinglePage(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testReplicatedSinglePage(ImmutableList.of(SMALLINT));
        testReplicatedSinglePage(ImmutableList.of(INTEGER));
        testReplicatedSinglePage(ImmutableList.of(BOOLEAN));
        testReplicatedSinglePage(ImmutableList.of(VARCHAR));

        testReplicatedSinglePage(ImmutableList.of(BIGINT, BIGINT));
        testReplicatedSinglePage(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION - 1)));
        testReplicatedSinglePage(ImmutableList.of(SMALLINT, SMALLINT));
        testReplicatedSinglePage(ImmutableList.of(INTEGER, INTEGER));
        testReplicatedSinglePage(ImmutableList.of(BOOLEAN, BOOLEAN));
        testReplicatedSinglePage(ImmutableList.of(VARCHAR, VARCHAR));
    }

    @Test
    public void testReplicatedSinglePageForArray()
    {
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType((createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)));

        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(BIGINT))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(SMALLINT))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(INTEGER))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(BOOLEAN))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(VARCHAR))));

        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BIGINT)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(SMALLINT)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(INTEGER)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BOOLEAN)))));
        testReplicatedSinglePage(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(VARCHAR)))));
    }

    @Test
    public void testReplicatedSinglePageForMap()
    {
        testReplicatedSinglePage(ImmutableList.of(createMapType(BIGINT, BIGINT)));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(SMALLINT, SMALLINT)));
        testReplicatedSinglePage(ImmutableList.of(createMapType(INTEGER, INTEGER)));
        testReplicatedSinglePage(ImmutableList.of(createMapType(BOOLEAN, BOOLEAN)));
        testReplicatedSinglePage(ImmutableList.of(createMapType(VARCHAR, VARCHAR)));

        testReplicatedSinglePage(ImmutableList.of(createMapType(BIGINT, createMapType(BIGINT, BIGINT))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(INTEGER, createMapType(INTEGER, INTEGER))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR))));

        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(BIGINT, BIGINT), new ArrayType(createMapType(BIGINT, BIGINT)))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(SMALLINT, SMALLINT), new ArrayType(createMapType(SMALLINT, SMALLINT)))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testReplicatedSinglePage(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
    }

    @Test
    public void testReplicatedSinglePageForRow()
    {
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER, INTEGER))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));

        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))))));

        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))));

        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, BIGINT)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, INTEGER)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, SMALLINT)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, BOOLEAN)))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, VARCHAR)))));

        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER)))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT)))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN)))))));
        testReplicatedSinglePage(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR)))))));
    }

    @Test
    public void testReplicatedMultiplePagesPrimitiveTypes()
    {
        testReplicatedMultiplePages(ImmutableList.of(BIGINT));
        testReplicatedMultiplePages(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1)));
        testReplicatedMultiplePages(ImmutableList.of(SMALLINT));
        testReplicatedMultiplePages(ImmutableList.of(INTEGER));
        testReplicatedMultiplePages(ImmutableList.of(BOOLEAN));
        testReplicatedMultiplePages(ImmutableList.of(VARCHAR));

        testReplicatedMultiplePages(ImmutableList.of(BIGINT, BIGINT));
        testReplicatedMultiplePages(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION - 1)));
        testReplicatedMultiplePages(ImmutableList.of(SMALLINT, SMALLINT));
        testReplicatedMultiplePages(ImmutableList.of(INTEGER, INTEGER));
        testReplicatedMultiplePages(ImmutableList.of(BOOLEAN, BOOLEAN));
        testReplicatedMultiplePages(ImmutableList.of(VARCHAR, VARCHAR));
    }

    @Test
    public void testReplicatedMultiplePagesForArray()
    {
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType((createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)));

        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(BIGINT))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(SMALLINT))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(INTEGER))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(BOOLEAN))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(VARCHAR))));

        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BIGINT)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(SMALLINT)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(INTEGER)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(BOOLEAN)))));
        testReplicatedMultiplePages(ImmutableList.of(new ArrayType(new ArrayType(new ArrayType(VARCHAR)))));
    }

    @Test
    public void testReplicatedMultiplePagesForMap()
    {
        testReplicatedMultiplePages(ImmutableList.of(createMapType(BIGINT, BIGINT)));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(SMALLINT, SMALLINT)));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(INTEGER, INTEGER)));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(BOOLEAN, BOOLEAN)));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(VARCHAR, VARCHAR)));

        testReplicatedMultiplePages(ImmutableList.of(createMapType(BIGINT, createMapType(BIGINT, BIGINT))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createDecimalType(MAX_SHORT_PRECISION + 1), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(SMALLINT, createMapType(SMALLINT, SMALLINT))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(INTEGER, createMapType(INTEGER, INTEGER))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(BOOLEAN, createMapType(BOOLEAN, BOOLEAN))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(VARCHAR, createMapType(VARCHAR, VARCHAR))));

        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(BIGINT, BIGINT), new ArrayType(createMapType(BIGINT, BIGINT)))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(SMALLINT, SMALLINT), new ArrayType(createMapType(SMALLINT, SMALLINT)))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
        testReplicatedMultiplePages(ImmutableList.of(createMapType(createMapType(INTEGER, INTEGER), new ArrayType(createMapType(INTEGER, INTEGER)))));
    }

    @Test
    public void testReplicatedMultiplePagesForRow()
    {
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT, BIGINT))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER, INTEGER))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT, SMALLINT))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))));

        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, BOOLEAN))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR, VARCHAR))))));

        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), new ArrayType(BIGINT)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), new ArrayType(INTEGER)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), new ArrayType(SMALLINT)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), new ArrayType(BOOLEAN)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), new ArrayType(VARCHAR)))));

        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, BIGINT)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, createDecimalType(MAX_SHORT_PRECISION + 1))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, INTEGER)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, SMALLINT)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, BOOLEAN)))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, VARCHAR)))));

        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BIGINT), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(BIGINT, BIGINT)))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(createDecimalType(MAX_SHORT_PRECISION + 1)), createMapType(BIGINT, withDefaultFieldNames(ImmutableList.of(createDecimalType(MAX_SHORT_PRECISION + 1), createDecimalType(MAX_SHORT_PRECISION + 1))))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(INTEGER), createMapType(INTEGER, withDefaultFieldNames(ImmutableList.of(INTEGER, INTEGER)))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(SMALLINT), createMapType(SMALLINT, withDefaultFieldNames(ImmutableList.of(SMALLINT, SMALLINT)))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(BOOLEAN), createMapType(BOOLEAN, withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN)))))));
        testReplicatedMultiplePages(ImmutableList.of(withDefaultFieldNames(ImmutableList.of(new ArrayType(VARCHAR), createMapType(VARCHAR, withDefaultFieldNames(ImmutableList.of(VARCHAR, VARCHAR)))))));
    }

    @Test
    public void testEmptyPage()
    {
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(ImmutableList.of(BIGINT), true, false);
        Page page = PageAssertions.createPageWithRandomData(ImmutableList.of(BIGINT), 0, true, false, 0.0f, 0.0f, false, ImmutableList.of());

        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
    }

    @Test
    public void testPageWithNoBlocks()
    {
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(ImmutableList.of(), false, false);
        Page page = PageAssertions.createPageWithRandomData(ImmutableList.of(), 1, false, false, 0.0f, 0.0f, false, ImmutableList.of());

        testPartitionedForZeroBlocks(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
    }

    /**
     * Test BlockFlattener's allocator does not return borrowed arrays among blocks
     */
    @Test
    public void testPageWithBlocksOfDifferentPositionCounts()
    {
        Block[] blocks = new Block[4];

        // PreComputed Hash Block
        blocks[0] = createRandomLongsBlock(POSITION_COUNT, 0.0f);

        // Create blocks whose base blocks are with increasing number of positions
        blocks[1] = wrapBlock(createRandomStringBlock(10, 0.2f, 10), POSITION_COUNT, ImmutableList.of(DICTIONARY, DICTIONARY));
        blocks[2] = wrapBlock(createRandomStringBlock(100, 0.2f, 10), POSITION_COUNT, ImmutableList.of(DICTIONARY, DICTIONARY));
        blocks[3] = wrapBlock(createRandomStringBlock(1000, 0.2f, 10), POSITION_COUNT, ImmutableList.of(DICTIONARY, DICTIONARY));

        Page page = new Page(blocks);

        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, VARCHAR, VARCHAR);
        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testPartitioned(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));
    }

    @Test
    public void testPageWithVariableWidthBlocksOfSliceViews()
    {
        Block[] blocks = new Block[2];

        // PreComputed Hash Block
        blocks[0] = createRandomLongsBlock(POSITION_COUNT, 0.0f);

        // Create blocks whose base blocks are with increasing number of positions
        blocks[1] = createVariableWidthBlockOverSliceView(POSITION_COUNT);

        Page page = new Page(blocks);

        List<Type> types = ImmutableList.of(BIGINT, VARCHAR);

        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testPartitioned(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));
    }

    private void testPartitionedSinglePage(List<Type> targetTypes)
    {
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(targetTypes, true, false);

        // Test plain blocks: no block views, no Dicrtionary/RLE blocks
        Page page = PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT, true, false, 0.2f, 0.2f, false, ImmutableList.of());

        // First test for the cases where the buffer can hold the whole page, then force flushing for every a few rows.
        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testPartitioned(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));

        // Test block views and Dicrtionary/RLE blocks
        page = PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT, true, false, 0.2f, 0.2f, true, ImmutableList.of(DICTIONARY, RUN_LENGTH, DICTIONARY, RUN_LENGTH));
        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testPartitioned(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));

        page = PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT, true, false, 0.2f, 0.2f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, RUN_LENGTH, DICTIONARY));
        testPartitioned(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testPartitioned(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));
    }

    private void testPartitionedMultiplePages(List<Type> targetTypes)
    {
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(targetTypes, true, false);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < PAGE_COUNT; i++) {
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, false, ImmutableList.of()));
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, true, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        }

        testPartitioned(types, pages, new DataSize(128, MEGABYTE));
        testPartitioned(types, pages, new DataSize(1, KILOBYTE));

        pages.clear();
        for (int i = 0; i < PAGE_COUNT / 3; i++) {
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, false, ImmutableList.of()));
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, true, ImmutableList.of(DICTIONARY, DICTIONARY, RUN_LENGTH)));
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, false, 0.2f, 0.2f, true, ImmutableList.of(RUN_LENGTH, DICTIONARY, DICTIONARY)));
        }

        testPartitioned(types, pages, new DataSize(128, MEGABYTE));
        testPartitioned(types, pages, new DataSize(1, KILOBYTE));
    }

    private void testReplicatedSinglePage(List<Type> targetTypes)
    {
        // Add a block that only contain null as the last block to force replicating all rows.
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(targetTypes, true, true);
        Page page = PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT, true, true, 0.2f, 0.2f, false, ImmutableList.of());
        testReplicated(types, ImmutableList.of(page), new DataSize(128, MEGABYTE));
        testReplicated(types, ImmutableList.of(page), new DataSize(1, KILOBYTE));
    }

    private void testReplicatedMultiplePages(List<Type> targetTypes)
    {
        List<Type> types = updateBlockTypesWithHashBlockAndNullBlock(targetTypes, true, true);
        List<Page> pages = new ArrayList<>();
        for (int i = 0; i < PAGE_COUNT; i++) {
            pages.add(PageAssertions.createPageWithRandomData(targetTypes, POSITION_COUNT + RANDOM.nextInt(POSITION_COUNT), true, true, 0.2f, 0.2f, false, ImmutableList.of()));
        }

        testReplicated(types, pages, new DataSize(128, MEGABYTE));
        testReplicated(types, pages, new DataSize(1, KILOBYTE));
    }

    private void testPartitionedForZeroBlocks(List<Type> types, List<Page> pages, DataSize maxMemory)
    {
        testPartitioned(types, pages, maxMemory, ImmutableList.of(), new InterpretedHashGenerator(ImmutableList.of(), new int[0]));
    }

    private void testPartitioned(List<Type> types, List<Page> pages, DataSize maxMemory)
    {
        testPartitioned(types, pages, maxMemory, ImmutableList.of(0), new PrecomputedHashGenerator(0));
    }

    private void testPartitioned(List<Type> types, List<Page> pages, DataSize maxMemory, List<Integer> partitionChannel, HashGenerator hashGenerator)
    {
        TestingPartitionedOutputBuffer outputBuffer = createPartitionedOutputBuffer();
        PartitionFunction partitionFunction = new LocalPartitionGenerator(hashGenerator, PARTITION_COUNT);
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(
                types,
                partitionChannel,
                partitionFunction,
                outputBuffer,
                OptionalInt.empty(),
                maxMemory);

        Map<Integer, List<Page>> expectedPageList = new HashMap<>();

        for (Page page : pages) {
            Map<Integer, List<Integer>> positionsByPartition = new HashMap<>();
            for (int i = 0; i < page.getPositionCount(); i++) {
                int partitionNumber = partitionFunction.getPartition(page, i);
                positionsByPartition.computeIfAbsent(partitionNumber, k -> new ArrayList<>()).add(i);
            }

            for (Map.Entry<Integer, List<Integer>> entry : positionsByPartition.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    expectedPageList.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).add(copyPositions(page, entry.getValue()));
                }
            }

            operator.addInput(page);
        }
        operator.finish();

        Map<Integer, Page> expectedPages = Maps.transformValues(expectedPageList, outputPages -> mergePages(types, outputPages));
        Map<Integer, Page> actualPages = Maps.transformValues(outputBuffer.getPages(), outputPages -> mergePages(types, outputPages));

        assertEquals(actualPages.size(), expectedPages.size());

        assertEquals(actualPages.keySet(), expectedPages.keySet());

        for (Map.Entry<Integer, Page> entry : expectedPages.entrySet()) {
            int key = entry.getKey();
            assertPageEquals(types, actualPages.get(key), entry.getValue());
        }
    }

    private void testReplicated(List<Type> types, List<Page> pages, DataSize maxMemory)
    {
        TestingPartitionedOutputBuffer outputBuffer = createPartitionedOutputBuffer();
        PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(
                types,
                ImmutableList.of(0),
                partitionFunction,
                outputBuffer,
                OptionalInt.of(types.size() - 1),
                maxMemory);

        for (Page page : pages) {
            operator.addInput(page);
        }
        operator.finish();

        Map<Integer, List<Page>> acutualPageLists = outputBuffer.getPages();

        assertEquals(acutualPageLists.size(), PARTITION_COUNT);

        Page expectedPage = mergePages(types, pages);

        acutualPageLists.values().forEach(pageList -> assertPageEquals(types, mergePages(types, pageList), expectedPage));
    }

    @Test
    public void testOutputForSimplePage()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), false);
        processPages(operator, TESTING_PAGE);

        verifyOutputSizes(operator, PAGE_COUNT * TESTING_PAGE.getLogicalSizeInBytes(), PAGE_COUNT * TESTING_PAGE.getPositionCount());
    }

    @Test
    public void testOutputSizeForPageWithDictionary()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), false);
        processPages(operator, TESTING_PAGE_WITH_DICTIONARY_BLOCK);

        verifyOutputSizes(operator, PAGE_COUNT * TESTING_PAGE_WITH_DICTIONARY_BLOCK.getLogicalSizeInBytes(), PAGE_COUNT * TESTING_PAGE_WITH_DICTIONARY_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLength()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), false);
        processPages(operator, TESTING_PAGE_WITH_RLE_BLOCK);

        verifyOutputSizes(operator, PAGE_COUNT * TESTING_PAGE_WITH_RLE_BLOCK.getLogicalSizeInBytes(), PAGE_COUNT * TESTING_PAGE_WITH_RLE_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForSimplePageReplicated()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), true);
        processPages(operator, TESTING_PAGE_WITH_NULL_BLOCK);

        // Use TESTING_PAGE instead of TESTING_PAGE_WITH_NULL_BLOCK's logical size to estimate the output data size, because the null Block should not be sent over the wire.
        verifyOutputSizes(operator, PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE.getLogicalSizeInBytes(), PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE_WITH_NULL_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithDictionaryReplicated()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), true);
        processPages(operator, TESTING_PAGE_WITH_NULL_AND_DICTIONARY_BLOCK);

        // Use TESTING_PAGE_WITH_DICTIONARY_BLOCK instead of TESTING_PAGE_WITH_NULL_AND_DICTIONARY_BLOCK's logical size to estimate the output data size, because the null Block should not be sent over the wire.
        verifyOutputSizes(operator, PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE_WITH_DICTIONARY_BLOCK.getLogicalSizeInBytes(), PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE_WITH_DICTIONARY_BLOCK.getPositionCount());
    }

    @Test
    public void testOutputForPageWithRunLengthReplicated()
    {
        OptimizedPartitionedOutputOperator operator = createOptimizedPartitionedOutputOperator(ImmutableList.of(BIGINT), true);
        processPages(operator, TESTING_PAGE_WITH_NULL_AND_RLE_BLOCK);

        // Use TESTING_PAGE_WITH_RLE_BLOCK instead of TESTING_PAGE_WITH_NULL_AND_RLE_BLOCK's logical size to estimate the output data size, because the null Block should not be sent over the wire.
        verifyOutputSizes(operator, PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE_WITH_RLE_BLOCK.getLogicalSizeInBytes(), PARTITION_COUNT * PAGE_COUNT * TESTING_PAGE_WITH_NULL_AND_RLE_BLOCK.getPositionCount());
    }

    private static void processPages(OptimizedPartitionedOutputOperator operator, Page testingPageWithRleBlock)
    {
        for (int i = 0; i < PAGE_COUNT; i++) {
            operator.addInput(testingPageWithRleBlock);
        }
        operator.finish();
    }

    private static void verifyOutputSizes(OptimizedPartitionedOutputOperator operator, long expectedSizeInBytes, long expectedPositionCount)
    {
        OperatorContext operatorContext = operator.getOperatorContext();
        assertBetweenInclusive(operatorContext.getOutputDataSize().getTotalCount(),
                (long) (expectedSizeInBytes / OUTPUT_SIZE_ESTIMATION_ERROR_ALLOWANCE),
                (long) (expectedSizeInBytes * OUTPUT_SIZE_ESTIMATION_ERROR_ALLOWANCE));
        assertEquals(operatorContext.getOutputPositions().getTotalCount(), expectedPositionCount);
    }

    private Page copyPositions(Page page, List<Integer> positions)
    {
        Block[] blocks = new Block[page.getChannelCount()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = page.getBlock(i).copyPositions(positions.stream().mapToInt(j -> j).toArray(), 0, positions.size());
        }
        return new Page(positions.size(), blocks);
    }

    private TestingPartitionedOutputBuffer createPartitionedOutputBuffer()
    {
        OutputBuffers buffers = createInitialEmptyOutputBuffers(PARTITIONED);
        for (int partition = 0; partition < PARTITION_COUNT; partition++) {
            buffers = buffers.withBuffer(new OutputBuffers.OutputBufferId(partition), partition);
        }
        TestingPartitionedOutputBuffer buffer = createPartitionedBuffer(
                buffers.withNoMoreBufferIds(),
                new DataSize(Long.MAX_VALUE, BYTE)); // don't let output buffer block
        buffer.registerLifespanCompletionCallback(ignore -> {});

        return buffer;
    }

    private OptimizedPartitionedOutputOperator createOptimizedPartitionedOutputOperator(List<Type> types, boolean replicateAllRows)
    {
        TestingPartitionedOutputBuffer outputBuffer = createPartitionedOutputBuffer();
        PartitionFunction partitionFunction = new LocalPartitionGenerator(new PrecomputedHashGenerator(0), PARTITION_COUNT);

        if (replicateAllRows) {
            List<Type> replicatedTypes = updateBlockTypesWithHashBlockAndNullBlock(types, false, true);
            return createOptimizedPartitionedOutputOperator(
                    replicatedTypes,
                    ImmutableList.of(0),
                    partitionFunction,
                    outputBuffer,
                    OptionalInt.of(replicatedTypes.size() - 1),
                    MAX_MEMORY);
        }
        else {
            return createOptimizedPartitionedOutputOperator(
                    types,
                    ImmutableList.of(0),
                    partitionFunction,
                    outputBuffer,
                    OptionalInt.empty(),
                    MAX_MEMORY);
        }
    }

    private OptimizedPartitionedOutputOperator createOptimizedPartitionedOutputOperator(
            List<Type> types,
            List<Integer> partitionChannel,
            PartitionFunction partitionFunction,
            PartitionedOutputBuffer buffer,
            OptionalInt nullChannel,
            DataSize maxMemory)
    {
        PagesSerdeFactory serdeFactory = new PagesSerdeFactory(new BlockEncodingManager(), false);

        OutputPartitioning outputPartitioning = new OutputPartitioning(
                partitionFunction,
                partitionChannel,
                ImmutableList.of(Optional.empty()),
                false,
                nullChannel);

        OptimizedPartitionedOutputFactory operatorFactory = new OptimizedPartitionedOutputFactory(buffer, maxMemory);

        return (OptimizedPartitionedOutputOperator) operatorFactory
                .createOutputOperator(0, new PlanNodeId("plan-node-0"), types, Function.identity(), Optional.of(outputPartitioning), serdeFactory)
                .createOperator(createDriverContext());
    }

    private DriverContext createDriverContext()
    {
        Session testSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .build();

        return TestingTaskContext.builder(EXECUTOR, SCHEDULER, testSession)
                .setMemoryPoolSize(MAX_MEMORY)
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private TestingPartitionedOutputBuffer createPartitionedBuffer(OutputBuffers buffers, DataSize dataSize)
    {
        return new TestingPartitionedOutputBuffer(
                "task-instance-id",
                new StateMachine<>("bufferState", SCHEDULER, OPEN, TERMINAL_BUFFER_STATES),
                buffers,
                dataSize,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                SCHEDULER);
    }

    private static Block createVariableWidthBlockOverSliceView(int entries)
    {
        // Create a slice view whose address starts in the middle of the original slice, and length is half of original slice
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(entries * 2);
        for (int i = 0; i < entries * 2; i++) {
            dynamicSliceOutput.writeByte(i);
        }
        Slice slice = dynamicSliceOutput.slice().slice(entries, entries);

        int[] offsets = IntStream.range(0, entries + 1).toArray();
        return new VariableWidthBlock(entries, slice, offsets, Optional.empty());
    }

    private static class TestingPartitionedOutputBuffer
            extends PartitionedOutputBuffer
    {
        private final Map<Integer, List<Page>> pages = new HashMap<>();

        public TestingPartitionedOutputBuffer(
                String taskInstanceId,
                StateMachine<BufferState> state,
                OutputBuffers outputBuffers,
                DataSize maxBufferSize,
                Supplier<LocalMemoryContext> systemMemoryContextSupplier,
                Executor notificationExecutor)
        {
            super(taskInstanceId, state, outputBuffers, maxBufferSize, systemMemoryContextSupplier, notificationExecutor);
        }

        @Override
        public void enqueue(Lifespan lifespan, int partitionNumber, List<SerializedPage> pages)
        {
            this.pages.computeIfAbsent(partitionNumber, k -> new ArrayList<>());
            pages.stream().map(PAGES_SERDE::deserialize).forEach(this.pages.get(partitionNumber)::add);
        }

        public Map<Integer, List<Page>> getPages()
        {
            return pages;
        }
    }
}
