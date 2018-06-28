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
package com.facebook.presto.spiller;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.operator.PartitionFunction;
import com.facebook.presto.operator.SpillContext;
import com.facebook.presto.operator.TestingOperatorContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.PartitioningSpiller.PartitioningSpillResult;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.IntPredicate;

import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createTempDirectory;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestGenericPartitioningSpiller
{
    private static final int FIRST_PARTITION_START = -10;
    private static final int SECOND_PARTITION_START = 0;
    private static final int THIRD_PARTITION_START = 10;
    private static final int FOURTH_PARTITION_START = 20;

    private static final List<Type> TYPES = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, BIGINT);
    private final BlockEncodingSerde blockEncodingSerde = new BlockEncodingManager(new TypeRegistry());

    private Path tempDirectory;
    private SingleStreamSpillerFactory singleStreamSpillerFactory;
    private GenericPartitioningSpillerFactory factory;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempDirectory = createTempDirectory(getClass().getSimpleName());
        FeaturesConfig featuresConfig = new FeaturesConfig();
        featuresConfig.setSpillerSpillPaths(tempDirectory.toString());
        featuresConfig.setSpillerThreads(8);
        featuresConfig.setSpillMaxUsedSpaceThreshold(1.0);
        singleStreamSpillerFactory = new FileSingleStreamSpillerFactory(blockEncodingSerde, new SpillerStats(), featuresConfig);
        factory = new GenericPartitioningSpillerFactory(singleStreamSpillerFactory);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        deleteRecursively(tempDirectory, ALLOW_INSECURE);
    }

    @Test
    public void testFileSpiller()
            throws Exception
    {
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                new FourFixedPartitionsPartitionFunction(0),
                mockSpillContext(),
                mockMemoryContext())) {
            RowPagesBuilder builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);
            builder.addSequencePage(10, FIRST_PARTITION_START, -5, 0, 5);

            List<Page> firstSpill = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);
            builder.addSequencePage(10, FOURTH_PARTITION_START, 25, 30, 35);

            List<Page> secondSpill = builder.build();

            IntPredicate spillPartitionMask = ImmutableSet.of(1, 2)::contains;
            PartitioningSpillResult result = spiller.partitionAndSpill(firstSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 0);

            result = spiller.partitionAndSpill(firstSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 10);

            result = spiller.partitionAndSpill(secondSpill.get(0), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 0);

            result = spiller.partitionAndSpill(secondSpill.get(1), spillPartitionMask);
            result.getSpillingFuture().get();
            assertEquals(result.getRetained().getPositionCount(), 10);

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, SECOND_PARTITION_START, 5, 10, 15);

            List<Page> secondPartition = builder.build();

            builder = RowPagesBuilder.rowPagesBuilder(TYPES);
            builder.addSequencePage(10, THIRD_PARTITION_START, 15, 20, 25);

            List<Page> thirdPartition = builder.build();

            assertSpilledPages(
                    TYPES,
                    spiller,
                    ImmutableList.of(ImmutableList.of(), secondPartition, thirdPartition, ImmutableList.of()));
        }
    }

    @Test
    public void testCloseDuringReading()
            throws Exception
    {
        Iterator<Page> readingInProgress;
        try (PartitioningSpiller spiller = factory.create(
                TYPES,
                new ModuloPartitionFunction(0, 4),
                mockSpillContext(),
                mockMemoryContext())) {
            Page page = SequencePageBuilder.createSequencePage(TYPES, 10, FIRST_PARTITION_START, 5, 10, 15);
            PartitioningSpillResult spillResult = spiller.partitionAndSpill(page, partition -> true);
            assertEquals(spillResult.getRetained().getPositionCount(), 0);
            getFutureValue(spillResult.getSpillingFuture());

            // We get the iterator but we do not exhaust it, so that close happens during reading
            readingInProgress = spiller.getSpilledPages(0);
        }

        try {
            readingInProgress.hasNext();
            fail("Iterator.hasNext() should fail since underlying resources are closed");
        }
        catch (UncheckedIOException ignored) {
            // expected
        }
    }

    @Test
    public void testWriteManyPartitions()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT);
        int partitionCount = 4;
        AggregatedMemoryContext memoryContext = mockMemoryContext();

        try (GenericPartitioningSpiller spiller = (GenericPartitioningSpiller) factory.create(
                types,
                new ModuloPartitionFunction(0, partitionCount),
                mockSpillContext(),
                memoryContext)) {
            for (int i = 0; i < 50_000; i++) {
                Page page = SequencePageBuilder.createSequencePage(types, partitionCount, 0);
                PartitioningSpillResult spillResult = spiller.partitionAndSpill(page, partition -> true);
                assertEquals(spillResult.getRetained().getPositionCount(), 0);
                getFutureValue(spillResult.getSpillingFuture());
                getFutureValue(spiller.flush());
            }
        }
        assertEquals(memoryContext.getBytes(), 0, "Reserved bytes should be zeroed after spiller is closed");
    }

    private void assertSpilledPages(
            List<Type> types,
            PartitioningSpiller spiller,
            List<List<Page>> expectedPartitions)
    {
        for (int partition = 0; partition < expectedPartitions.size(); partition++) {
            List<Page> actualSpill = ImmutableList.copyOf(spiller.getSpilledPages(partition));
            List<Page> expectedSpill = expectedPartitions.get(partition);

            assertEquals(actualSpill.size(), expectedSpill.size());
            for (int j = 0; j < actualSpill.size(); j++) {
                assertPageEquals(types, actualSpill.get(j), expectedSpill.get(j));
            }
        }
    }

    private static AggregatedMemoryContext mockMemoryContext()
    {
        // It's important to use OperatorContext's system memory context, because it does additional bookkeeping.
        return TestingOperatorContext.create().newAggregateSystemMemoryContext();
    }

    private static SpillContext mockSpillContext()
    {
        return bytes -> {};
    }

    private static class FourFixedPartitionsPartitionFunction
            implements PartitionFunction
    {
        private final int valueChannel;

        FourFixedPartitionsPartitionFunction(int valueChannel)
        {
            this.valueChannel = valueChannel;
        }

        @Override
        public int getPartitionCount()
        {
            return 4;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = page.getBlock(valueChannel).getLong(position, 0);
            if (value >= FOURTH_PARTITION_START) {
                return 3;
            }
            if (value >= THIRD_PARTITION_START) {
                return 2;
            }
            if (value >= SECOND_PARTITION_START) {
                return 1;
            }
            return 0;
        }
    }

    private static class ModuloPartitionFunction
            implements PartitionFunction
    {
        private final int valueChannel;
        private final int partitionCount;

        ModuloPartitionFunction(int valueChannel, int partitionCount)
        {
            this.valueChannel = valueChannel;
            checkArgument(partitionCount > 0);
            this.partitionCount = partitionCount;
        }

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public int getPartition(Page page, int position)
        {
            long value = page.getBlock(valueChannel).getLong(position, 0);
            return toIntExact(Math.abs(value) % partitionCount);
        }
    }
}
