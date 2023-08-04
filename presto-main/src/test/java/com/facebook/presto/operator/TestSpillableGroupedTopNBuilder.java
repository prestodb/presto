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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.TestingMemoryContext;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spiller.TestingSpillContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.UpdateMemory.NOOP;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSpillableGroupedTopNBuilder
{
    @DataProvider
    public static Object[][] produceRowNumbers()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testThatRevokeSpillsDuringAddInput(boolean produceRowNumbers)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        Supplier<GroupByHash> groupByHashSupplier = () -> createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0));

        LocalMemoryContext userMemoryContext = new TestingMemoryContext(200L);
        LocalMemoryContext revocableMemoryContext = new TestingMemoryContext(1000L);
        DriverYieldSignal driverYieldSignal = new DriverYieldSignal();
        AggregatedMemoryContext aggregatedMemoryContextForMerge = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        AggregatedMemoryContext aggregatedMemoryContextForSpill = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        TestingSpillContext spillContext = new TestingSpillContext();

        SpillableGroupedTopNBuilder spillableGroupedTopNBuilder = new SpillableGroupedTopNBuilder(
                types,
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                () -> immediateFuture(null),
                100_000,
                userMemoryContext,
                revocableMemoryContext,
                aggregatedMemoryContextForMerge,
                aggregatedMemoryContextForSpill,
                spillContext,
                driverYieldSignal,
                spillerFactory);

        List<Page> inputPages = generatePages(1000, 10, 100);

        long emptyBuilderSize = spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes();

        // add input
        for (int i = 0; i < 20; i++) {
            spillableGroupedTopNBuilder.processPage(inputPages.get(i)).process();
            spillableGroupedTopNBuilder.updateMemoryReservations();
        }

        // revoke
        spillableGroupedTopNBuilder.startMemoryRevoke();
        spillableGroupedTopNBuilder.finishMemoryRevoke();
        // assert that spill files were created
        assertEquals(spillerFactory.getSpillsCount(), 1);
        // assert that the memory was emptied
        assertEquals(spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes(), emptyBuilderSize);
        assertEquals(userMemoryContext.getBytes(), 0);
        // assert that input uses revocable memory and that spillable builder ensures revocable memory is updated with input builder memory
        assertEquals(revocableMemoryContext.getBytes(), spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes());
        assertEquals(userMemoryContext.getBytes(), 0);

        // add input
        for (int i = 21; i < 40; i++) {
            spillableGroupedTopNBuilder.processPage(inputPages.get(i)).process();
            spillableGroupedTopNBuilder.updateMemoryReservations();
        }
        // revoke
        spillableGroupedTopNBuilder.startMemoryRevoke();
        spillableGroupedTopNBuilder.finishMemoryRevoke();
        // assert that spill files were created
        assertEquals(spillerFactory.getSpillsCount(), 2);
        // assert that the revocable memory was emptied
        assertEquals(spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes(), emptyBuilderSize);

        // add input
        for (int i = 41; i < 100; i++) {
            spillableGroupedTopNBuilder.processPage(inputPages.get(i)).process();
            spillableGroupedTopNBuilder.updateMemoryReservations();
        }

        WorkProcessor<Page> result = spillableGroupedTopNBuilder.buildResult();
        // when we call buildResult, we should have either moved the last chunk of input
        // from revocable memory to user memory, if it doesn't fit, we should have spilled it

        while (!result.isFinished()) {
            boolean res = result.process();
            if (res && !result.isFinished()) {
                Page resPage = result.getResult();
            }
        }
        assertEquals(spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes(), emptyBuilderSize);

        // assert that builder.close clears memory accounts
        spillableGroupedTopNBuilder.close();
        assertEquals(userMemoryContext.getBytes(), 0);
        assertEquals(revocableMemoryContext.getBytes(), 0);
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testNoSpilling(boolean produceRowNumbers)
    {
        DummySpillerFactory spillerFactory = new DummySpillerFactory();
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        Supplier<GroupByHash> groupByHashSupplier = () -> createGroupByHash(ImmutableList.of(types.get(0)), ImmutableList.of(0));

        // set userMemory high enough that no spilling is needed
        LocalMemoryContext userMemoryContext = new TestingMemoryContext(1000000L);
        LocalMemoryContext revocableMemoryContext = new TestingMemoryContext(1000000L);
        DriverYieldSignal driverYieldSignal = new DriverYieldSignal();
        AggregatedMemoryContext aggregatedMemoryContextForMerge = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        AggregatedMemoryContext aggregatedMemoryContextForSpill = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        TestingSpillContext spillContext = new TestingSpillContext();

        SpillableGroupedTopNBuilder spillableGroupedTopNBuilder = new SpillableGroupedTopNBuilder(
                types,
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                () -> immediateFuture(null),
                100_000,
                userMemoryContext,
                revocableMemoryContext,
                aggregatedMemoryContextForMerge,
                aggregatedMemoryContextForSpill,
                spillContext,
                driverYieldSignal,
                spillerFactory);

        List<Page> inputPages = generatePages(100, 2, 100);

        // add input
        for (int i = 0; i < 3; i++) {
            spillableGroupedTopNBuilder.processPage(inputPages.get(i)).process();
        }
        spillableGroupedTopNBuilder.updateMemoryReservations();

        // get output
        WorkProcessor<Page> outputPages = spillableGroupedTopNBuilder.buildResult();

        // assert that revocable memory was moved to user memory
        assertEquals(revocableMemoryContext.getBytes(), 0);
        assertEquals(userMemoryContext.getBytes(), spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes());

        // get output page (only 1 page in this test case)
        boolean isResAvailable = outputPages.process();
        assertTrue(isResAvailable);
        Page resPage = outputPages.getResult();
        assertEquals(resPage.getPositionCount(), 200);
    }

    @Test(dataProvider = "produceRowNumbers")
    public void testThatBuilderYieldsDuringBuildResultAndResumesWhenUnblocked(boolean produceRowNumbers)
    {
        class MemoryFuture
        {
            ListenableFuture<?> future;

            public void setFuture(ListenableFuture<?> future)
            {
                this.future = future;
            }

            public ListenableFuture<?> getFuture()
            {
                return future;
            }
        }

        DummySpillerFactory spillerFactory = new DummySpillerFactory();
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE);
        final MemoryFuture memoryWaitingFuture = new MemoryFuture();
        memoryWaitingFuture.setFuture(immediateFuture(null));
        Supplier<GroupByHash> groupByHashSupplier = () -> GroupByHash.createGroupByHash(
                ImmutableList.of(types.get(0)),
                Ints.toArray(ImmutableList.of(0)),
                Optional.empty(),
                1,
                false,
                new JoinCompiler(createTestMetadataManager(), new FeaturesConfig()),
                () -> memoryWaitingFuture.getFuture().isDone());

        LocalMemoryContext userMemoryContext = new TestingMemoryContext(200L);
        LocalMemoryContext revocableMemoryContext = new TestingMemoryContext(1000L);
        DriverYieldSignal driverYieldSignal = new DriverYieldSignal();
        AggregatedMemoryContext aggregatedMemoryContextForMerge = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        AggregatedMemoryContext aggregatedMemoryContextForSpill = AggregatedMemoryContext.newSimpleAggregatedMemoryContext();
        TestingSpillContext spillContext = new TestingSpillContext();
        SpillableGroupedTopNBuilder spillableGroupedTopNBuilder = new SpillableGroupedTopNBuilder(
                types,
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                () -> new InMemoryGroupedTopNBuilder(
                        types,
                        new SimplePageWithPositionComparator(types, ImmutableList.of(1), ImmutableList.of(ASC_NULLS_LAST)),
                        4,
                        produceRowNumbers,
                        revocableMemoryContext,
                        groupByHashSupplier.get()),
                memoryWaitingFuture::getFuture,
                100_000,
                userMemoryContext,
                revocableMemoryContext,
                aggregatedMemoryContextForMerge,
                aggregatedMemoryContextForSpill,
                spillContext,
                driverYieldSignal,
                spillerFactory);

        List<Page> inputPages = generatePages(1000, 10, 100);

        long emptyBuilderSize = spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes();

        // add input
        for (int i = 0; i < 20; i++) {
            spillableGroupedTopNBuilder.processPage(inputPages.get(i)).process();
            spillableGroupedTopNBuilder.updateMemoryReservations();
        }

        // revoke
        spillableGroupedTopNBuilder.startMemoryRevoke();
        spillableGroupedTopNBuilder.finishMemoryRevoke();

        // assert that spill files were created
        assertEquals(spillerFactory.getSpillsCount(), 1);
        // assert that the memory was emptied
        assertEquals(spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes(), emptyBuilderSize);

        assertEquals(userMemoryContext.getBytes(), 0);
        // assert that input uses revocable memory and that spillable builder ensures revocable memory is updated with input builder memory
        assertEquals(revocableMemoryContext.getBytes(), spillableGroupedTopNBuilder.getInputInMemoryGroupedTopNBuilder().getEstimatedSizeInBytes());
        assertEquals(userMemoryContext.getBytes(), 0);

        WorkProcessor<Page> result = spillableGroupedTopNBuilder.buildResult();

        // Yield after producing first output Page
        SettableFuture<?> currentWaitingFuture = SettableFuture.create();
        memoryWaitingFuture.setFuture(currentWaitingFuture);
        assertTrue(!memoryWaitingFuture.getFuture().isDone());

        // try to get output and assert that none is available
        boolean isResAvailble = result.process();
        assertFalse(isResAvailble);

        // unblock
        currentWaitingFuture.set(null);

        // output should be available
        isResAvailble = result.process();
        assertTrue(isResAvailble);
    }

    private static GroupByHash createGroupByHash(List<Type> partitionTypes, List<Integer> partitionChannels)
    {
        return GroupByHash.createGroupByHash(
                partitionTypes,
                Ints.toArray(partitionChannels),
                Optional.empty(),
                1,
                false,
                new JoinCompiler(createTestMetadataManager(), new FeaturesConfig()),
                NOOP);
    }

    private static List<Page> generatePages(int groupCount, int rowsPerGroup, int rowsPerPage)
    {
        //create input
        List<Type> types = ImmutableList.of(BIGINT, DOUBLE, VARCHAR);
        RowPagesBuilder pagesBuilder = rowPagesBuilder(types);
        int nextVal = 0;
        int nextGroup = 0;
        int totalRows = 0;
        for (int i = 0; i < groupCount; i++) {
            for (int j = 0; j < rowsPerGroup; j++) {
                pagesBuilder.row(nextGroup++, nextVal++, "Unit test written during times of increased intensity");

                if (totalRows++ % rowsPerPage == 0) {
                    pagesBuilder.pageBreak();
                }
            }
        }
        return pagesBuilder.build();
    }
}
