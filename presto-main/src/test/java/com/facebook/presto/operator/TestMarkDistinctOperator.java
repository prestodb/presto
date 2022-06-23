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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMarkDistinctOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private JoinCompiler joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] {{true}, {false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testMarkDistinct(boolean hashEnabled)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), rowPagesBuilder.getTypes(), ImmutableList.of(0), rowPagesBuilder.getHashChannel(), joinCompiler);

        MaterializedResult.Builder expected = resultBuilder(driverContext.getSession(), BIGINT, BOOLEAN);
        for (long i = 0; i < 100; i++) {
            expected.row(i, true);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected.build(), hashEnabled, Optional.of(1));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testRleDistinctMask(boolean hashEnabled)
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        List<Page> inputs = rowPagesBuilder
                .addSequencePage(100, 0)
                .addSequencePage(100, 50)
                .addSequencePage(1, 200)
                .addSequencePage(1, 100)
                .build();
        Page firstInput = inputs.get(0);
        Page secondInput = inputs.get(1);
        Page singleDistinctPage = inputs.get(2);
        Page singleNotDistinctPage = inputs.get(3);
        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), rowPagesBuilder.getTypes(), ImmutableList.of(0), rowPagesBuilder.getHashChannel(), joinCompiler);

        int maskChannel = firstInput.getChannelCount(); // mask channel is appended to the input
        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            operator.addInput(firstInput);
            Block allDistinctOutput = operator.getOutput().getBlock(maskChannel);
            operator.addInput(firstInput);
            Block noDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // all distinct and no distinct conditions produce RLE blocks
            assertInstanceOf(allDistinctOutput, RunLengthEncodedBlock.class);
            assertTrue(BOOLEAN.getBoolean(allDistinctOutput, 0));
            assertInstanceOf(noDistinctOutput, RunLengthEncodedBlock.class);
            assertFalse(BOOLEAN.getBoolean(noDistinctOutput, 0));

            operator.addInput(secondInput);
            Block halfDistinctOutput = operator.getOutput().getBlock(maskChannel);
            // [0,50) is not distinct
            for (int position = 0; position < 50; position++) {
                assertFalse(BOOLEAN.getBoolean(halfDistinctOutput, position));
            }
            for (int position = 50; position < 100; position++) {
                assertTrue(BOOLEAN.getBoolean(halfDistinctOutput, position));
            }

            operator.addInput(singleDistinctPage);
            Block singleDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertFalse(singleDistinctBlock instanceof RunLengthEncodedBlock, "single position inputs should not be RLE");
            assertTrue(BOOLEAN.getBoolean(singleDistinctBlock, 0));

            operator.addInput(singleNotDistinctPage);
            Block singleNotDistinctBlock = operator.getOutput().getBlock(maskChannel);
            assertFalse(singleNotDistinctBlock instanceof RunLengthEncodedBlock, "single position inputs should not be RLE");
            assertFalse(BOOLEAN.getBoolean(singleNotDistinctBlock, 0));
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Test(dataProvider = "dataType")
    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, new PlanNodeId("test"), ImmutableList.of(type), ImmutableList.of(0), Optional.of(1), joinCompiler);

        // get result with yield; pick a relatively small buffer for partitionRowCount's memory usage
        GroupByHashYieldAssertion.GroupByHashYieldResult result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, operator -> ((MarkDistinctOperator) operator).getCapacity(), 1_400_000);
        assertGreaterThan(result.getYieldCount(), 5);
        assertGreaterThan(result.getMaxReservedBytes(), 20L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            assertEquals(page.getChannelCount(), 3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(2).getByte(i), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }
}
