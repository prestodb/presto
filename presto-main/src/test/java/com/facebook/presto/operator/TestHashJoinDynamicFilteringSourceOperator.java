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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.server.DynamicFilterService;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.assertions.Assert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;

public class TestHashJoinDynamicFilteringSourceOperator
{
    private ExecutorService executorService;
    private ScheduledExecutorService scheduledExecutorService;
    private PipelineContext pipelineContext;
    private DynamicFilterService dynamicFilterService;

    @BeforeMethod
    public void setUp()
    {
        executorService = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutorService = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executorService, scheduledExecutorService, TEST_SESSION)
                .addPipelineContext(0, true, true, false);
        dynamicFilterService = new DynamicFilterService();
        dynamicFilterService.registerTasks("0", ImmutableSet.of(new TaskId("query", 0, 0, 0)));
    }

    @AfterMethod
    public void tearDown()
    {
        executorService.shutdownNow();
        scheduledExecutorService.shutdownNow();
    }

    private void verifyPassThrough(Operator operator, List<Type> types, Page... pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private OperatorFactory createOperatorFactory(HashJoinDynamicFilterSourceOperator.Channel... buildChannels)
    {
        return new HashJoinDynamicFilterSourceOperator.HashJoinDynamicFilterSourceOperatorFactory(
                TEST_SESSION,
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                Arrays.stream(buildChannels).collect(toList()),
                new InMemoryDynamicFilterClientSupplier(dynamicFilterService),
                "0",
                4);
    }

    private Operator createOperator(OperatorFactory operatorFactory)
    {
        return operatorFactory.createOperator(pipelineContext.addDriverContext());
    }

    private static HashJoinDynamicFilterSourceOperator.Channel channel(int index, Type type)
    {
        return new HashJoinDynamicFilterSourceOperator.Channel(type, index);
    }

    @Test
    public void testCollectMultipleOperators()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));

        Operator operator1 = createOperator(operatorFactory);
        verifyPassThrough(operator1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(3, 5)));

        Operator operator2 = createOperator(operatorFactory);
        operatorFactory.noMoreOperators();
        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator1).getValues(), ImmutableList.of("1", "2", "3", "5"));

        verifyPassThrough(operator2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));
        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator2).getValues(),
                ImmutableList.of("2", "3", "1", "4"));
    }

    @Test
    public void testCollectMultipleColumns()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN), channel(1, DOUBLE));
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(operator,
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(),
                ImmutableList.of("true1.5", "true3.0", "false4.5"));
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN));
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(operator,
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(),
                ImmutableList.of("true", "true", "false"));
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(1, DOUBLE));
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(operator,
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(),
                ImmutableList.of("1.5", "3.0", "4.5"));
    }

    @Test
    public void testCollectWithNulls()
    {
        Block blockWithNulls = INTEGER.createFixedSizeBlockBuilder(0)
                .writeInt(3)
                .appendNull()
                .writeInt(4)
                .build();

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, INTEGER));
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(operator,
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(),
                ImmutableList.of("1", "2", "3", "3", "", "4", "4", "5"));
    }

    @Test
    public void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory();
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(operator,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(), ImmutableList.of());
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        Operator operator = createOperator(operatorFactory);
        verifyPassThrough(createOperator(operatorFactory), ImmutableList.of(BIGINT));

        Assert.assertEquals(((HashJoinDynamicFilterSourceOperator) operator).getValues(), ImmutableList.of());
    }
}
