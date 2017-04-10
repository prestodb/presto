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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Collections.singleton;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void test()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .build();

        FilterFunction filter = new FilterFunction()
        {
            @Override
            public boolean filter(int position, Block... blocks)
            {
                long value = BIGINT.getLong(blocks[1], position);
                return 10 <= value && value < 20;
            }

            @Override
            public boolean filter(RecordCursor cursor)
            {
                long value = cursor.getLong(0);
                return 10 <= value && value < 20;
            }

            @Override
            public Set<Integer> getInputChannels()
            {
                return singleton(1);
            }
        };
        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                () -> new GenericPageProcessor(filter, ImmutableList.of(singleColumn(VARCHAR, 0), new Add5Projection(1))),
                ImmutableList.of(VARCHAR, BIGINT));

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    private static class Add5Projection
            implements ProjectionFunction
    {
        private final int channelIndex;

        public Add5Projection(int channelIndex)
        {
            this.channelIndex = channelIndex;
        }

        @Override
        public Type getType()
        {
            return BIGINT;
        }

        @Override
        public void project(int position, Block[] blocks, BlockBuilder output)
        {
            if (blocks[channelIndex].isNull(position)) {
                output.appendNull();
            }
            else {
                BIGINT.writeLong(output, BIGINT.getLong(blocks[channelIndex], position) + 5);
            }
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            if (cursor.isNull(channelIndex)) {
                output.appendNull();
            }
            else {
                BIGINT.writeLong(output, cursor.getLong(channelIndex) + 5);
            }
        }

        @Override
        public Set<Integer> getInputChannels()
        {
            return ImmutableSet.of(channelIndex);
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }
    }
}
