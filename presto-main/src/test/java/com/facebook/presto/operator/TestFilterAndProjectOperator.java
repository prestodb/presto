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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
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

        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, TEST_SESSION)
                .addPipelineContext(true, true)
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
        };
        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new GenericPageProcessor(filter, ImmutableList.of(singleColumn(VARCHAR, 0), new Add5Projection(1))),
                ImmutableList.<Type>of(VARCHAR, BIGINT));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("10", 15)
                .row("11", 16)
                .row("12", 17)
                .row("13", 18)
                .row("14", 19)
                .row("15", 20)
                .row("16", 21)
                .row("17", 22)
                .row("18", 23)
                .row("19", 24)
                .build();

        assertOperatorEquals(operator, input, expected);
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
    }
}
