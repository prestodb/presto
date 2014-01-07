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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.FilterAndProjectOperator.FilterAndProjectOperatorFactory;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
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
        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .addSequencePage(100, 0, 0)
                .build();

        OperatorFactory operatorFactory = new FilterAndProjectOperatorFactory(
                0,
                new FilterFunction()
                {
                    @Override
                    public boolean filter(TupleReadable... cursors)
                    {
                        long value = cursors[1].getLong();
                        return 10 <= value && value < 20;
                    }

                    @Override
                    public boolean filter(RecordCursor cursor)
                    {
                        long value = cursor.getLong(0);
                        return 10 <= value && value < 20;
                    }
                }, ImmutableList.of(singleColumn(VARIABLE_BINARY, 0), new Add5Projection(1)));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, FIXED_INT_64)
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
        public TupleInfo getTupleInfo()
        {
            return SINGLE_LONG;
        }

        @Override
        public void project(TupleReadable[] cursors, BlockBuilder output)
        {
            if (cursors[channelIndex].isNull()) {
                output.appendNull();
            }
            else {
                output.append(cursors[channelIndex].getLong() + 5);
            }
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            if (cursor.isNull(channelIndex)) {
                output.appendNull();
            }
            else {
                output.append(cursor.getLong(channelIndex) + 5);
            }
        }
    }
}
