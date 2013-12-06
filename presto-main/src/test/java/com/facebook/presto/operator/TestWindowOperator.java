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
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestWindowOperator
{
    private static final List<WindowFunction> ROW_NUMBER = ImmutableList.<WindowFunction>of(new RowNumberFunction());

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
    public void testRowNumber()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(2, 0.3)
                .row(4, 0.2)
                .row(6, 0.1)
                .pageBreak()
                .row(-1, -0.1)
                .row(5, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                ints(1, 0),
                ROW_NUMBER,
                ints(),
                ints(0),
                sortOrder(SortOrder.ASC_NULLS_LAST),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(DOUBLE, FIXED_INT_64, FIXED_INT_64)
                .row(-0.1, -1, 1)
                .row(0.3, 2, 2)
                .row(0.2, 4, 3)
                .row(0.4, 5, 4)
                .row(0.1, 6, 5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_DOUBLE, SINGLE_BOOLEAN)
                .row("b", -1, -0.1, true)
                .row("a", 2, 0.3, false)
                .row("a", 4, 0.2, true)
                .pageBreak()
                .row("b", 5, 0.4, false)
                .row("a", 6, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_DOUBLE, SINGLE_BOOLEAN),
                ints(0, 1, 2, 3),
                ROW_NUMBER,
                ints(0),
                ints(1),
                sortOrder(SortOrder.ASC_NULLS_LAST),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(VARIABLE_BINARY, FIXED_INT_64, DOUBLE, BOOLEAN, FIXED_INT_64)
                .row("a", 2, 0.3, false, 1)
                .row("a", 4, 0.2, true, 2)
                .row("a", 6, 0.1, true, 3)
                .row("b", -1, -0.1, true, 1)
                .row("b", 5, 0.4, false, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testRowNumberArbitrary()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .row(1)
                .row(3)
                .row(5)
                .row(7)
                .pageBreak()
                .row(2)
                .row(4)
                .row(6)
                .row(8)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG),
                ints(0),
                ROW_NUMBER,
                ints(),
                ints(),
                sortOrder(),
                10);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(FIXED_INT_64, FIXED_INT_64)
                .row(1, 1)
                .row(3, 2)
                .row(5, 3)
                .row(7, 4)
                .row(2, 5)
                .row(4, 6)
                .row(6, 7)
                .row(8, 8)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                ints(1),
                ROW_NUMBER,
                ints(),
                ints(0),
                sortOrder(SortOrder.ASC_NULLS_LAST),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    private static int[] ints(int... array)
    {
        return array;
    }

    private static SortOrder[] sortOrder(SortOrder... array)
    {
        return array;
    }
}
