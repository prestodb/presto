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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestWindowOperator
{
    private static final List<WindowFunction> ROW_NUMBER = ImmutableList.<WindowFunction>of(new RowNumberFunction());

    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", TimeZone.getTimeZone("UTC"), Locale.ENGLISH, "address", "agent");
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
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2, 0.3)
                .row(4, 0.2)
                .row(6, 0.1)
                .pageBreak()
                .row(-1, -0.1)
                .row(5, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(DOUBLE, BIGINT, BIGINT)
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
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1, -0.1, true)
                .row("a", 2, 0.3, false)
                .row("a", 4, 0.2, true)
                .pageBreak()
                .row("b", 5, 0.4, false)
                .row("a", 6, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
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
        List<Page> input = rowPagesBuilder(BIGINT)
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
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}),
                10);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT, BIGINT)
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

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", TimeZone.getTimeZone("UTC"), Locale.ENGLISH, "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = new WindowOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }
}
