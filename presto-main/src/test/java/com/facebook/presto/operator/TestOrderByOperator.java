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
import com.facebook.presto.operator.OrderByOperator.OrderByOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestOrderByOperator
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
    public void testSingleFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .row("a", 1)
                .row("b", 2)
                .pageBreak()
                .row("b", 3)
                .row("a", 4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                ImmutableList.of(VARCHAR, BIGINT),
                ImmutableList.of(0, 1),
                10,
                ImmutableList.of(0, 1),
                ImmutableList.of(ASC_NULLS_LAST, DESC_NULLS_LAST));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("a", 4)
                .row("a", 1)
                .row("b", 3)
                .row("b", 2)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testReverseOrder()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(0),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(4)
                .row(2)
                .row(1)
                .row(-1)
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

        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        OrderByOperatorFactory operatorFactory = new OrderByOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST));

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }
}
