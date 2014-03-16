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
import com.facebook.presto.spi.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestDistinctLimitOperator
{
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
    public void testDistinctLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(5, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, ImmutableList.of(BIGINT), 5);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT)
                .row(1)
                .row(2)
                .row(3)
                .row(4)
                .row(5)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testDistinctLimitWithPageAlignment()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, ImmutableList.of(BIGINT), 3);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT)
                .row(1)
                .row(2)
                .row(3)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testDistinctLimitValuesLessThanLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .addSequencePage(3, 1)
                .addSequencePage(3, 2)
                .build();

        OperatorFactory operatorFactory = new DistinctLimitOperator.DistinctLimitOperatorFactory(0, ImmutableList.of(BIGINT), 5);
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT)
                .row(1)
                .row(2)
                .row(3)
                .row(4)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, input, expected);
    }
}
