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
import com.facebook.presto.operator.TopNOperator.TopNOperatorFactory;
import com.facebook.presto.spi.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.appendSampleWeight;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestTopNOperator
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
    public void testSampledTopN()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .row(5, 0.5)
                .pageBreak()
                .build();
        input = appendSampleWeight(input, 2);

        TopNOperatorFactory factory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE, BIGINT),
                5,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                Optional.of(input.get(0).getChannelCount() - 1),
                false);

        Operator operator = factory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT, DOUBLE, BIGINT)
                .row(6, 0.6, 2)
                .row(5, 0.5, 1)
                .row(5, 0.5, 2)
                .build();

        assertOperatorEquals(operator, input, expected);
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
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory factory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                Optional.<Integer>absent(),
                false);

        Operator operator = factory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT, DOUBLE)
                .row(6, 0.6)
                .row(5, 0.5)
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
                .row("f", 3)
                .row("a", 4)
                .pageBreak()
                .row("d", 5)
                .row("d", 7)
                .row("e", 6)
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                ImmutableList.of(VARCHAR, BIGINT),
                3,
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST),
                Optional.<Integer>absent(),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(VARCHAR, BIGINT)
                .row("f", 3)
                .row("e", 6)
                .row("d", 7)
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
                .pageBreak()
                .row(5, 0.5)
                .row(4, 0.41)
                .row(6, 0.6)
                .pageBreak()
                .build();

        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                ImmutableList.of(BIGINT, DOUBLE),
                2,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_LAST),
                Optional.<Integer>absent(),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(BIGINT, DOUBLE)
                .row(-1, -0.1)
                .row(1, 0.1)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
