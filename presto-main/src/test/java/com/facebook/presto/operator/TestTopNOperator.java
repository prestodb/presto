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
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestTopNOperator
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
    public void testSingleFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
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
                2,
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0), singleColumn(DOUBLE, 1, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortOrder.DESC_NULLS_LAST))),
                false);

        Operator operator = factory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(FIXED_INT_64, DOUBLE)
                .row(6, 0.6)
                .row(5, 0.5)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG)
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

        FieldOrderedTupleComparator comparator = new FieldOrderedTupleComparator(ImmutableList.of(0, 1), ImmutableList.of(SortOrder.DESC_NULLS_LAST, SortOrder.DESC_NULLS_LAST));
        TopNOperatorFactory operatorFactory = new TopNOperatorFactory(
                0,
                3,
                ImmutableList.of(singleColumn(VARIABLE_BINARY, 0, 0), singleColumn(FIXED_INT_64, 1, 0)),
                Ordering.from(comparator),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, FIXED_INT_64)
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
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
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
                2,
                ImmutableList.of(singleColumn(FIXED_INT_64, 0, 0), singleColumn(DOUBLE, 1, 0)),
                Ordering.from(new FieldOrderedTupleComparator(ImmutableList.of(0), ImmutableList.of(SortOrder.ASC_NULLS_LAST))),
                false);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(FIXED_INT_64, DOUBLE)
                .row(-1, -0.1)
                .row(1, 0.1)
                .build();

        assertOperatorEquals(operator, input, expected);
    }
}
