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

import com.facebook.presto.SequencePageBuilder;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.index.PageRecordSet;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingSplit;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestScanFilterAndProjectOperator
{
    private final ExecutorService executor;

    public TestScanFilterAndProjectOperator()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @Test
    public void testPageSource()
            throws Exception
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                new PageSourceProvider() {
                    @Override
                    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
                    {
                        return new FixedPageSource(ImmutableList.of(input));
                    }
                },
                () -> new GenericCursorProcessor(FilterFunctions.TRUE_FUNCTION, ImmutableList.of(singleColumn(VARCHAR, 0))),
                () -> new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, ImmutableList.of(singleColumn(VARCHAR, 0))),
                ImmutableList.of(),
                ImmutableList.of(VARCHAR));

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    @Test
    public void testRecordCursorSource()
            throws Exception
    {
        final Page input = SequencePageBuilder.createSequencePage(ImmutableList.of(VARCHAR), 10_000, 0);
        DriverContext driverContext = newDriverContext();

        ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory factory = new ScanFilterAndProjectOperator.ScanFilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                new PlanNodeId("0"),
                new PageSourceProvider() {
                    @Override
                    public ConnectorPageSource createPageSource(Session session, Split split, List<ColumnHandle> columns)
                    {
                        return new RecordPageSource(new PageRecordSet(ImmutableList.of(VARCHAR), input));
                    }
                },
                () -> new GenericCursorProcessor(FilterFunctions.TRUE_FUNCTION, ImmutableList.of(singleColumn(VARCHAR, 0))),
                () -> new GenericPageProcessor(FilterFunctions.TRUE_FUNCTION, ImmutableList.of(singleColumn(VARCHAR, 0))),
                ImmutableList.of(),
                ImmutableList.of(VARCHAR));

        SourceOperator operator = factory.createOperator(driverContext);
        operator.addSplit(new Split(new ConnectorId("test"), TestingTransactionHandle.create(), TestingSplit.createLocalSplit()));
        operator.noMoreSplits();

        MaterializedResult expected = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), ImmutableList.of(input));
        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), ImmutableList.of(VARCHAR), toPages(operator));

        assertEquals(actual.getRowCount(), expected.getRowCount());
        assertEquals(actual, expected);
    }

    public static List<Page> toPages(Operator operator)
    {
        ImmutableList.Builder<Page> outputPages = ImmutableList.builder();

        // read output until input is needed or operator is finished
        int nullPages = 0;
        while (!operator.isFinished()) {
            Page outputPage = operator.getOutput();
            if (outputPage == null) {
                // break infinite loop due to null pages
                assertTrue(nullPages < 1_000_000, "Too many null pages; infinite loop?");
                nullPages++;
            }
            else {
                outputPages.add(outputPage);
                nullPages = 0;
            }
        }

        return outputPages.build();
    }

    private DriverContext newDriverContext()
    {
        return createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }
}
