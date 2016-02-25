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

import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.util.InfiniteRecordSet;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

@Test(singleThreaded = true)
public class TestRecordProjectOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSingleColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(VARCHAR), ImmutableList.copyOf(new List<?>[] {ImmutableList.of("abc"), ImmutableList.of("def"),
                                                                                                                        ImmutableList.of("g")}));

        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), RecordProjectOperator.class.getSimpleName());
        Operator operator = new RecordProjectOperator(operatorContext, records);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR)
                .row("abc")
                .row("def")
                .row("g")
                .build();

        OperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testMultiColumn()
            throws Exception
    {
        InMemoryRecordSet records = new InMemoryRecordSet(ImmutableList.of(VARCHAR, BIGINT), ImmutableList.of(
                ImmutableList.of("abc", 1L),
                ImmutableList.of("def", 2L),
                ImmutableList.of("g", 0L)));

        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), RecordProjectOperator.class.getSimpleName());
        Operator operator = new RecordProjectOperator(operatorContext, records);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("abc", 1L)
                .row("def", 2L)
                .row("g", 0L)
                .build();

        OperatorAssertion.assertOperatorEquals(operator, expected);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        InfiniteRecordSet records = new InfiniteRecordSet(ImmutableList.<Type>of(VARCHAR, BIGINT), ImmutableList.of("abc", 1L));

        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), RecordProjectOperator.class.getSimpleName());
        Operator operator = new RecordProjectOperator(operatorContext, records);

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // first read will be null due to buffering
        assertNull(operator.getOutput());

        // read first page
        Page page = null;
        for (int i = 0; i < 100; i++) {
            page = operator.getOutput();
            if (page != null) {
                break;
            }
        }
        assertNotNull(page);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // start second page... will be null due to buffering
        assertNull(operator.getOutput());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // finish
        operator.finish();

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read the buffered page
        assertNotNull(operator.getOutput());

        // verify state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }
}
