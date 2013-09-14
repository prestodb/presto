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

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.AlignmentOperator.AlignmentOperatorFactory;
import com.facebook.presto.sql.analyzer.Session;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.block.BlockAssertions.blockIterableBuilder;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.RowPageBuilder.rowPageBuilder;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestAlignmentOperator
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
    public void testAlignment()
            throws Exception
    {
        Operator operator = createAlignmentOperator();

        List<Page> expected = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .pageBreak()
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .pageBreak()
                .row("alice", 8)
                .row("bob", 9)
                .row("charlie", 10)
                .row("dave", 11)
                .build();

        assertOperatorEquals(operator, expected);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        Operator operator = createAlignmentOperator();

        // verify initial state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read first page
        assertPageEquals(operator.getOutput(), rowPageBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .row("alice", 0)
                .row("bob", 1)
                .row("charlie", 2)
                .row("dave", 3)
                .build());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // read second page
        assertPageEquals(operator.getOutput(), rowPageBuilder(SINGLE_VARBINARY, SINGLE_LONG)
                .row("alice", 4)
                .row("bob", 5)
                .row("charlie", 6)
                .row("dave", 7)
                .build());

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);

        // finish
        operator.finish();

        // verify state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);
    }

    private Operator createAlignmentOperator()
    {
        BlockIterable channel0 = blockIterableBuilder(VARIABLE_BINARY)
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .newBlock()
                .append("alice")
                .append("bob")
                .append("charlie")
                .append("dave")
                .build();

        BlockIterable channel1 = blockIterableBuilder(FIXED_INT_64)
                .append(0)
                .append(1)
                .append(2)
                .append(3)
                .append(4)
                .append(5)
                .append(6)
                .append(7)
                .append(8)
                .append(9)
                .append(10)
                .append(11)
                .build();

        return new AlignmentOperatorFactory(0, channel0, channel1).createOperator(driverContext);
    }
}
