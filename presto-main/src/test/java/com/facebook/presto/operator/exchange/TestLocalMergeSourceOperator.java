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
package com.facebook.presto.operator.exchange;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.exchange.LocalMergeSourceOperator.LocalMergeSourceOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestLocalMergeSourceOperator
{
    @Test
    public void testSingleSource()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(1);
        List<Type> types = ImmutableList.of(INTEGER, INTEGER);

        Operator operator = createMergeOperator(
                exchange,
                types,
                ImmutableList.of(1),
                ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));

        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        List<Page> input = rowPagesBuilder(types)
                .row(1, 1)
                .row(2, 2)
                .pageBreak()
                .row(3, 3)
                .row(4, 4)
                .build();

        exchange.getBuffer(0).enqueuePage(input.get(0));
        assertTrue(operator.isBlocked().isDone());
        assertNull(operator.getOutput());
        assertFalse(operator.isBlocked().isDone());

        exchange.getBuffer(0).enqueuePage(input.get(1));
        exchange.getBuffer(0).finishWrite();
        assertTrue(operator.isBlocked().isDone());

        Page expected = rowPagesBuilder(types)
                .row(1, 1)
                .row(2, 2)
                .row(3, 3)
                .row(4, 4)
                .build()
                .get(0);
        assertPageEquals(types, operator.getOutput(), expected);
    }

    @Test
    public void testMultipleSources()
            throws Exception
    {
        LocalMergeExchange exchange = new LocalMergeExchange(3, 3);
        List<Type> types = ImmutableList.of(INTEGER, INTEGER, INTEGER);

        Operator operator = createMergeOperator(
                exchange,
                types,
                ImmutableList.of(0),
                ImmutableList.of(ASC_NULLS_FIRST));

        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        List<Page> source1 = rowPagesBuilder(types)
                .row(1, 1, 2)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .pageBreak()
                .row(55, 1, 2)
                .row(89, 1, 3)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .pageBreak()
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build();

        List<Page> source2 = rowPagesBuilder(types)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .pageBreak()
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(99, 2, 2)
                .pageBreak()
                .row(99, 1, 1)
                .row(100, 1, 7)
                .row(100, 1, 6)
                .build();

        List<Page> source3 = rowPagesBuilder(types)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .pageBreak()
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .build();

        source1.forEach(page -> exchange.getBuffer(0).enqueuePage(page));
        source2.forEach(page -> exchange.getBuffer(1).enqueuePage(page));
        source3.forEach(page -> exchange.getBuffer(2).enqueuePage(page));
        exchange.getBuffer(0).finishWrite();
        exchange.getBuffer(1).finishWrite();
        exchange.getBuffer(2).finishWrite();

        Page expected = rowPagesBuilder(types)
                .row(1, 1, 2)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .row(55, 1, 2)
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .row(99, 2, 2)
                .row(99, 1, 1)
                .row(100, 1, 7)
                .row(100, 1, 6)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build()
                .get(0);
        assertTrue(operator.isBlocked().isDone());
        assertPageEquals(types, operator.getOutput(), expected);
    }

    private static Operator createMergeOperator(
            LocalMergeExchange exchange,
            List<Type> types,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders)
    {
        DriverContext driverContext = createTaskContext(directExecutor(), TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();

        OperatorFactory factory = new LocalMergeSourceOperatorFactory(
                1,
                new PlanNodeId("id"),
                exchange,
                types,
                new OrderingCompiler(),
                sortChannels,
                sortOrders);

        return factory.createOperator(driverContext);
    }
}
