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

import com.facebook.presto.common.Page;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.BETWEEN;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestFilterAndProjectOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void test()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .build();

        MetadataManager metadata = createTestMetadataManager();
        FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
        RowExpression filter = call(
                BETWEEN.name(),
                functionAndTypeManager.resolveOperator(BETWEEN, fromTypes(BIGINT, BIGINT, BIGINT)),
                BOOLEAN,
                field(1, BIGINT),
                constant(10L, BIGINT),
                constant(19L, BIGINT));

        RowExpression field0 = field(0, VARCHAR);
        RowExpression add5 = call(
                ADD.name(),
                functionAndTypeManager.resolveOperator(ADD, fromTypes(BIGINT, BIGINT)),
                BIGINT,
                field(1, BIGINT),
                constant(5L, BIGINT));

        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(filter), ImmutableList.of(field0, add5));

        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(VARCHAR, BIGINT),
                new DataSize(0, BYTE),
                0);

        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), VARCHAR, BIGINT)
                .row("10", 15L)
                .row("11", 16L)
                .row("12", 17L)
                .row("13", 18L)
                .row("14", 19L)
                .row("15", 20L)
                .row("16", 21L)
                .row("17", 22L)
                .row("18", 23L)
                .row("19", 24L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMergeOutput()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .addSequencePage(100, 0, 0)
                .build();
        MetadataManager metadata = createTestMetadataManager();

        RowExpression filter = call(
                EQUAL.name(),
                metadata.getFunctionAndTypeManager().resolveOperator(EQUAL, fromTypes(BIGINT, BIGINT)),
                BOOLEAN,
                field(1, BIGINT),
                constant(10L, BIGINT));

        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(TEST_SESSION.getSqlFunctionProperties(), Optional.of(filter), ImmutableList.of(field(1, BIGINT)));

        OperatorFactory operatorFactory = new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                0,
                new PlanNodeId("test"),
                processor,
                ImmutableList.of(BIGINT),
                new DataSize(64, KILOBYTE),
                2);

        List<Page> expected = rowPagesBuilder(BIGINT)
                .row(10L)
                .row(10L)
                .row(10L)
                .row(10L)
                .build();

        assertOperatorEquals(operatorFactory, ImmutableList.of(BIGINT), driverContext, input, expected);
    }
}
