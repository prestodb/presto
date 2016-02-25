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
import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.operator.HashSemiJoinOperator.HashSemiJoinOperatorFactory;
import com.facebook.presto.operator.SetBuilderOperator.SetBuilderOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestHashSemiJoinOperator
{
    private ExecutorService executor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
        taskContext = createTaskContext(executor, TEST_SESSION);
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testSemiJoin(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, rowPagesBuilder.getTypes(), rowPagesBuilder
                .row(10L)
                .row(30L)
                .row(30L)
                .row(35L)
                .row(36L)
                .row(37L)
                .row(50L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(1, new PlanNodeId("test"), buildOperator.getTypes().get(0), 0, rowPagesBuilder.getHashChannel(), 10);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT, BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, BIGINT);
        List<Page> probeInput = rowPagesBuilderProbe
                .addSequencePage(10, 30, 0)
                .build();
        HashSemiJoinOperatorFactory joinOperatorFactory = new HashSemiJoinOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0);
        Operator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(30L, 0L, true)
                .row(31L, 1L, false)
                .row(32L, 2L, false)
                .row(33L, 3L, false)
                .row(34L, 4L, false)
                .row(35L, 5L, true)
                .row(36L, 6L, true)
                .row(37L, 7L, true)
                .row(38L, 8L, false)
                .row(39L, 9L, false)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBuildSideNulls(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, buildTypes, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row(2L)
                .row(2L)
                .row(3L)
                .row((Object) null)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(1, new PlanNodeId("test"), buildOperator.getTypes().get(0), 0, rowPagesBuilder.getHashChannel(), 10);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .addSequencePage(4, 1)
                .build();
        HashSemiJoinOperatorFactory joinOperatorFactory = new HashSemiJoinOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0);
        Operator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(1L, true)
                .row(2L, true)
                .row(3L, true)
                .row(4L, null)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeSideNulls(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, buildTypes, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row(3L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(1, new PlanNodeId("test"), buildOperator.getTypes().get(0), 0, rowPagesBuilder.getHashChannel(), 10);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .row(0L)
                .row((Object) null)
                .row(1L)
                .row(2L)
                .build();
        HashSemiJoinOperatorFactory joinOperatorFactory = new HashSemiJoinOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0);
        Operator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(0L, true)
                .row(null, null)
                .row(1L, true)
                .row(2L, false)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeAndBuildNulls(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, buildTypes, rowPagesBuilder
                .row(0L)
                .row(1L)
                .row((Object) null)
                .row(3L)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(1, new PlanNodeId("test"), buildOperator.getTypes().get(0), 0, rowPagesBuilder.getHashChannel(), 10);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilderProbe = rowPagesBuilder(hashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = rowPagesBuilderProbe
                .row(0L)
                .row((Object) null)
                .row(1L)
                .row(2L)
                .build();
        HashSemiJoinOperatorFactory joinOperatorFactory = new HashSemiJoinOperatorFactory(
                2,
                new PlanNodeId("test"),
                setBuilderOperatorFactory.getSetProvider(),
                rowPagesBuilderProbe.getTypes(),
                0);
        Operator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(driverContext.getSession(), concat(probeTypes, ImmutableList.of(BOOLEAN)))
                .row(0L, true)
                .row(null, null)
                .row(1L, true)
                .row(2L, null)
                .build();

        OperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected, hashEnabled, ImmutableList.of(probeTypes.size()));
    }

    @Test(dataProvider = "hashEnabledValues", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of.*")
    public void testMemoryLimit(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(100, BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), buildTypes);
        Operator buildOperator = new ValuesOperator(operatorContext, buildTypes, rowPagesBuilder
                .addSequencePage(10000, 20)
                .build());
        SetBuilderOperatorFactory setBuilderOperatorFactory = new SetBuilderOperatorFactory(1, new PlanNodeId("test"), buildOperator.getTypes().get(0), 0, rowPagesBuilder.getHashChannel(), 10);
        Operator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }
    }
}
