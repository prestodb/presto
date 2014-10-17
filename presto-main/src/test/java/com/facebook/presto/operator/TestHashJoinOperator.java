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
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.RowPagesBuilderWithHash.rowPagesBuilderWithHash;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestHashJoinOperator
{
    private ExecutorService executor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        taskContext = new TaskContext(new TaskId("query", "stage", "task"), executor, TEST_SESSION);
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testInnerJoin()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Integer> buildChannels = ImmutableList.of(0);
        int buildHashChannel = 3;
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(buildChannels, buildTypes)
                .addSequencePage(10, 20, 30, 40)
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), buildChannels, buildHashChannel, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT, BIGINT); // Additional BIGINT for hash
        List<Integer> probeChannels = ImmutableList.of(0);
        int probeHashChannel = 3;
        List<Page> probeInput = rowPagesBuilderWithHash(probeChannels, probeTypes)
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                probeChannels,
                probeHashChannel);

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020, 2020, "20", 30, 40)
                .row("21", 1021, 2021, "21", 31, 41)
                .row("22", 1022, 2022, "22", 32, 42)
                .row("23", 1023, 2023, "23", 33, 43)
                .row("24", 1024, 2024, "24", 34, 44)
                .row("25", 1025, 2025, "25", 35, 45)
                .row("26", 1026, 2026, "26", 36, 46)
                .row("27", 1027, 2027, "27", 37, 47)
                .row("28", 1028, 2028, "28", 38, 48)
                .row("29", 1029, 2029, "29", 39, 49)
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, ImmutableList.of(3, 7));
    }

    @Test
    public void testInnerJoinWithNullProbe()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test
    public void testInnerJoinWithNullBuild()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperatorTypes, Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test
    public void testProbeOuterJoin()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .addSequencePage(10, 20, 30, 40)
                .build());

        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 3, 100);
        Operator hashBuilderOperator = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.outerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                3);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020, 2020, "20", 30, 40)
                .row("21", 1021, 2021, "21", 31, 41)
                .row("22", 1022, 2022, "22", 32, 42)
                .row("23", 1023, 2023, "23", 33, 43)
                .row("24", 1024, 2024, "24", 34, 44)
                .row("25", 1025, 2025, "25", 35, 45)
                .row("26", 1026, 2026, "26", 36, 46)
                .row("27", 1027, 2027, "27", 37, 47)
                .row("28", 1028, 2028, "28", 38, 48)
                .row("29", 1029, 2029, "29", 39, 49)
                .row("30", 1030, 2030, null, null, null)
                .row("31", 1031, 2031, null, null, null)
                .row("32", 1032, 2032, null, null, null)
                .row("33", 1033, 2033, null, null, null)
                .row("34", 1034, 2034, null, null, null)
                .build();

        OperatorAssertion.assertOperatorEqualsIgnoreOrderWithoutHashes(joinOperator, probeInput, expected, Ints.asList(3, 7));
    }

    @Test
    public void testOuterJoinWithNullProbe()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.outerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test
    public void testOuterJoinWithNullBuild()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.outerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test
    public void testOuterJoinWithNullOnBothSides()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 1, 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        List<Type> probeOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT);
        List<Page> probeInput = rowPagesBuilderWithHash(Ints.asList(0), probeTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.outerJoin(
                0,
                hashBuilderOperatorFactory.getLookupSourceSupplier(),
                probeOperatorTypes,
                Ints.asList(0),
                1);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        OperatorAssertion.assertOperatorEqualsWithoutHashes(joinOperator, probeInput, expected, Ints.asList(1, 3));
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size.*")
    public void testMemoryLimit()
            throws Exception
    {
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, TEST_SESSION, new DataSize(100, BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        OperatorContext operatorContext = driverContext.addOperatorContext(0, ValuesOperator.class.getSimpleName());
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        List<Type> buildOperatorTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        Operator buildOperator = new ValuesOperator(operatorContext, buildOperatorTypes, rowPagesBuilderWithHash(Ints.asList(0), buildTypes)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator hashBuilderOperator = new HashBuilderOperatorFactory(1, buildOperator.getTypes(), Ints.asList(0), 3, 1_500_000).createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }
    }
}
