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
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.HashJoinOperator.HashJoinOperatorFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestHashJoinOperator
{
    private ExecutorService executor;
    private TaskContext taskContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        taskContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session);
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
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.innerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG),
                Ints.asList(0));

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY,
                FIXED_INT_64,
                FIXED_INT_64,
                VARIABLE_BINARY,
                FIXED_INT_64,
                FIXED_INT_64)
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

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNullProbe()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.innerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNullBuild()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.innerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.innerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testProbeOuterJoin()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());

        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator hashBuilderOperator = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.outerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY,
                FIXED_INT_64,
                FIXED_INT_64,
                VARIABLE_BINARY,
                FIXED_INT_64,
                FIXED_INT_64)
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

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullProbe()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.outerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullBuild()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.outerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testOuterJoinWithNullOnBothSides()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 100);
        Operator sourceHashProvider = hashBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        HashJoinOperatorFactory joinOperatorFactory = HashJoinOperator.outerJoin(
                0,
                hashBuilderOperatorFactory.getHashSupplier(),
                ImmutableList.of(SINGLE_VARBINARY),
                Ints.asList(0));
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(VARIABLE_BINARY, VARIABLE_BINARY)
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size.*")
    public void testMemoryLimit()
            throws Exception
    {
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(100, BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        Operator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator hashBuilderOperator = new HashBuilderOperatorFactory(1, buildOperator.getTupleInfos(), Ints.asList(0), 1_500_000).createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }
    }
}
