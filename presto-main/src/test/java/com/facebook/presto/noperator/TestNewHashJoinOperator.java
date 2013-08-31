package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.noperator.NewHashBuilderOperator.NewHashSupplier;
import com.facebook.presto.noperator.NewHashJoinOperator.NewHashJoinOperatorFactory;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.noperator.NewOperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestNewHashJoinOperator
{
    @Test
    public void testInnerJoin()
            throws Exception
    {
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 100, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.innerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
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
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.innerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build();
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.innerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
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
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.innerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator hashBuilderOperator = new NewHashBuilderOperator(hashSupplier, 0, 100, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.outerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64, VARIABLE_BINARY, FIXED_INT_64, FIXED_INT_64))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
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
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.outerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row("b")
                .row("c")
                .build();
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.outerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(new DataSize(256, MEGABYTE));
        OperatorStats operatorStats = new OperatorStats();

        // build
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator sourceHashProvider = new NewHashBuilderOperator(hashSupplier, 0, 10, taskMemoryManager);

        Driver driver = new Driver(operatorStats, buildOperator, sourceHashProvider);
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
        NewHashJoinOperatorFactory joinOperatorFactory = NewHashJoinOperator.outerJoin(hashSupplier, ImmutableList.of(SINGLE_VARBINARY), 0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(operatorStats, taskMemoryManager);

        // expected
        MaterializedResult expected = resultBuilder(new TupleInfo(VARIABLE_BINARY, VARIABLE_BINARY))
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
        NewOperator buildOperator = new StaticOperator(rowPagesBuilder(SINGLE_VARBINARY, SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 20, 30, 40)
                .build());
        NewHashSupplier hashSupplier = new NewHashSupplier(buildOperator.getTupleInfos());
        NewHashBuilderOperator hashBuilderOperator = new NewHashBuilderOperator(hashSupplier, 0, 100, new TaskMemoryManager(new DataSize(100, BYTE)));

        Driver driver = new Driver(new OperatorStats(), buildOperator, hashBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }
    }
}
