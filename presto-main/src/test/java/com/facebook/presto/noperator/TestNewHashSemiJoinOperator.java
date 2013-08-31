package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.noperator.NewHashSemiJoinOperator.NewHashSemiJoinOperatorFactory;
import com.facebook.presto.noperator.NewSetBuilderOperator.NewSetBuilderOperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestNewHashSemiJoinOperator
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
    public void testSemiJoin()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        NewOperator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_LONG)
                .row(10)
                .row(30)
                .row(30)
                .row(35)
                .row(36)
                .row(37)
                .row(50)
                .build());
        NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(1, buildOperator.getTupleInfos(), 0, 10);
        NewOperator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_LONG, SINGLE_LONG)
                .addSequencePage(10, 30, 0)
                .build();
        NewHashSemiJoinOperatorFactory joinOperatorFactory = new NewHashSemiJoinOperatorFactory(
                2,
                setBuilderOperatorFactory.getSetProvider(),
                ImmutableList.of(SINGLE_LONG, SINGLE_LONG),
                0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(FIXED_INT_64, FIXED_INT_64, BOOLEAN)
                .row(30, 0, true)
                .row(31, 1, false)
                .row(32, 2, false)
                .row(33, 3, false)
                .row(34, 4, false)
                .row(35, 5, true)
                .row(36, 6, true)
                .row(37, 7, true)
                .row(38, 8, false)
                .row(39, 9, false)
                .build();

        NewOperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testBuildSideNulls()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        NewOperator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_LONG)
                .row(0)
                .row(1)
                .row(2)
                .row(2)
                .row(3)
                .row((Object) null)
                .build());
        NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(1, buildOperator.getTupleInfos(), 0, 10);
        NewOperator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(4, 1)
                .build();
        NewHashSemiJoinOperatorFactory joinOperatorFactory = new NewHashSemiJoinOperatorFactory(
                2,
                setBuilderOperatorFactory.getSetProvider(),
                ImmutableList.of(SINGLE_LONG),
                0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(FIXED_INT_64, BOOLEAN)
                .row(1, true)
                .row(2, true)
                .row(3, true)
                .row(4, null)
                .build();

        NewOperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testProbeSideNulls()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        NewOperator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_LONG)
                .row(0)
                .row(1)
                .row(3)
                .build());
        NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(1, buildOperator.getTupleInfos(), 0, 10);
        NewOperator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_LONG)
                .row(0)
                .row((Object) null)
                .row(1)
                .row(2)
                .build();
        NewHashSemiJoinOperatorFactory joinOperatorFactory = new NewHashSemiJoinOperatorFactory(
                2,
                setBuilderOperatorFactory.getSetProvider(),
                ImmutableList.of(SINGLE_LONG),
                0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(FIXED_INT_64, BOOLEAN)
                .row(0, true)
                .row(null, null)
                .row(1, true)
                .row(2, false)
                .build();

        NewOperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected);
    }

    @Test
    public void testProbeAndBuildNulls()
            throws Exception
    {
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

        // build
        OperatorContext operatorContext = driverContext.addOperatorContext(0, StaticOperator.class.getSimpleName());
        NewOperator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_LONG)
                .row(0)
                .row(1)
                .row((Object) null)
                .row(3)
                .build());
        NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(1, buildOperator.getTupleInfos(), 0, 10);
        NewOperator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }

        // probe
        List<Page> probeInput = rowPagesBuilder(SINGLE_LONG)
                .row(0)
                .row((Object) null)
                .row(1)
                .row(2)
                .build();
        NewHashSemiJoinOperatorFactory joinOperatorFactory = new NewHashSemiJoinOperatorFactory(
                2,
                setBuilderOperatorFactory.getSetProvider(),
                ImmutableList.of(SINGLE_LONG),
                0);
        NewOperator joinOperator = joinOperatorFactory.createOperator(driverContext);

        // expected
        MaterializedResult expected = resultBuilder(FIXED_INT_64, BOOLEAN)
                .row(0, true)
                .row(null, null)
                .row(1, true)
                .row(2, null)
                .build();

        NewOperatorAssertion.assertOperatorEquals(joinOperator, probeInput, expected);
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
        NewOperator buildOperator = new StaticOperator(operatorContext, rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(1000, 20)
                .build());
        NewSetBuilderOperatorFactory setBuilderOperatorFactory = new NewSetBuilderOperatorFactory(1, buildOperator.getTupleInfos(), 0, 10);
        NewOperator setBuilderOperator = setBuilderOperatorFactory.createOperator(driverContext);

        Driver driver = new Driver(driverContext, buildOperator, setBuilderOperator);
        while (!driver.isFinished()) {
            driver.process();
        }
    }
}
