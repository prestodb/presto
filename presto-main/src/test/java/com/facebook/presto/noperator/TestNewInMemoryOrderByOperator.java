package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.noperator.NewInMemoryOrderByOperator.NewInMemoryOrderByOperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.noperator.NewOperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.noperator.NewOperatorAssertion.toPages;
import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestNewInMemoryOrderByOperator
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
    public void testSingleFieldKey()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        NewInMemoryOrderByOperatorFactory operatorFactory = new NewInMemoryOrderByOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                0,
                new int[]{1},
                10);

        NewOperator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(DOUBLE)
                .row(-0.1)
                .row(0.1)
                .row(0.2)
                .row(0.4)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testMultiFieldKey()
            throws Exception
    {
        TupleInfo tupleInfo = new TupleInfo(VARIABLE_BINARY, FIXED_INT_64);
        List<Page> input = rowPagesBuilder(tupleInfo)
                .row("a", 1)
                .row("b", 2)
                .pageBreak()
                .row("b", 3)
                .row("a", 4)
                .build();

        NewInMemoryOrderByOperatorFactory operatorFactory = new NewInMemoryOrderByOperatorFactory(
                0,
                ImmutableList.of(tupleInfo),
                0,
                new int[]{0},
                10);

        NewOperator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(tupleInfo)
                .row("a", 1)
                .row("a", 4)
                .row("b", 2)
                .row("b", 3)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testReverseOrder()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        NewInMemoryOrderByOperatorFactory operatorFactory = new NewInMemoryOrderByOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                0,
                new int[]{0},
                10,
                new int[]{0},
                new boolean[]{false});

        NewOperator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(FIXED_INT_64)
                .row(4)
                .row(2)
                .row(1)
                .row(-1)
                .build();

        assertOperatorEquals(operator, input, expected);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG, SINGLE_DOUBLE)
                .row(1, 0.1)
                .row(2, 0.2)
                .pageBreak()
                .row(-1, -0.1)
                .row(4, 0.4)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        NewInMemoryOrderByOperatorFactory operatorFactory = new NewInMemoryOrderByOperatorFactory(
                0,
                ImmutableList.of(SINGLE_LONG, SINGLE_DOUBLE),
                0,
                new int[]{1},
                10);

        NewOperator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }
}
