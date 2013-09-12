package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.noperator.NewLimitOperator.NewLimitOperatorFactory;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.noperator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class TestNewLimitOperator
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
    public void testLimitWithPageAlignment()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        NewOperatorFactory operatorFactory = new NewLimitOperatorFactory(0, ImmutableList.of(SINGLE_LONG), 5);
        NewOperator operator = operatorFactory.createOperator(driverContext);

        List<Page> expected = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, input, expected);
    }

    @Test
    public void testLimitWithBlockView()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(2, 6)
                .build();

        NewOperatorFactory operatorFactory = new NewLimitOperatorFactory(0, ImmutableList.of(SINGLE_LONG), 6);
        NewOperator operator = operatorFactory.createOperator(driverContext);

        List<Page> expected = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(3, 1)
                .addSequencePage(2, 4)
                .addSequencePage(1, 6)
                .build();

        NewOperatorAssertion.assertOperatorEquals(operator, input, expected);
    }
}
