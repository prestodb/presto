package com.facebook.presto.benchmark;

import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.NewOperatorFactory;
import com.facebook.presto.operator.NullOutputOperator.NullOutputOperatorFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public abstract class AbstractSimpleOperatorBenchmark
        extends AbstractNewOperatorBenchmark
{
    protected AbstractSimpleOperatorBenchmark(ExecutorService executor,
            TpchBlocksProvider tpchBlocksProvider,
            String benchmarkName, int warmupIterations, int measuredIterations)
    {
        super(executor, tpchBlocksProvider, benchmarkName, warmupIterations, measuredIterations);
    }

    protected abstract List<? extends NewOperatorFactory> createOperatorFactories();

    protected DriverFactory createDriverFactory()
    {
        List<NewOperatorFactory> operatorFactories = new ArrayList<>(createOperatorFactories());

        operatorFactories.add(new NullOutputOperatorFactory(999, Iterables.getLast(operatorFactories).getTupleInfos()));

        return new DriverFactory(true, true, operatorFactories);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        DriverFactory driverFactory = createDriverFactory();
        DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();
        Driver driver = driverFactory.createDriver(driverContext);
        return ImmutableList.of(driver);
    }
}
