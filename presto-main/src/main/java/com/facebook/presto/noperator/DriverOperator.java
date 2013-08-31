package com.facebook.presto.noperator;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class DriverOperator
        implements Operator
{
    private final DataSize maxMemory;
    private DriverFactory driverFactory;

    public DriverOperator(NewOperatorFactory firstOperatorFactory, NewOperatorFactory... otherOperatorFactories)
    {
        this(ImmutableList.<NewOperatorFactory>builder()
                .add(checkNotNull(firstOperatorFactory, "firstOperatorFactory is null"))
                .add(checkNotNull(otherOperatorFactories, "otherOperatorFactories is null"))
                .build());
    }

    public DriverOperator(Iterable<NewOperatorFactory> operatorFactories)
    {
        this(
                new DataSize(256, Unit.MEGABYTE),
                checkNotNull(operatorFactories, "operatorFactories is null"));
    }

    public DriverOperator(DataSize maxMemory, Iterable<NewOperatorFactory> operatorFactories)
    {
        this(
                checkNotNull(maxMemory, "maxMemory is null"),
                new DriverFactory(ImmutableList.copyOf(checkNotNull(operatorFactories, "operatorFactories is null"))));
    }

    public DriverOperator(DataSize maxMemory, DriverFactory driverFactory)
    {
        this.maxMemory = maxMemory;
        this.driverFactory = driverFactory;
    }

    @Override
    public int getChannelCount()
    {
        // this shouldn't be needed
        throw new UnsupportedOperationException();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        // this shouldn't be needed
        throw new UnsupportedOperationException();
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        checkNotNull(operatorStats, "operatorStats is null");

        TaskMemoryManager taskMemoryManager = new TaskMemoryManager(maxMemory);

        Driver driver = driverFactory.createDriver(operatorStats, taskMemoryManager);
        return new DriverOperatorIterator(driver, operatorStats);
    }

    public static class DriverOperatorIterator
            extends AbstractPageIterator
    {
        private final Driver driver;
        private final NewOperator outputOperator;
        private final OperatorStats operatorStats;

        public DriverOperatorIterator(Driver driver, OperatorStats operatorStats)
        {
            super(driver.getOutputOperator().getTupleInfos());
            this.driver = driver;
            this.outputOperator = driver.getOutputOperator();
            this.operatorStats = operatorStats;
        }

        @Override
        protected Page computeNext()
        {
            // note bad operator code can cause this to spin for ever
            while (!operatorStats.isDone() && !driver.isFinished()) {
                driver.process();
                Page output = outputOperator.getOutput();
                if (output != null) {
                    return output;
                }
            }
            return endOfData();
        }

        @Override
        protected void doClose()
        {
            driver.finish();
        }
    }
}
