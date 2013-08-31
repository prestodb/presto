package com.facebook.presto.noperator;

import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Lists.newArrayList;

public class DriverOperator
        implements Operator
{
    private final DataSize maxMemory;
    private final List<DriverFactory> driverFactories;
    private final InMemoryExchange inMemoryExchange;

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
        this.maxMemory = checkNotNull(maxMemory, "maxMemory is null");
        List<NewOperatorFactory> operators = newArrayList(operatorFactories);

        inMemoryExchange = new InMemoryExchange(Iterables.getLast(operators).getTupleInfos());
        operators.add(inMemoryExchange.createSinkFactory(99));
        inMemoryExchange.noMoreSinkFactories();

        driverFactories = ImmutableList.of(new DriverFactory(operators));
    }

    @Override
    public int getChannelCount()
    {
        return inMemoryExchange.getTupleInfos().size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return inMemoryExchange.getTupleInfos();
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        checkNotNull(operatorStats, "operatorStats is null");

        DriverContext driverContext = null;

        ImmutableList.Builder<Driver> drivers = ImmutableList.builder();
        for (DriverFactory driverFactory : driverFactories) {
            drivers.add(driverFactory.createDriver(driverContext));
        }

        return new DriverOperatorIterator(drivers.build(), inMemoryExchange, operatorStats);
    }

    public static class DriverOperatorIterator
            extends AbstractPageIterator
    {
        private final List<Driver> drivers;
        private final InMemoryExchange inMemoryExchange;
        private final OperatorStats operatorStats;

        public DriverOperatorIterator(List<Driver> drivers, InMemoryExchange inMemoryExchange, OperatorStats operatorStats)
        {
            super(checkNotNull(inMemoryExchange, "inMemoryExchange is null").getTupleInfos());
            this.drivers = ImmutableList.copyOf(checkNotNull(drivers, "drivers is null"));
            this.inMemoryExchange = inMemoryExchange;
            this.operatorStats = checkNotNull(operatorStats, "operatorStats is null");
        }

        @Override
        protected Page computeNext()
        {
            boolean done = false;
            while (!done && !operatorStats.isDone()) {
                boolean processed = false;
                for (Driver driver : drivers) {
                    if (!driver.isFinished()) {
                        ListenableFuture<?> blocked = driver.process();
                        processed = true;
                    }
                }
                Page page = inMemoryExchange.removePage();
                if (page != null) {
                    return page;
                }
                done = !processed;
            }
            return endOfData();
        }

        @Override
        protected void doClose()
        {
            for (Driver driver : drivers) {
                driver.finish();
            }
        }
    }
}
