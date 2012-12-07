/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.AbstractPageIterator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class QueryDriversOperator
        implements Operator
{
    private static final Logger log = Logger.get(QueryDriversOperator.class);
    private final List<QueryDriverProvider> driverProviders;
    private final int pageBufferMax;
    private final List<TupleInfo> tupleInfos;

    public QueryDriversOperator(int pageBufferMax, QueryDriverProvider... driverProviders)
    {
        this(pageBufferMax, ImmutableList.copyOf(driverProviders));
    }

    public QueryDriversOperator(int pageBufferMax, Iterable<? extends QueryDriverProvider> driverProviders)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "blockBufferMax must be at least 1");
        Preconditions.checkNotNull(driverProviders, "driverProviders is null");

        this.pageBufferMax = pageBufferMax;
        this.driverProviders = ImmutableList.copyOf(driverProviders);
        Preconditions.checkArgument(!this.driverProviders.isEmpty(), "driverProviders is empty");
        tupleInfos = this.driverProviders.get(0).getTupleInfos();
    }

    @Override
    public int getChannelCount()
    {
        return driverProviders.get(0).getChannelCount();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public QueryDriversIterator iterator()
    {
        ImmutableList.Builder<QueryDriver> queries = ImmutableList.builder();
        try {
            PageBuffer outputBuffer = new PageBuffer(tupleInfos, driverProviders.size(), pageBufferMax);
            for (QueryDriverProvider provider : driverProviders) {
                QueryDriver queryDriver = provider.create(outputBuffer);
                queries.add(queryDriver);
                queryDriver.start();
            }

            return new QueryDriversIterator(outputBuffer, queries.build());
        }
        catch (Throwable e) {
            for (QueryDriver queryDriver : queries.build()) {
                queryDriver.abort();
            }
            throw Throwables.propagate(e);
        }
    }

    public static class QueryDriversIterator extends AbstractPageIterator
    {
        private final PageBuffer pageBuffer;
        private final List<QueryDriver> queryDrivers;

        private QueryDriversIterator(PageBuffer pageBuffer, Iterable<QueryDriver> queries)
        {
            super(pageBuffer.getTupleInfos());
            this.pageBuffer = pageBuffer;
            this.queryDrivers = ImmutableList.copyOf(queries);
        }

        @Override
        protected Page computeNext()
        {
            try {
                // get the next page
                while (!pageBuffer.isDone()) {
                    List<Page> nextPages = pageBuffer.getNextPages(1, new Duration(1, TimeUnit.SECONDS));
                    if (!nextPages.isEmpty()) {
                        Page page = Iterables.getOnlyElement(nextPages);
                        return page;
                    }
                }
                return endOfData();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }

        @Override
        protected void doClose()
        {
            pageBuffer.finish();
            for (QueryDriver queryDriver : queryDrivers) {
                try {
                    queryDriver.abort();
                }
                catch (Exception e) {
                    log.warn(e, "Error canceling query driver");
                }
            }
        }
    }
}
