/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractIterator;
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
        tupleInfos = this.driverProviders.get(0).getTupleInfos();
        Preconditions.checkArgument(!this.driverProviders.isEmpty(), "driverProviders is empty");
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
            QueryState queryState = new QueryState(tupleInfos, driverProviders.size(), pageBufferMax);
            for (QueryDriverProvider provider : driverProviders) {
                QueryDriver queryDriver = provider.create(queryState);
                queries.add(queryDriver);
                queryDriver.start();
            }

            return new QueryDriversIterator(queryState, queries.build());
        }
        catch (Throwable e) {
            for (QueryDriver queryDriver : queries.build()) {
                queryDriver.cancel();
            }
            throw Throwables.propagate(e);
        }
    }

    public static class QueryDriversIterator extends AbstractIterator<Page>
    {
        private final QueryState queryState;
        private final List<QueryDriver> queryDrivers;

        private QueryDriversIterator(QueryState queryState, Iterable<QueryDriver> queries)
        {
            this.queryState = queryState;
            this.queryDrivers = ImmutableList.copyOf(queries);
        }

        public void cancel()
        {
            queryState.cancel();
            for (QueryDriver queryDriver : queryDrivers) {
                try {
                    queryDriver.cancel();
                }
                catch (Exception e) {
                    log.warn("Error canceling query driver", e);
                }
            }
        }

        @Override
        protected Page computeNext()
        {
            try {
                if (queryState.isCanceled()) {
                    return endOfData();
                }
                // wait forever for the next page to show up
                List<Page> nextPages = queryState.getNextPages(1, new Duration(365, TimeUnit.DAYS));
                if (nextPages.isEmpty()) {
                    return endOfData();
                }

                Page page = Iterables.getOnlyElement(nextPages);
                return page;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
    }

}
