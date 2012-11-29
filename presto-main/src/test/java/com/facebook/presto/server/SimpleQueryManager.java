/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.server.QueryState.State;
import com.facebook.presto.split.Split;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

@ThreadSafe
public class SimpleQueryManager implements QueryManager
{
    private final int pageBufferMax;
    private final int initialPages;

    @GuardedBy("this")
    private int nextQueryId;

    // todo this is a potential memory leak if the query objects are not removed up
    @GuardedBy("this")
    private final Map<String, QueryState> queries = new HashMap<>();

    @Inject
    public SimpleQueryManager()
    {
        this(20, 12);
    }

    public SimpleQueryManager(int pageBufferMax, int initialPages)
    {
        Preconditions.checkArgument(pageBufferMax > 0, "pageBufferMax must be at least 1");
        Preconditions.checkArgument(initialPages >= 0, "initialPages is negative");
        Preconditions.checkArgument(initialPages <= pageBufferMax, "initialPages is greater than pageBufferMax");
        this.pageBufferMax = pageBufferMax;
        this.initialPages = initialPages;
    }

    @Override
    public QueryInfo createQueryFragment(Map<String, List<Split>> sourceSplits, PlanFragment planFragment)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getQueryInfo(String queryId)
    {
        return new MasterQueryState(queryId, getQuery(queryId)).toQueryInfo();
    }

    @Override
    public List<QueryInfo> getAllQueryInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized QueryInfo createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        String queryId = String.valueOf(nextQueryId++);
        QueryState queryState = new QueryState(ImmutableList.of(SINGLE_VARBINARY), 1, pageBufferMax);
        queries.put(queryId, queryState);

        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");

        // load initial pages
        for (int i = 0; i < initialPages; i++) {
            try {
                queryState.addPage(new Page(createStringsBlock(Iterables.concat(Collections.nCopies(i + 1, data)))));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        queryState.sourceFinished();

        return new QueryInfo(queryId, ImmutableList.of(SINGLE_VARBINARY));
    }

    @Override
    public State getQueryStatus(String queryId)
    {
        return getQuery(queryId).getState();
    }

    @Override
    public List<Page> getQueryResults(String queryId, int maxPageCount)
            throws InterruptedException
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkArgument(maxPageCount > 0, "maxPageCount must be at least 1");

        return getQuery(queryId).getNextPages(maxPageCount);
    }

    @Override
    public void destroyQuery(String queryId)
    {
        // todo should we drop the query reference
        Preconditions.checkNotNull(queryId, "queryId is null");
        getQuery(queryId).sourceFinished();
    }

    public synchronized QueryState getQuery(String queryId)
            throws NoSuchElementException
    {
        QueryState queryState = queries.get(queryId);
        if (queryState == null) {
            throw new NoSuchElementException();
        }
        return queryState;
    }
}
