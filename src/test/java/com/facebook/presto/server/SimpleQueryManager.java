/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.block.uncompressed.UncompressedBlock;
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

import static com.facebook.presto.block.Blocks.createStringBlock;

@ThreadSafe
public class SimpleQueryManager implements QueryManager
{
    private final int blockBufferMax;
    private final int initialBlocks;

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

    public SimpleQueryManager(int blockBufferMax, int initialBlocks)
    {
        Preconditions.checkArgument(blockBufferMax > 0, "blockBufferMax must be at least 1");
        Preconditions.checkArgument(initialBlocks >= 0, "initialBlocks is negative");
        Preconditions.checkArgument(initialBlocks <= blockBufferMax, "initialBlocks is greater than blockBufferMax");
        this.blockBufferMax = blockBufferMax;
        this.initialBlocks = initialBlocks;
    }

    @Override
    public synchronized String createQuery(String query)
    {
        Preconditions.checkNotNull(query, "query is null");

        String queryId = String.valueOf(nextQueryId++);
        QueryState queryState = new QueryState(1, blockBufferMax);
        queries.put(queryId, queryState);

        List<String> data = ImmutableList.of("apple", "banana", "cherry", "date");

        // load initial blocks
        for (int i = 0; i < initialBlocks; i++) {
            UncompressedBlock block = createStringBlock(0, Iterables.concat(Collections.nCopies(i + 1, data)));
            try {
                queryState.addBlock(block);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        queryState.sourceFinished();

        return queryId;
    }

    @Override
    public List<UncompressedBlock> getQueryResults(String queryId, int maxBlockCount)
            throws InterruptedException
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkArgument(maxBlockCount > 0, "maxBlockCount must be at least 1");

        return getQuery(queryId).getNextBlocks(maxBlockCount);
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
