/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;

import javax.inject.Inject;

public class ExchangeOperatorFactory
{
    private final AsyncHttpClient httpClient;
    private final int maxBufferedPages;
    private final int expectedPagesPerRequest;
    private final int concurrentRequestMultiplier;

    @Inject
    public ExchangeOperatorFactory(QueryManagerConfig queryManagerConfig, @ForExchange AsyncHttpClient httpClient)
    {
        this(httpClient,
                queryManagerConfig.getExchangeMaxBufferedPages(),
                queryManagerConfig.getExchangeExpectedPagesPerRequest(),
                queryManagerConfig.getExchangeConcurrentRequestMultiplier());
    }

    public ExchangeOperatorFactory(AsyncHttpClient httpClient, int maxBufferedPages, int expectedPagesPerRequest, int concurrentRequestMultiplier)
    {
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkArgument(maxBufferedPages > 0, "maxBufferedPages must be at least 1: %s", maxBufferedPages);
        Preconditions.checkArgument(expectedPagesPerRequest > 0, "expectedPagesPerRequest must be at least 1: %s", expectedPagesPerRequest);
        Preconditions.checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);

        this.httpClient = httpClient;
        this.maxBufferedPages = maxBufferedPages;
        this.expectedPagesPerRequest = expectedPagesPerRequest;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
    }

    public ExchangeOperator createExchangeOperator(Iterable<TupleInfo> tupleInfos)
    {
        return new ExchangeOperator(httpClient, tupleInfos, maxBufferedPages, expectedPagesPerRequest, concurrentRequestMultiplier);
    }
}
