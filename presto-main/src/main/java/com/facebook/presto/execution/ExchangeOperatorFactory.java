/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.ForExchange;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.DataSize;

import javax.inject.Inject;

public class ExchangeOperatorFactory
{
    private final AsyncHttpClient httpClient;
    private final DataSize maxBufferSize;
    private final int concurrentRequestMultiplier;

    @Inject
    public ExchangeOperatorFactory(QueryManagerConfig queryManagerConfig, @ForExchange AsyncHttpClient httpClient)
    {
        this(httpClient,
                queryManagerConfig.getExchangeMaxBufferSize(),
                queryManagerConfig.getExchangeConcurrentRequestMultiplier());
    }

    public ExchangeOperatorFactory(AsyncHttpClient httpClient, DataSize maxBufferSize, int concurrentRequestMultiplier)
    {
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkArgument(maxBufferSize.toBytes() > 0, "maxBufferSize must be at least 1 byte: %s", maxBufferSize);
        Preconditions.checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);

        this.httpClient = httpClient;
        this.maxBufferSize = maxBufferSize;
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
    }

    public ExchangeOperator createExchangeOperator(Iterable<TupleInfo> tupleInfos)
    {
        return new ExchangeOperator(httpClient, tupleInfos, maxBufferSize, concurrentRequestMultiplier);
    }
}
