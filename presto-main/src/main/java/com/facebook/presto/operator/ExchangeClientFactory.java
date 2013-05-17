package com.facebook.presto.operator;

import com.facebook.presto.execution.QueryManagerConfig;
import com.google.inject.Provider;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.inject.Inject;
import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeClientFactory
        implements Provider<ExchangeClient>
{
    private final DataSize maxBufferedBytes;
    private final int concurrentRequestMultiplier;
    private final AsyncHttpClient httpClient;
    private final DataSize maxResponseSize;
    private final Executor executor;

    @Inject
    public ExchangeClientFactory(QueryManagerConfig queryManagerConfig, @ForExchange AsyncHttpClient httpClient,  @ForExchange Executor executor)
    {
        this(queryManagerConfig.getExchangeMaxBufferSize(),
                new DataSize(10, Unit.MEGABYTE),
                queryManagerConfig.getExchangeConcurrentRequestMultiplier(),
                httpClient,
                executor);
    }

    public ExchangeClientFactory(DataSize maxBufferedBytes,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            AsyncHttpClient httpClient,
            Executor executor)
    {
        this.maxBufferedBytes = checkNotNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.maxResponseSize = checkNotNull(maxResponseSize, "maxResponseSize is null");
        this.executor = checkNotNull(executor, "executor is null");

        checkArgument(maxBufferedBytes.toBytes() > 0, "maxBufferSize must be at least 1 byte: %s", maxBufferedBytes);
        checkArgument(maxResponseSize.toBytes() > 0, "maxResponseSize must be at least 1 byte: %s", maxResponseSize);
        checkArgument(concurrentRequestMultiplier > 0, "concurrentRequestMultiplier must be at least 1: %s", concurrentRequestMultiplier);

    }

    @Override
    public ExchangeClient get()
    {
        return new ExchangeClient(maxBufferedBytes, maxResponseSize, concurrentRequestMultiplier, httpClient, executor);
    }
}
