package com.facebook.presto.execution;

import io.airlift.configuration.Config;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class QueryManagerConfig
{
    private boolean coordinator = true;
    private boolean importsEnabled = true;
    private DataSize maxOperatorMemoryUsage = new DataSize(256, Unit.MEGABYTE);
    private long maxSplitCount = 100_000;
    private int maxPendingSplitsPerNode = 100;
    private int maxShardProcessorThreads = Runtime.getRuntime().availableProcessors() * 4;
    private Integer sinkMaxBufferedPages;
    private Duration maxQueryAge = new Duration(15, TimeUnit.MINUTES);
    private Duration clientTimeout = new Duration(1, TimeUnit.MINUTES);

    private int exchangeMaxBufferedPages = 100;
    private int exchangeExpectedPagesPerRequest = 10;
    private int exchangeConcurrentRequestMultiplier = 3;

    public boolean isCoordinator()
    {
        return coordinator;
    }

    @Config("coordinator")
    public QueryManagerConfig setCoordinator(boolean coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public boolean isImportsEnabled()
    {
        return importsEnabled;
    }

    @Config("import.enabled")
    public QueryManagerConfig setImportsEnabled(boolean importsEnabled)
    {
        this.importsEnabled = importsEnabled;
        return this;
    }

    public long getMaxSplitCount()
    {
        return maxSplitCount;
    }

    @Config("query.max-splits")
    public QueryManagerConfig setMaxSplitCount(long maxSplitCount)
    {
        this.maxSplitCount = maxSplitCount;
        return this;
    }

    @Min(1)
    public int getMaxPendingSplitsPerNode()
    {
        return maxPendingSplitsPerNode;
    }

    @Config("query.max-pending-splits-per-node")
    public QueryManagerConfig setMaxPendingSplitsPerNode(int maxPendingSplitsPerNode)
    {
        this.maxPendingSplitsPerNode = maxPendingSplitsPerNode;
        return this;
    }

    @NotNull
    public DataSize getMaxOperatorMemoryUsage()
    {
        return maxOperatorMemoryUsage;
    }

    @Config("query.operator.max-memory")
    public QueryManagerConfig setMaxOperatorMemoryUsage(DataSize maxOperatorMemoryUsage)
    {
        this.maxOperatorMemoryUsage = maxOperatorMemoryUsage;
        return this;
    }

    @Min(1)
    public int getMaxShardProcessorThreads()
    {
        return maxShardProcessorThreads;
    }

    @Config("query.shard.max-threads")
    public QueryManagerConfig setMaxShardProcessorThreads(int maxShardProcessorThreads)
    {
        this.maxShardProcessorThreads = maxShardProcessorThreads;
        return this;
    }

    public Integer getSinkMaxBufferedPages()
    {
        return sinkMaxBufferedPages;
    }

    @Config("sink.page-buffer-max")
    public QueryManagerConfig setSinkMaxBufferedPages(Integer sinkMaxBufferedPages)
    {
        this.sinkMaxBufferedPages = sinkMaxBufferedPages;
        return this;
    }

    @NotNull
    public Duration getMaxQueryAge()
    {
        return maxQueryAge;
    }

    @Config("query.max-age")
    public QueryManagerConfig setMaxQueryAge(Duration maxQueryAge)
    {
        this.maxQueryAge = maxQueryAge;
        return this;
    }

    @MinDuration("5s")
    @NotNull
    public Duration getClientTimeout()
    {
        return clientTimeout;
    }

    @Config("query.client.timeout")
    public QueryManagerConfig setClientTimeout(Duration clientTimeout)
    {
        this.clientTimeout = clientTimeout;
        return this;
    }

    @Min(1)
    public int getExchangeMaxBufferedPages()
    {
        return exchangeMaxBufferedPages;
    }

    @Config("exchange.page-buffer-max")
    public QueryManagerConfig setExchangeMaxBufferedPages(int exchangeMaxBufferedPages)
    {
        this.exchangeMaxBufferedPages = exchangeMaxBufferedPages;
        return this;
    }

    @Min(1)
    public int getExchangeExpectedPagesPerRequest()
    {
        return exchangeExpectedPagesPerRequest;
    }

    @Config("exchange.expected-pages-per-request")
    public QueryManagerConfig setExchangeExpectedPagesPerRequest(int exchangeExpectedPagesPerRequest)
    {
        this.exchangeExpectedPagesPerRequest = exchangeExpectedPagesPerRequest;
        return this;
    }

    @Min(1)
    public int getExchangeConcurrentRequestMultiplier()
    {
        return exchangeConcurrentRequestMultiplier;
    }

    @Config("exchange.concurrent-request-multiplier")
    public QueryManagerConfig setExchangeConcurrentRequestMultiplier(int exchangeConcurrentRequestMultiplier)
    {
        this.exchangeConcurrentRequestMultiplier = exchangeConcurrentRequestMultiplier;
        return this;
    }
}
