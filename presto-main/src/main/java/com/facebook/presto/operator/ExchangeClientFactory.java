/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.base.Supplier;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeClientFactory
        implements Supplier<ExchangeClient>
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final DataSize maxBufferedBytes;
    private final int concurrentRequestMultiplier;
    private final Duration minErrorDuration;
    private final AsyncHttpClient httpClient;
    private final DataSize maxResponseSize;
    private final Executor executor;

    @Inject
    public ExchangeClientFactory(BlockEncodingSerde blockEncodingSerde,
            ExchangeClientConfig config,
            @ForExchange AsyncHttpClient httpClient,
            @ForExchange Executor executor)
    {
        this(blockEncodingSerde,
                config.getMaxBufferSize(),
                config.getMaxResponseSize(),
                config.getConcurrentRequestMultiplier(),
                config.getMinErrorDuration(),
                httpClient,
                executor);
    }

    public ExchangeClientFactory(
            BlockEncodingSerde blockEncodingSerde,
            DataSize maxBufferedBytes,
            DataSize maxResponseSize,
            int concurrentRequestMultiplier,
            Duration minErrorDuration,
            AsyncHttpClient httpClient,
            Executor executor)
    {
        this.blockEncodingSerde = blockEncodingSerde;
        this.maxBufferedBytes = checkNotNull(maxBufferedBytes, "maxBufferedBytes is null");
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.minErrorDuration = checkNotNull(minErrorDuration, "minErrorDuration is null");
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
        return new ExchangeClient(
                blockEncodingSerde,
                maxBufferedBytes,
                maxResponseSize,
                concurrentRequestMultiplier,
                minErrorDuration,
                httpClient,
                executor);
    }
}
