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

import com.google.common.base.Supplier;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import javax.inject.Inject;

import java.util.concurrent.Executor;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ExchangeClientFactory
        implements Supplier<ExchangeClient>
{
    private final DataSize maxBufferedBytes;
    private final int concurrentRequestMultiplier;
    private final AsyncHttpClient httpClient;
    private final DataSize maxResponseSize;
    private final Executor executor;

    @Inject
    public ExchangeClientFactory(ExchangeClientConfig config, @ForExchange AsyncHttpClient httpClient, @ForExchange Executor executor)
    {
        this(config.getExchangeMaxBufferSize(),
                new DataSize(10, Unit.MEGABYTE),
                config.getExchangeConcurrentRequestMultiplier(),
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
