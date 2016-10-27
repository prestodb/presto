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
package com.facebook.presto.split;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Split;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class BufferingSplitSource
        implements SplitSource
{
    private final int bufferSize;
    private final SplitSource source;

    public BufferingSplitSource(SplitSource source, int bufferSize)
    {
        this.source = requireNonNull(source, "source is null");
        this.bufferSize = bufferSize;
    }

    @Override
    public ConnectorId getConnectorId()
    {
        return source.getConnectorId();
    }

    @Override
    public CompletableFuture<List<Split>> getNextBatch(int maxSize)
    {
        checkArgument(maxSize > 0, "Cannot fetch a batch of zero size");
        List<Split> result = synchronizedList(new ArrayList<>(maxSize));
        CompletableFuture<?> future = fetchSplits(Math.min(bufferSize, maxSize), maxSize, result);
        return future.thenApply(ignored -> ImmutableList.copyOf(result));
    }

    private CompletableFuture<?> fetchSplits(int min, int max, List<Split> output)
    {
        checkArgument(min <= max, "Min splits greater than max splits");
        if (source.isFinished() || output.size() >= min) {
            return completedFuture(null);
        }
        return source.getNextBatch(max - output.size())
                .thenCompose(splits -> {
                    output.addAll(splits);
                    return fetchSplits(min, max, output);
                });
    }

    @Override
    public void close()
    {
        source.close();
    }

    @Override
    public boolean isFinished()
    {
        return source.isFinished();
    }
}
