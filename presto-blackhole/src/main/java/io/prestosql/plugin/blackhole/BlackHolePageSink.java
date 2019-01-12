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
package io.prestosql.plugin.blackhole;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ConnectorPageSink;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class BlackHolePageSink
        implements ConnectorPageSink
{
    private static final CompletableFuture<Collection<Slice>> NON_BLOCKED = CompletableFuture.completedFuture(ImmutableList.of());

    private final ListeningScheduledExecutorService executorService;
    private final long pageProcessingDelayMillis;
    private CompletableFuture<Collection<Slice>> appendFuture = NON_BLOCKED;

    public BlackHolePageSink(ListeningScheduledExecutorService executorService, Duration pageProcessingDelay)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.pageProcessingDelayMillis = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null").toMillis();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        appendFuture = scheduleAppend();
        return appendFuture;
    }

    private CompletableFuture<Collection<Slice>> scheduleAppend()
    {
        if (pageProcessingDelayMillis > 0) {
            return toCompletableFuture(executorService.schedule(() -> ImmutableList.of(), pageProcessingDelayMillis, MILLISECONDS));
        }
        return NON_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return appendFuture;
    }

    @Override
    public void abort() {}
}
