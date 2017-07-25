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
package com.facebook.presto.hive;

import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.DynamicFilterDescription;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.concurrent.MoreFutures;
import org.joda.time.DateTimeZone;

import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILE_NOT_FOUND;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_UNKNOWN_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.failedFuture;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private final AsyncQueue<ConnectorSplit> queue;
    private final AtomicReference<Throwable> throwable = new AtomicReference<>();
    private final HiveSplitLoader splitLoader;
    private final List<Future<DynamicFilterDescription>> filters;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private volatile boolean closed;

    HiveSplitSource(int maxOutstandingSplits, HiveSplitLoader splitLoader, Executor executor, List<Future<DynamicFilterDescription>> dynamicFilters, DateTimeZone timeZone, TypeManager typeManager)
    {
        this.queue = new AsyncQueue<>(maxOutstandingSplits, executor);
        this.splitLoader = splitLoader;
        this.filters = ImmutableList.copyOf(dynamicFilters);
        this.timeZone = timeZone;
        this.typeManager = typeManager;
    }

    @VisibleForTesting
    int getOutstandingSplitCount()
    {
        return queue.size();
    }

    CompletableFuture<?> addToQueue(Iterator<? extends ConnectorSplit> splits)
    {
        CompletableFuture<?> lastResult = CompletableFuture.completedFuture(null);
        while (splits.hasNext()) {
            ConnectorSplit split = splits.next();
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    CompletableFuture<?> addToQueue(ConnectorSplit split)
    {
        if (throwable.get() == null) {
            return queue.offer(split);
        }
        return CompletableFuture.completedFuture(null);
    }

    void finished()
    {
        if (throwable.get() == null) {
            queue.finish();
            splitLoader.stop();
        }
    }

    void fail(Throwable e)
    {
        // only record the first error message
        if (throwable.compareAndSet(null, e)) {
            // add finish the queue
            queue.finish();

            // no need to process any more jobs
            splitLoader.stop();
        }
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize)
    {
        checkState(!closed, "Provider is already closed");

        CompletableFuture<List<ConnectorSplit>> future = queue.getBatchAsync(maxSize);

        // Before returning, check if there is a registered failure.
        // If so, we want to throw the error, instead of returning because the scheduler can block
        // while scheduling splits and wait for work to finish before continuing.  In this case,
        // we want to end the query as soon as possible and abort the work
        if (throwable.get() != null) {
            return failedFuture(throwable.get());
        }

        return future.thenApply(this::dynamicallyFilterSplits);
    }

    private List<ConnectorSplit> dynamicallyFilterSplits(List<ConnectorSplit> splits)
    {
        List<DynamicFilterDescription> dynamicFilterDescriptions = getAvailableDynamicFilterDescriptions();

        TupleDomain<HiveColumnHandle> runtimeTupleDomain = dynamicFilterDescriptions.stream()
                .map(DynamicFilterDescription::getTupleDomain)
                .map(domain -> domain.transform(HiveColumnHandle.class::cast))
                .reduce(TupleDomain.all(), TupleDomain::columnWiseUnion);

        Optional<Map<HiveColumnHandle, Domain>> domains = runtimeTupleDomain.getDomains();
        if (!domains.isPresent() || domains.get().size() == 0) {
            return splits;
        }

        DomainsCache domainsCache = new DomainsCache(domains.get());

        Iterator<ConnectorSplit> iter = splits.iterator();
        while (iter.hasNext()) {
            HiveSplit hiveSplit = (HiveSplit) iter.next();
            for (HivePartitionKey partitionKey : hiveSplit.getPartitionKeys()) {
                String partitionKeyName = partitionKey.getName().toLowerCase(Locale.ENGLISH);
                Collection<Domain> relevantDomains = domainsCache.getDomains(partitionKeyName);

                boolean matched = false;
                for (Domain predicateDomain : relevantDomains) {
                    Type type = partitionKey.getHiveType().getType(typeManager);
                    Object objectToWrite;
                    try {
                        objectToWrite = HiveUtil.parsePartitionValue(partitionKey.getName(), partitionKey.getValue(), type, timeZone).getValue();
                    }
                    catch (PrestoException e) {
                        // if the type is not supported, skip pruning for that partition
                        if (e.getErrorCode().equals(NOT_SUPPORTED)) {
                            // TODO: log information about not supported type
                            continue;
                        }

                        throw e;
                    }
                    if (predicateDomain.overlaps(Domain.singleValue(type, objectToWrite))) {
                        matched = true;
                        break;
                    }
                }
                if (!matched) {
                    // no relevant predicate matched, so remove that split
                    iter.remove();
                }
            }
        }
        return splits;
    }

    class DomainsCache
    {
        private final Map<HiveColumnHandle, Domain> domains;
        private final Map<String, Collection<Domain>> domainsCache = new HashMap<>();

        public DomainsCache(Map<HiveColumnHandle, Domain> domains)
        {
            this.domains = ImmutableMap.copyOf(domains);
        }

        public Collection<Domain> getDomains(String partitionName)
        {
            String partitionNameLower = partitionName.toLowerCase(Locale.ENGLISH);
            Collection<Domain> relevantDomains = domainsCache.get(partitionNameLower);
            if (relevantDomains == null) {
                relevantDomains = domains.entrySet().stream()
                        .filter(entry -> entry.getKey().getName().equalsIgnoreCase(partitionNameLower))
                        .map(Map.Entry::getValue)
                        .collect(toImmutableList());
                domainsCache.put(partitionNameLower, relevantDomains);
            }

            return relevantDomains;
        }
    }

    private List<DynamicFilterDescription> getAvailableDynamicFilterDescriptions()
    {
        ImmutableList.Builder<DynamicFilterDescription> dynamicFilterDescriptionBuilder = ImmutableList.builder();
        for (Future<DynamicFilterDescription> descriptionFuture : filters) {
            Optional<DynamicFilterDescription> description = MoreFutures.tryGetFutureValue(descriptionFuture);
            description.ifPresent(dynamicFilterDescriptionBuilder::add);
        }
        return dynamicFilterDescriptionBuilder.build();
    }

    @Override
    public boolean isFinished()
    {
        // the finished marker must be checked before checking the throwable
        // to avoid a race with the fail method
        boolean isFinished = queue.isFinished();
        if (throwable.get() != null) {
            throw propagatePrestoException(throwable.get());
        }
        return isFinished;
    }

    @Override
    public void close()
    {
        queue.finish();
        splitLoader.stop();

        closed = true;
    }

    private static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HIVE_UNKNOWN_ERROR, throwable);
    }
}
