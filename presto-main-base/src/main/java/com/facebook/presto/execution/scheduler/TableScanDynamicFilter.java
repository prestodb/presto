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
package com.facebook.presto.execution.scheduler;

import com.facebook.airlift.units.Duration;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.connector.DynamicFilter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Wraps one or more {@link JoinDynamicFilter}s for a single table scan,
 * composing their constraints via intersection. Translates column names
 * to {@link ColumnHandle}s at the SPI boundary.
 */
@ThreadSafe
public class TableScanDynamicFilter
        implements DynamicFilter
{
    private final List<JoinDynamicFilter> filters;
    private final Map<String, ColumnHandle> columnNameToHandle;
    private final Duration waitTimeout;

    public TableScanDynamicFilter(List<JoinDynamicFilter> filters, Map<String, ColumnHandle> columnNameToHandle)
    {
        requireNonNull(filters, "filters is null");
        requireNonNull(columnNameToHandle, "columnNameToHandle is null");
        verify(!filters.isEmpty(), "filters list cannot be empty");

        this.filters = ImmutableList.copyOf(filters);
        this.columnNameToHandle = ImmutableMap.copyOf(columnNameToHandle);

        for (JoinDynamicFilter filter : this.filters) {
            String columnName = filter.getColumnName();
            verify(this.columnNameToHandle.containsKey(columnName),
                    "Dynamic filter column '%s' not found in scan columns: %s",
                    columnName, this.columnNameToHandle.keySet());
        }

        this.waitTimeout = filters.stream()
                .map(JoinDynamicFilter::getWaitTimeout)
                .min(Comparator.comparing(Duration::toMillis))
                .orElse(new Duration(0, MILLISECONDS));
    }

    public List<JoinDynamicFilter> getFilters()
    {
        return filters;
    }

    @Override
    public TupleDomain<ColumnHandle> getCurrentPredicate()
    {
        // Incomplete filters return all() (identity for intersection), so the
        // constraint tightens progressively as individual filters complete
        return filters.stream()
                .map(JoinDynamicFilter::getCurrentConstraintByColumnName)
                .map(this::translateToColumnHandle)
                .reduce(TupleDomain::intersect)
                .orElse(TupleDomain.all());
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        filters.forEach(JoinDynamicFilter::startTimeout);

        List<CompletableFuture<?>> pendingFutures = filters.stream()
                .map(JoinDynamicFilter::isBlocked)
                .filter(future -> !future.isDone())
                .collect(toList());

        if (pendingFutures.isEmpty()) {
            return NOT_BLOCKED;
        }

        return CompletableFuture.anyOf(pendingFutures.toArray(new CompletableFuture[0]));
    }

    @Override
    public Duration getWaitTimeout()
    {
        return waitTimeout;
    }

    @Override
    public boolean isComplete()
    {
        return filters.stream().allMatch(JoinDynamicFilter::isComplete);
    }

    public String getFilterId()
    {
        return filters.stream()
                .map(JoinDynamicFilter::getFilterId)
                .collect(joining(","));
    }

    private TupleDomain<ColumnHandle> translateToColumnHandle(TupleDomain<String> columnNameDomain)
    {
        return columnNameDomain.transform(columnNameToHandle::get);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("filterCount", filters.size())
                .add("filterIds", getFilterId())
                .add("columnNameToHandle", columnNameToHandle)
                .add("waitTimeout", waitTimeout)
                .add("complete", isComplete())
                .toString();
    }
}
