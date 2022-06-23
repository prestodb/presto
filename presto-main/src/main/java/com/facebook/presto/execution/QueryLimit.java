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
package com.facebook.presto.execution;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;

public class QueryLimit<T extends Comparable<T>>
{
    private final T limit;
    private final Source source;

    private QueryLimit(
            T limit,
            Source source)
    {
        this.limit = requireNonNull(limit, "limit is null");
        this.source = requireNonNull(source, "source is null");
    }

    public static QueryLimit<Duration> createDurationLimit(Duration limit, Source source)
    {
        return new QueryLimit<>(limit, source);
    }

    public static QueryLimit<DataSize> createDataSizeLimit(DataSize limit, Source source)
    {
        return new QueryLimit<>(limit, source);
    }

    public T getLimit()
    {
        return limit;
    }

    public Source getLimitSource()
    {
        return source;
    }

    public enum Source
    {
        QUERY,
        SYSTEM,
        RESOURCE_GROUP,
        /**/;
    }

    @SafeVarargs
    public static <S extends Comparable<S>> QueryLimit<S> getMinimum(QueryLimit<S> limit, QueryLimit<S>... limits)
    {
        Optional<QueryLimit<S>> queryLimit = Stream.concat(
                limits != null ? Arrays.stream(limits) : Stream.empty(),
                limit != null ? Stream.of(limit) : Stream.empty())
                .filter(Objects::nonNull)
                .min(Comparator.comparing(QueryLimit::getLimit));
        return queryLimit.orElseThrow(() -> new IllegalArgumentException("At least one nonnull argument is required."));
    }
}
