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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.raptor.RaptorBucketFunction;
import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;
import org.joda.time.DateTimeZone;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ShardSplitter
{
    final OptionalInt bucketCount;
    final OptionalLong temporalColumnId;
    final Optional<Type> temporalColumnType;
    final Optional<DateTimeZone> shardDayBoundaryTimeZone;

    public ShardSplitter(OptionalInt bucketCount, OptionalLong temporalColumnId, Optional<Type> temporalColumnType, Optional<DateTimeZone> shardDayBoundaryTimeZone)
    {
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        this.temporalColumnId = requireNonNull(temporalColumnId, "temporalColumnId is null");
        this.temporalColumnType = requireNonNull(temporalColumnType, "temperalColumnType is null");
        this.shardDayBoundaryTimeZone = requireNonNull(shardDayBoundaryTimeZone, "shardDayBoundaryTimeZone is null");
    }

    public boolean shouldSplit()
    {
        return (bucketCount.isPresent() || temporalColumnId.isPresent());
    }

    public interface TemporalFunction
    {
        int getDay(Block block, int position);
    }

    public Optional<TemporalFunction> createTemporalFunction()
    {
        if (!temporalColumnType.isPresent()) {
            return Optional.empty();
        }
        verify(shardDayBoundaryTimeZone.isPresent());

        if (temporalColumnType.equals(DATE)) {
            return Optional.of((temporalBlock, position) -> toIntExact(DATE.getLong(temporalBlock, position)));
        }

        if (temporalColumnType.equals(TIMESTAMP)) {
            return Optional.of((temporalBlock, position) -> toIntExact(MILLISECONDS.toDays(shardDayBoundaryTimeZone.get().convertUTCToLocal(temporalBlock.getLong(position, 0)))));
        }

        throw new IllegalArgumentException("Wrong type for temporal column: " + temporalColumnType);
    }

    public Optional<BucketFunction> createBucketFunction(List<Type> types)
    {
        return bucketCount.isPresent() ? Optional.of(new RaptorBucketFunction(bucketCount.getAsInt(), types)) : Optional.empty();
    }

    public Collection<Collection<ShardIndexInfo>> getShardsByDaysBuckets(Collection<ShardIndexInfo> shards)
    {
        // Neither bucketed nor temporal, no partitioning required
        if (!bucketCount.isPresent() && !temporalColumnId.isPresent()) {
            return ImmutableList.of(shards);
        }

        // if only bucketed, partition by bucket number
        if (bucketCount.isPresent() && !temporalColumnId.isPresent()) {
            return Multimaps.index(shards, shard -> shard.getBucketNumber().getAsInt()).asMap().values();
        }

        // if temporal, partition into days first
        ImmutableMultimap.Builder<Long, ShardIndexInfo> shardsByDaysBuilder = ImmutableMultimap.builder();
        shards.stream()
                .filter(shard -> shard.getTemporalRange().isPresent())
                .forEach(shard -> {
                    long day = determineDay(shard.getTemporalRange().get());
                    shardsByDaysBuilder.put(day, shard);
                });

        Collection<Collection<ShardIndexInfo>> byDays = shardsByDaysBuilder.build().asMap().values();

        // if table is bucketed further partition by bucket number
        if (!bucketCount.isPresent()) {
            return byDays;
        }

        ImmutableList.Builder<Collection<ShardIndexInfo>> sets = ImmutableList.builder();
        for (Collection<ShardIndexInfo> s : byDays) {
            sets.addAll(Multimaps.index(s, ShardIndexInfo::getBucketNumber).asMap().values());
        }
        return sets.build();
    }

    private long determineDay(ShardRange temporalRange)
    {
        Tuple min = temporalRange.getMinTuple();
        Tuple max = temporalRange.getMaxTuple();

        verify(temporalColumnType.isPresent());
        verify(getOnlyElement(min.getTypes()).equals(temporalColumnType.get()));
        verify(min.getTypes().equals(max.getTypes()));
        Type type = getOnlyElement(min.getTypes());
        verify(type.equals(DATE) || type.equals(TimestampType.TIMESTAMP));

        if (type.equals(DATE)) {
            return ((Integer) getOnlyElement(min.getValues())).longValue();
        }

        Long minValue = shardDayBoundaryTimeZone.get().convertUTCToLocal((Long) getOnlyElement(min.getValues()));
        Long maxValue = shardDayBoundaryTimeZone.get().convertUTCToLocal((Long) getOnlyElement(max.getValues()));
        return determineDay(minValue, maxValue);
    }

    private long determineDay(long rangeStart, long rangeEnd)
    {
        long startDay = Duration.ofMillis(rangeStart).toDays();
        long endDay = Duration.ofMillis(rangeEnd).toDays();
        if (startDay == endDay) {
            return startDay;
        }

        if ((endDay - startDay) > 1) {
            // range spans multiple days, return the first full day
            return startDay + 1;
        }

        // range spans two days, return the day that has the larger time range
        long millisInStartDay = Duration.ofDays(endDay).toMillis() - rangeStart;
        long millisInEndDay = rangeEnd - Duration.ofDays(endDay).toMillis();
        return (millisInStartDay >= millisInEndDay) ? startDay : endDay;
    }
}
