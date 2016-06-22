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

import com.facebook.presto.raptor.metadata.ShardMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimaps;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;

import static com.facebook.presto.spi.type.DateType.DATE;

public class ShardOrganizerUtil
{
    private ShardOrganizerUtil() {}

    public static Collection<Collection<ShardMetadata>> getShardsByDaysBuckets(Set<ShardMetadata> shardMetadata, Type type)
    {
        // bucket shards by the start day
        ImmutableMultimap.Builder<Long, ShardMetadata> shardsByDays = ImmutableMultimap.builder();

        // skip shards that do not have temporal information
        shardMetadata.stream()
                .filter(shard -> shard.getRangeStart().isPresent() && shard.getRangeEnd().isPresent())
                .forEach(shard -> {
                    long day = determineDay(shard.getRangeStart().getAsLong(), shard.getRangeEnd().getAsLong(), type);
                    shardsByDays.put(day, shard);
                });
        Collection<Collection<ShardMetadata>> byDays = shardsByDays.build().asMap().values();

        ImmutableList.Builder<Collection<ShardMetadata>> sets = ImmutableList.builder();
        for (Collection<ShardMetadata> shards : byDays) {
            sets.addAll(Multimaps.index(shards, ShardMetadata::getBucketNumber).asMap().values());
        }
        return sets.build();
    }

    private static long determineDay(long rangeStart, long rangeEnd, Type type)
    {
        if (type.equals(DATE)) {
            return rangeStart;
        }

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
