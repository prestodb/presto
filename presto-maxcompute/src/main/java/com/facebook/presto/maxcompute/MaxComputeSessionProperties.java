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
package com.facebook.presto.maxcompute;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanProperty;
import static com.facebook.presto.spi.session.PropertyMetadata.integerProperty;

public final class MaxComputeSessionProperties
{
    public static final String SPLIT_LIMIT = "split_limit";
    public static final String IGNORE_PARTITION_CACHE = "ignore_partition_cache";
    public static final String ROWS_PER_SPLIT = "rows_per_split";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public MaxComputeSessionProperties(MaxComputeConfig maxComputeConfig)
    {
        sessionProperties = ImmutableList.of(
                integerProperty(
                        SPLIT_LIMIT,
                        "How many splits when doing table scan",
                        1000,
                        false),
                integerProperty(
                        ROWS_PER_SPLIT,
                        "How many rows in a split",
                        maxComputeConfig.getRowsPerSplit(),
                        false),
                booleanProperty(IGNORE_PARTITION_CACHE,
                        "ignore partition cache",
                        false,
                        false));
    }

    public static int getSplitLimit(ConnectorSession session)
    {
        return session.getProperty(SPLIT_LIMIT, Integer.class);
    }

    public static int getRowsPerSplit(ConnectorSession session)
    {
        return session.getProperty(ROWS_PER_SPLIT, Integer.class);
    }

    public static boolean getIgnorePartitionCache(ConnectorSession session)
    {
        return session.getProperty(IGNORE_PARTITION_CACHE, Boolean.class);
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }
}
