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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.spi.session.PropertyMetadata.booleanSessionProperty;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public final class HiveSessionProperties
{
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String OPTIMIZED_READER_ENABLED = "optimized_reader_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    public static final String MAX_SPLIT_SIZE = "max_split_size";
    public static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HiveSessionProperties(HiveClientConfig config)
    {
        sessionProperties = ImmutableList.of(
                booleanSessionProperty(
                        FORCE_LOCAL_SCHEDULING,
                        "Only schedule splits on workers colocated with data node",
                        config.isForceLocalScheduling(),
                        false),
                booleanSessionProperty(
                        OPTIMIZED_READER_ENABLED,
                        "Enable optimized readers",
                        config.isOptimizedReaderEnabled(),
                        true),
                dataSizeSessionProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between to reads to merge into a single read",
                        config.getOrcMaxMergeDistance(),
                        false),
                dataSizeSessionProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        config.getOrcMaxBufferSize(),
                        false),
                dataSizeSessionProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        config.getOrcMaxBufferSize(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isForceLocalScheduling(ConnectorSession session)
    {
        return session.getProperty(FORCE_LOCAL_SCHEDULING, Boolean.class);
    }

    public static boolean isOptimizedReaderEnabled(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZED_READER_ENABLED, Boolean.class);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_STREAM_BUFFER_SIZE, DataSize.class);
    }

    public static PropertyMetadata<DataSize> dataSizeSessionProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                VARCHAR,
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value));
    }

    public static DataSize getMaxSplitSize(ConnectorSession session, DataSize defaultValue)
    {
        String maxSplitSize = session.getProperties().get(MAX_SPLIT_SIZE);
        if (maxSplitSize == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxSplitSize);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, MAX_SPLIT_SIZE + " is invalid: " + maxSplitSize);
        }
    }

    public static DataSize getMaxInitialSplitSize(ConnectorSession session, DataSize defaultValue)
    {
        String maxInitialSplitSize = session.getProperties().get(MAX_INITIAL_SPLIT_SIZE);
        if (maxInitialSplitSize == null) {
            return defaultValue;
        }

        try {
            return DataSize.valueOf(maxInitialSplitSize);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, MAX_INITIAL_SPLIT_SIZE + " is invalid: " + maxInitialSplitSize);
        }
    }
}
