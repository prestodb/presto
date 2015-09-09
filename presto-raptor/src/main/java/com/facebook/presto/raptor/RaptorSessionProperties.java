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
package com.facebook.presto.raptor;

import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.session.PropertyMetadata.stringSessionProperty;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class RaptorSessionProperties
{
    private static final String EXTERNAL_BATCH_ID = "external_batch_id";

    private static final String READER_MAX_MERGE_DISTANCE = "reader_max_merge_distance";
    private static final String READER_MAX_READ_SIZE = "reader_max_read_size";
    private static final String READER_STREAM_BUFFER_SIZE = "reader_stream_buffer_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public RaptorSessionProperties(StorageManagerConfig config)
    {
        sessionProperties = ImmutableList.of(
                stringSessionProperty(
                        EXTERNAL_BATCH_ID,
                        "Two-phase commit batch ID",
                        null,
                        true),
                dataSizeSessionProperty(
                        READER_MAX_MERGE_DISTANCE,
                        "Reader: Maximum size of gap between to reads to merge into a single read",
                        config.getOrcMaxMergeDistance(),
                        false),
                dataSizeSessionProperty(
                        READER_MAX_READ_SIZE,
                        "Reader: Maximum size of a single read",
                        config.getOrcMaxReadSize(),
                        false),
                dataSizeSessionProperty(
                        READER_STREAM_BUFFER_SIZE,
                        "Reader: Size of buffer for streaming reads",
                        config.getOrcStreamBufferSize(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<String> getExternalBatchId(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(EXTERNAL_BATCH_ID, String.class));
    }

    public static DataSize getReaderMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(READER_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getReaderMaxReadSize(ConnectorSession session)
    {
        return session.getProperty(READER_MAX_READ_SIZE, DataSize.class);
    }

    public static DataSize getReaderStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(READER_STREAM_BUFFER_SIZE, DataSize.class);
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
}
