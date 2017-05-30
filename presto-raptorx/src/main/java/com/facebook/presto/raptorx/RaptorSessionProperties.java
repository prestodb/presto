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
package com.facebook.presto.raptorx;

import com.facebook.presto.raptorx.storage.StorageConfig;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.session.PropertyMetadata;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.raptorx.util.PropertyUtil.dataSizeProperty;

public class RaptorSessionProperties
{
    private static final String READER_MAX_MERGE_DISTANCE = "reader_max_merge_distance";
    private static final String READER_MAX_READ_SIZE = "reader_max_read_size";
    private static final String READER_STREAM_BUFFER_SIZE = "reader_stream_buffer_size";
    private static final String READER_TINY_STRIPE_THRESHOLD = "reader_tiny_stripe_threshold";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public RaptorSessionProperties(StorageConfig config)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(dataSizeProperty(
                        READER_MAX_MERGE_DISTANCE,
                        "Reader: Maximum size of gap between two reads to merge into a single read",
                        config.getOrcMaxMergeDistance()))
                .add(dataSizeProperty(
                        READER_MAX_READ_SIZE,
                        "Reader: Maximum size of a single read",
                        config.getOrcMaxReadSize()))
                .add(dataSizeProperty(
                        READER_STREAM_BUFFER_SIZE,
                        "Reader: Size of buffer for streaming reads",
                        config.getOrcStreamBufferSize()))
                .add(dataSizeProperty(
                        READER_TINY_STRIPE_THRESHOLD,
                        "Reader: Threshold below which an ORC stripe or file will read in its entirety",
                        config.getOrcTinyStripeThreshold()))
                .build();
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
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

    public static DataSize getReaderTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(READER_TINY_STRIPE_THRESHOLD, DataSize.class);
    }
}
