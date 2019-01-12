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
package io.prestosql.plugin.raptor.legacy;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.plugin.raptor.legacy.storage.StorageManagerConfig;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;

public class RaptorSessionProperties
{
    private static final String EXTERNAL_BATCH_ID = "external_batch_id";

    private static final String READER_MAX_MERGE_DISTANCE = "reader_max_merge_distance";
    private static final String READER_MAX_READ_SIZE = "reader_max_read_size";
    private static final String READER_STREAM_BUFFER_SIZE = "reader_stream_buffer_size";
    private static final String READER_TINY_STRIPE_THRESHOLD = "reader_tiny_stripe_threshold";
    private static final String READER_LAZY_READ_SMALL_RANGES = "reader_lazy_read_small_ranges";
    private static final String ONE_SPLIT_PER_BUCKET_THRESHOLD = "one_split_per_bucket_threshold";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public RaptorSessionProperties(StorageManagerConfig config)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        EXTERNAL_BATCH_ID,
                        "Two-phase commit batch ID",
                        null,
                        true),
                dataSizeSessionProperty(
                        READER_MAX_MERGE_DISTANCE,
                        "Reader: Maximum size of gap between two reads to merge into a single read",
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
                        false),
                dataSizeSessionProperty(
                        READER_TINY_STRIPE_THRESHOLD,
                        "Reader: Threshold below which an ORC stripe or file will read in its entirety",
                        config.getOrcTinyStripeThreshold(),
                        false),
                booleanProperty(
                        READER_LAZY_READ_SMALL_RANGES,
                        "Experimental: Reader: Read small file segments lazily",
                        config.isOrcLazyReadSmallRanges(),
                        false),
                integerProperty(
                        ONE_SPLIT_PER_BUCKET_THRESHOLD,
                        "Experimental: Maximum bucket count at which to produce multiple splits per bucket",
                        config.getOneSplitPerBucketThreshold(),
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

    public static DataSize getReaderTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(READER_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static boolean isReaderLazyReadSmallRanges(ConnectorSession session)
    {
        return session.getProperty(READER_LAZY_READ_SMALL_RANGES, Boolean.class);
    }

    public static int getOneSplitPerBucketThreshold(ConnectorSession session)
    {
        return session.getProperty(ONE_SPLIT_PER_BUCKET_THRESHOLD, Integer.class);
    }

    public static PropertyMetadata<DataSize> dataSizeSessionProperty(String name, String description, DataSize defaultValue, boolean hidden)
    {
        return new PropertyMetadata<>(
                name,
                description,
                createUnboundedVarcharType(),
                DataSize.class,
                defaultValue,
                hidden,
                value -> DataSize.valueOf((String) value),
                DataSize::toString);
    }
}
