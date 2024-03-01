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
package com.facebook.presto.druid.segment;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.druid.DruidColumnHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.Streams;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.utils.SerializerUtils;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.SimpleQueryableIndex;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.BitmapSerde;
import org.apache.druid.segment.data.BitmapSerdeFactory;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.Interval;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static org.apache.druid.segment.column.ColumnHolder.TIME_COLUMN_NAME;
import static org.apache.druid.segment.data.GenericIndexed.STRING_STRATEGY;

public class V9SegmentIndexSource
        implements SegmentIndexSource
{
    private static final Logger log = Logger.get(V9SegmentIndexSource.class);

    private static final String INDEX_METADATA_FILE_NAME = "index.drd";
    private static final String SEGMENT_METADATA_FILE_NAME = "metadata.drd";
    private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
    private static final SerializerUtils SERIALIZER_UTILS = new SerializerUtils();

    private final SegmentColumnSource segmentColumnSource;

    public V9SegmentIndexSource(SegmentColumnSource segmentColumnSource)
    {
        this.segmentColumnSource = requireNonNull(segmentColumnSource, "segmentColumnSource is null");
        NullHandling.initializeForTests();
    }

    @Override
    public QueryableIndex loadIndex(List<ColumnHandle> columnHandles)
            throws IOException
    {
        ByteBuffer indexBuffer = ByteBuffer.wrap(segmentColumnSource.getColumnData(INDEX_METADATA_FILE_NAME));
        GenericIndexed.read(indexBuffer, STRING_STRATEGY);
        GenericIndexed<String> allDimensions = GenericIndexed.read(
                indexBuffer,
                STRING_STRATEGY);

        Interval dataInterval = Intervals.utc(indexBuffer.getLong(), indexBuffer.getLong());

        BitmapSerdeFactory segmentBitmapSerdeFactory;

        if (indexBuffer.hasRemaining()) {
            segmentBitmapSerdeFactory = JSON_MAPPER.readValue(SERIALIZER_UTILS.readString(indexBuffer), BitmapSerdeFactory.class);
        }
        else {
            segmentBitmapSerdeFactory = new BitmapSerde.LegacyBitmapSerdeFactory();
        }

        Metadata metadata = null;
        ByteBuffer metadataBuffer = ByteBuffer.wrap(segmentColumnSource.getColumnData(SEGMENT_METADATA_FILE_NAME));
        try {
            metadata = JSON_MAPPER.readValue(SERIALIZER_UTILS.readBytes(metadataBuffer, metadataBuffer.remaining()), Metadata.class);
        }
        catch (JsonParseException | JsonMappingException e) {
            // Any jackson deserialization errors are ignored e.g. if metadata contains some aggregator which
            // is no longer supported then it is OK to not use the metadata instead of failing segment loading
            log.warn(e, "Failed to load metadata for segment");
        }

        Map<String, Supplier<ColumnHolder>> columns = new HashMap<>();
        for (ColumnHandle columnHandle : columnHandles) {
            String columnName = ((DruidColumnHandle) columnHandle).getColumnName();
            columns.put(columnName, () -> createColumnHolder(columnName));
        }

        List<String> availableDimensions = Streams.stream(allDimensions.iterator())
                .filter(columns::containsKey)
                .collect(toImmutableList());

        columns.put(TIME_COLUMN_NAME, () -> createColumnHolder(TIME_COLUMN_NAME));

        Indexed<String> indexed = new ListIndexed<>(availableDimensions);
        // TODO: get rid of the time column by creating Presto's SimpleQueryableIndex impl
        return new SimpleQueryableIndex(
                dataInterval,
                indexed,
                segmentBitmapSerdeFactory.getBitmapFactory(),
                columns,
                null,
                metadata,
                false);
    }

    private ColumnDescriptor readColumnDescriptor(ByteBuffer byteBuffer)
            throws IOException
    {
        return JSON_MAPPER.readValue(SERIALIZER_UTILS.readString(byteBuffer), ColumnDescriptor.class);
    }

    private ColumnHolder createColumnHolder(String columnName)
    {
        try {
            ByteBuffer columnData = ByteBuffer.wrap(segmentColumnSource.getColumnData(columnName));
            ColumnDescriptor columnDescriptor = readColumnDescriptor(columnData);
            return columnDescriptor.read(columnData, () -> 0, null);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }
}
