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
package com.facebook.presto.orc.writer;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.checkpoint.StreamCheckpoint;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.CompressedMetadataWriter;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MapFlatColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapFlatColumnWriter.class).instanceSize();
    private final int column;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final CompressedMetadataWriter metadataWriter;

    private boolean closed;

    public MapFlatColumnWriter(int column, ColumnWriterOptions columnWriterOptions, Optional<DwrfDataEncryptor> dwrfEncryptor, MetadataWriter metadataWriter)
    {
        // TODO: Flat map needs to get factories for key and value writers, instead of already initialized writers
        checkArgument(column >= 0, "column is negative");
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        this.column = column;
        this.compressed = columnWriterOptions.getCompressionKind() != NONE;
        this.columnEncoding = new ColumnEncoding(DWRF_MAP_FLAT, 0); // TODO Use c'tor with additionalSequenceEncodings
        this.metadataWriter = new CompressedMetadataWriter(metadataWriter, columnWriterOptions, dwrfEncryptor);
    }

    @Override
    public List<ColumnWriter> getNestedColumnWriters()
    {
        // TODO: implement me
        return ImmutableList.of();
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        ImmutableMap.Builder<Integer, ColumnEncoding> encodings = ImmutableMap.builder();
        encodings.put(column, columnEncoding);
        // TODO: Add key/value encodings
        return encodings.build();
    }

    @Override
    public void beginRowGroup()
    {
        // TODO: implement me
    }

    @Override
    public long writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        ColumnarMap columnarMap = toColumnarMap(block);
        return writeColumnarMap(columnarMap);
    }

    private long writeColumnarMap(ColumnarMap columnarMap)
    {
        // TODO: implement me
        return 0;
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        // TODO: implement me
        return ImmutableMap.of();
    }

    @Override
    public void close()
    {
        // TODO: implement me
        closed = true;
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        // TODO: implement me
        return ImmutableMap.of();
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(Optional<List<? extends StreamCheckpoint>> prependCheckpoints)
            throws IOException
    {
        checkState(closed);
        // TODO: implement me
        return ImmutableList.of();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);
        // TODO: implement me
        return ImmutableList.of();
    }

    @Override
    public long getBufferedBytes()
    {
        // TODO: implement me
        return 0;
    }

    @Override
    public long getRetainedBytes()
    {
        // TODO: implement me
        return INSTANCE_SIZE;
    }

    @Override
    public void reset()
    {
        // TODO: implement me
        closed = false;
    }
}
