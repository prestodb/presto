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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcRecordReaderOptions;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.stream.InputStreamSources;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_MAP_FLAT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MapSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapSelectiveStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final MapDirectSelectiveStreamReader directReader;
    @Nullable
    private final MapFlatSelectiveStreamReader flatReader;
    private SelectiveStreamReader currentReader;

    public MapSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Map<Subfield, TupleDomainFilter> filters,
            List<Subfield> requiredSubfields,
            Optional<Type> outputType,
            DateTimeZone hiveStorageTimeZone,
            OrcRecordReaderOptions options,
            OrcAggregatedMemoryContext systemMemoryContext,
            boolean isLowMemory)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        directReader = new MapDirectSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, outputType, hiveStorageTimeZone, options, systemMemoryContext, isLowMemory);
        if (streamDescriptor.getSequence() == DEFAULT_SEQUENCE_ID) {
            flatReader = new MapFlatSelectiveStreamReader(streamDescriptor, filters, requiredSubfields, outputType, hiveStorageTimeZone, options, systemMemoryContext);
        }
        else {
            // When sequence id is not DEFAULT_SEQUENCE_ID, this map is inside a flat map.
            // Flat maps can't be nested within another flat map.
            flatReader = null;
        }
    }

    @Override
    public void startStripe(ZoneId timeZone, Stripe stripe)
            throws IOException
    {
        ColumnEncodingKind kind = stripe.getColumnEncodings().get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getColumnEncodingKind();
        if (kind == DIRECT || kind == DIRECT_V2 || kind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (kind == DWRF_MAP_FLAT) {
            if (flatReader == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Flat map reader does not support nesting " + streamDescriptor);
            }
            currentReader = flatReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.startStripe(timeZone, stripe);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        currentReader.startRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + directReader.getRetainedSizeInBytes() +
                (flatReader == null ? 0L : flatReader.getRetainedSizeInBytes());
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        return currentReader.read(offset, positions, positionCount);
    }

    @Override
    public int[] getReadPositions()
    {
        return currentReader.getReadPositions();
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        return currentReader.getBlock(positions, positionCount);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        return currentReader.getBlockView(positions, positionCount);
    }

    @Override
    public void throwAnyError(int[] positions, int positionCount)
    {
        currentReader.throwAnyError(positions, positionCount);
    }

    @Override
    public void close()
    {
        directReader.close();
        if (flatReader != null) {
            flatReader.close();
        }
    }
}
