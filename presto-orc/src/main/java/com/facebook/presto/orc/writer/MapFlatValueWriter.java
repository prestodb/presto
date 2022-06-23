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
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.stream.InMapOutputStream;
import com.facebook.presto.orc.stream.StreamDataOutput;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Holds flat map value writer for a certain sequence/key.
 */
class MapFlatValueWriter
{
    private final int nodeIndex;
    private final int sequence;
    private final ColumnWriter valueWriter;
    private final DwrfProto.KeyInfo dwrfKey;
    private final InMapOutputStream inMapStream;

    public MapFlatValueWriter(
            int nodeIndex,
            int sequence,
            DwrfProto.KeyInfo dwrfKey,
            ColumnWriter valueWriter,
            ColumnWriterOptions columnWriterOptions,
            Optional<DwrfDataEncryptor> dwrfEncryptor)
    {
        checkArgument(nodeIndex > 0, "nodeIndex is invalid: %s", nodeIndex);
        checkArgument(sequence > 0, "sequence must be positive: %s", sequence);
        requireNonNull(columnWriterOptions, "columnWriterOptions is null");
        requireNonNull(dwrfEncryptor, "dwrfEncryptor is null");

        this.nodeIndex = nodeIndex;
        this.sequence = sequence;
        this.dwrfKey = requireNonNull(dwrfKey, "dwrfKey is null");
        this.valueWriter = requireNonNull(valueWriter, "valueWriter is null");
        this.inMapStream = new InMapOutputStream(columnWriterOptions, dwrfEncryptor);
    }

    public int getSequence()
    {
        return sequence;
    }

    public ColumnWriter getValueWriter()
    {
        return valueWriter;
    }

    public DwrfProto.KeyInfo getDwrfKey()
    {
        return dwrfKey;
    }

    public void beginRowGroup()
    {
        inMapStream.recordCheckpoint();
        valueWriter.beginRowGroup();
    }

    public long writeSingleEntryBlock(Block block)
    {
        // TODO: Implement correct size
        long size = valueWriter.writeBlock(block);
        inMapStream.writeBoolean(true);
        return size;
    }

    /**
     * Mark a row for this key as missing.
     */
    public long writeNotInMap(int count)
    {
        // TODO: Implement correct size
        inMapStream.writeBooleans(count, false);
        return count;
    }

    /**
     * Catch up on the `not-in-map` rows.
     */
    public void writeNotInMap(IntList rowsInFinishedRowGroups, int rowsInCurrentRowGroup)
    {
        for (int i = 0; i < rowsInFinishedRowGroups.size(); i++) {
            beginRowGroup();
            int rows = rowsInFinishedRowGroups.getInt(i);
            inMapStream.writeBooleans(rows, false);
            valueWriter.finishRowGroup();
        }

        beginRowGroup();
        if (rowsInCurrentRowGroup > 0) {
            inMapStream.writeBooleans(rowsInCurrentRowGroup, false);
        }
    }

    public List<StreamDataOutput> getDataStreams()
    {
        return ImmutableList.<StreamDataOutput>builder()
                .add(inMapStream.getStreamDataOutput(nodeIndex, sequence))
                .addAll(valueWriter.getDataStreams())
                .build();
    }

    public List<StreamDataOutput> getIndexStreams()
            throws IOException
    {
        return valueWriter.getIndexStreams(Optional.of(inMapStream.getCheckpoints()));
    }

    public long getBufferedBytes()
    {
        return valueWriter.getBufferedBytes() + inMapStream.getBufferedBytes();
    }

    public void close()
    {
        inMapStream.close();
        valueWriter.close();
    }
}
