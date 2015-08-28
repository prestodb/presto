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

import com.facebook.presto.orc.SliceVector;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Vector;
import com.facebook.presto.orc.block.BlockReader;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanStream;
import com.facebook.presto.orc.stream.StreamSource;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.orc.block.BlockReaders.createBlockReader;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.OrcReaderUtils.castOrcVector;
import static com.facebook.presto.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class BlockStreamReader
        implements StreamReader
{
    private final StreamDescriptor streamDescriptor;
    private final BlockReader blockReader;

    private boolean stripeOpen;
    private boolean rowGroupOpen;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;

    private final boolean[] isNullVector = new boolean[Vector.MAX_VECTOR_LENGTH];

    private int readOffset;
    private int nextBatchSize;

    @Nullable
    private StreamSources dictionaryStreamSources;
    @Nullable
    private StreamSources dataStreamSources;

    private List<ColumnEncoding> encoding;

    public BlockStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, Type type)
    {
        this.streamDescriptor = checkNotNull(streamDescriptor, "stream is null");
        this.blockReader = createBlockReader(streamDescriptor, false, hiveStorageTimeZone, type);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public void readBatch(Object vector)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }

            blockReader.skip(readOffset);
        }

        SliceVector sliceVector = castOrcVector(vector, SliceVector.class);
        if (presentStream != null) {
            presentStream.getUnsetBits(nextBatchSize, isNullVector);
        }

        for (int i = 0; i < nextBatchSize; i++) {
            if (!isNullVector[i]) {
                blockReader.readNextValueInto(null, false);

                sliceVector.vector[i] = blockReader.toSlice();
            }
            else {
                sliceVector.vector[i] = null;
            }
        }

        readOffset = 0;
        nextBatchSize = 0;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();

        if (!stripeOpen) {
            blockReader.openStripe(dictionaryStreamSources, encoding);
        }

        blockReader.openRowGroup(dataStreamSources);

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        this.dictionaryStreamSources = dictionaryStreamSources;
        this.dataStreamSources = null;
        this.encoding = encoding;

        presentStreamSource = missingStreamSource(BooleanStream.class);

        stripeOpen = false;
        rowGroupOpen = false;

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        this.dataStreamSources = dataStreamSources;

        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);

        rowGroupOpen = false;

        readOffset = 0;
        nextBatchSize = 0;

        Arrays.fill(isNullVector, false);

        presentStream = null;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
