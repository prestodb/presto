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

import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class LongDictionaryStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionaryStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;
    private boolean[] nullVector = new boolean[0];

    private InputStreamSource<LongInputStream> dictionaryDataStreamSource = missingStreamSource(LongInputStream.class);
    private int dictionarySize;
    private long[] dictionary = new long[0];

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream inDictionaryStream;
    private boolean[] inDictionaryVector = new boolean[0];

    private InputStreamSource<LongInputStream> dataStreamSource;
    @Nullable
    private LongInputStream dataStream;
    private long[] dataVector = new long[0];

    private boolean dictionaryOpen;
    private boolean rowGroupOpen;

    private LocalMemoryContext systemMemoryContext;

    public LongDictionaryStreamReader(StreamDescriptor streamDescriptor, LocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock(Type type)
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

            if (inDictionaryStream != null) {
                inDictionaryStream.skip(readOffset);
            }

            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                dataStream.skip(readOffset);
            }
        }

        BlockBuilder builder = type.createBlockBuilder(null, nextBatchSize);

        if (presentStream == null) {
            // Data doesn't have nulls
            if (dataStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            if (inDictionaryStream == null) {
                for (int i = 0; i < nextBatchSize; i++) {
                    type.writeLong(builder, dictionary[((int) dataStream.next())]);
                }
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    long id = dataStream.next();
                    if (inDictionaryStream.nextBit()) {
                        type.writeLong(builder, dictionary[(int) id]);
                    }
                    else {
                        type.writeLong(builder, id);
                    }
                }
            }
        }
        else {
            // Data has nulls
            if (dataStream == null) {
                // The only valid case for dataStream is null when data has nulls is that all values are nulls.
                int nullValues = presentStream.getUnsetBits(nextBatchSize);
                if (nullValues != nextBatchSize) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                for (int i = 0; i < nextBatchSize; i++) {
                    builder.appendNull();
                }
            }
            else {
                for (int i = 0; i < nextBatchSize; i++) {
                    if (!presentStream.nextBit()) {
                        builder.appendNull();
                    }
                    else {
                        long id = dataStream.next();
                        if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                            type.writeLong(builder, dictionary[(int) id]);
                        }
                        else {
                            type.writeLong(builder, id);
                        }
                    }
                }
            }
        }
        readOffset = 0;
        nextBatchSize = 0;
        return builder.build();
    }

    private void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!dictionaryOpen && dictionarySize > 0) {
            if (dictionary.length < dictionarySize) {
                dictionary = new long[dictionarySize];
            }

            LongInputStream dictionaryStream = dictionaryDataStreamSource.openStream();
            if (dictionaryStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but data stream is not present");
            }
            dictionaryStream.nextLongVector(dictionarySize, dictionary);
        }
        dictionaryOpen = true;

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
    {
        dictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, LongInputStream.class);
        dictionarySize = encoding.get(streamDescriptor.getStreamId())
                .getColumnEncoding(streamDescriptor.getSequence())
                .getDictionarySize();
        dictionaryOpen = false;

        inDictionaryStreamSource = missingStreamSource(BooleanInputStream.class);
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        inDictionaryStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, IN_DICTIONARY, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }

    @Override
    public void close()
    {
        systemMemoryContext.close();
        nullVector = null;
        dataVector = null;
        inDictionaryVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector) + sizeOf(dataVector) + sizeOf(inDictionaryVector);
    }
}
