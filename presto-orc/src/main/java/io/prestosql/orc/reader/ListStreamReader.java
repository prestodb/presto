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
package io.prestosql.orc.reader;

import com.google.common.io.Closer;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.StreamDescriptor;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.orc.metadata.Stream.StreamKind.LENGTH;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.StreamReaders.createStreamReader;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class ListStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ListStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;

    private final StreamReader elementStreamReader;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;

    public ListStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        this.elementStreamReader = createStreamReader(streamDescriptor.getNestedStreams().get(0), hiveStorageTimeZone, systemMemoryContext);
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
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                long elementSkipSize = lengthStream.sum(readOffset);
                elementStreamReader.prepareNextRead(toIntExact(elementSkipSize));
            }
        }

        // We will use the offsetVector as the buffer to read the length values from lengthStream,
        // and the length values will be converted in-place to an offset vector.
        int[] offsetVector = new int[nextBatchSize + 1];
        boolean[] nullVector = null;
        if (presentStream == null) {
            if (lengthStream == null) {
                throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
            }
            lengthStream.nextIntVector(nextBatchSize, offsetVector, 0);
        }
        else {
            nullVector = new boolean[nextBatchSize];
            int nullValues = presentStream.getUnsetBits(nextBatchSize, nullVector);
            if (nullValues != nextBatchSize) {
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Value is not null but data stream is not present");
                }
                lengthStream.nextIntVector(nextBatchSize, offsetVector, 0, nullVector);
            }
        }

        // Convert the length values in the offsetVector to offset values in place
        int currentLength = offsetVector[0];
        offsetVector[0] = 0;
        for (int i = 1; i < offsetVector.length; i++) {
            int nextLength = offsetVector[i];
            offsetVector[i] = offsetVector[i - 1] + currentLength;
            currentLength = nextLength;
        }

        Type elementType = type.getTypeParameters().get(0);
        int elementCount = offsetVector[offsetVector.length - 1];

        Block elements;
        if (elementCount > 0) {
            elementStreamReader.prepareNextRead(elementCount);
            elements = elementStreamReader.readBlock(elementType);
        }
        else {
            elements = elementType.createBlockBuilder(null, 0).build();
        }
        Block arrayBlock = ArrayBlock.fromElementBlock(nextBatchSize, Optional.ofNullable(nullVector), offsetVector, elements);

        readOffset = 0;
        nextBatchSize = 0;

        return arrayBlock;
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        lengthStream = null;

        rowGroupOpen = false;

        elementStreamReader.startRowGroup(dataStreamSources);
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
        try (Closer closer = Closer.create()) {
            closer.register(() -> elementStreamReader.close());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + elementStreamReader.getRetainedSizeInBytes();
    }
}
