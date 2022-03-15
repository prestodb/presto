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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.predicate.TupleDomainFilter;
import com.facebook.presto.orc.OrcLocalMemoryContext;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.reader.LongDictionaryProvider.DictionaryResult;
import com.facebook.presto.orc.stream.BooleanInputStream;
import com.facebook.presto.orc.stream.InputStreamSource;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.orc.stream.LongInputStream;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;

import static com.facebook.presto.common.array.Arrays.ensureCapacity;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.IN_DICTIONARY;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.PRESENT;
import static com.facebook.presto.orc.reader.SelectiveStreamReaders.initializeOutputPositions;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getBooleanMissingStreamSource;
import static com.facebook.presto.orc.stream.MissingInputStreamSource.getLongMissingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;

public class LongDictionarySelectiveStreamReader
        extends AbstractLongSelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionarySelectiveStreamReader.class).instanceSize();

    // filter evaluation status for dictionary entries; using byte instead of enum for memory efficiency
    private static final byte FILTER_NOT_EVALUATED = 0;
    private static final byte FILTER_PASSED = 1;
    private static final byte FILTER_FAILED = 2;

    private final OrcLocalMemoryContext systemMemoryContext;

    private InputStreamSource<BooleanInputStream> presentStreamSource = getBooleanMissingStreamSource();
    @Nullable
    private BooleanInputStream presentStream;
    @Nullable
    private TupleDomainFilter rowGroupFilter;

    private LongDictionaryProvider dictionaryProvider;
    private int dictionarySize;
    private boolean isDictionaryOwner;
    private long[] dictionary;
    private byte[] dictionaryFilterStatus;

    private InputStreamSource<BooleanInputStream> inDictionaryStreamSource = getBooleanMissingStreamSource();
    @Nullable
    private BooleanInputStream inDictionaryStream;

    private InputStreamSource<LongInputStream> dataStreamSource;
    @Nullable
    private LongInputStream dataStream;

    private boolean dictionaryOpen;
    private boolean rowGroupOpen;
    private int readOffset;

    public LongDictionarySelectiveStreamReader(SelectiveReaderContext context)
    {
        super(context);

        this.systemMemoryContext = context.getSystemMemoryContext().newOrcLocalMemoryContext(this.getClass().getSimpleName());
        this.isDictionaryOwner = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount)
            throws IOException
    {
        checkArgument(positionCount > 0, "positionCount must be greater than zero");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        prepareNextRead(positionCount, presentStream != null && context.isNullsAllowed());

        outputPositions = initializeOutputPositions(outputPositions, positions, positionCount);

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        // TODO In case of all nulls, the stream type will be LongDirect
        int streamPosition;
        if (rowGroupFilter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            streamPosition = readWithFilter(positions, positionCount);
        }

        readOffset = offset + streamPosition;

        return outputPositionCount;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        // no filter implies output is required
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
                values[i] = 0;
            }
            else {
                long value = dataStream.next();
                if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                    value = dictionary[(int) value];
                }
                values[i] = value;
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }

            streamPosition++;
        }

        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount)
            throws IOException
    {
        outputPositionCount = 0;
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if ((context.isNonDeterministicFilter() && rowGroupFilter.testNull()) || context.isNullsAllowed()) {
                    if (context.isOutputRequired()) {
                        nulls[outputPositionCount] = true;
                        values[outputPositionCount] = 0;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                boolean filterPassed;
                long value = dataStream.next();
                if (inDictionaryStream == null || inDictionaryStream.nextBit()) {
                    int id = (int) value;
                    value = dictionary[id];

                    if (dictionaryFilterStatus != null) {
                        if (dictionaryFilterStatus[id] == FILTER_NOT_EVALUATED) {
                            if (rowGroupFilter.testLong(value)) {
                                dictionaryFilterStatus[id] = FILTER_PASSED;
                            }
                            else {
                                dictionaryFilterStatus[id] = FILTER_FAILED;
                            }
                        }
                        filterPassed = dictionaryFilterStatus[id] == FILTER_PASSED;
                    }
                    else {
                        filterPassed = rowGroupFilter.testLong(value);
                    }
                }
                else {
                    filterPassed = rowGroupFilter.testLong(value);
                }
                if (filterPassed) {
                    if (context.isOutputRequired()) {
                        values[outputPositionCount] = value;
                        if (presentStream != null && context.isNullsAllowed()) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }

            streamPosition++;

            outputPositionCount -= rowGroupFilter.getPrecedingPositionsToFail();

            int succeedingPositionsToFail = rowGroupFilter.getSucceedingPositionsToFail();
            if (succeedingPositionsToFail > 0) {
                int positionsToSkip = 0;
                for (int j = 0; j < succeedingPositionsToFail; j++) {
                    i++;
                    int nextPosition = positions[i];
                    positionsToSkip += 1 + nextPosition - streamPosition;
                    streamPosition = nextPosition + 1;
                }
                skip(positionsToSkip);
            }
        }

        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(dataToSkip);
            }
            if (dataStream != null) {
                dataStream.skip(dataToSkip);
            }
        }
        else {
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(items);
            }
            dataStream.skip(items);
        }
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        rowGroupFilter = context.getRowGroupFilter(presentStream);

        // read the dictionary
        if (!dictionaryOpen && dictionarySize > 0) {
            DictionaryResult dictionaryResult = dictionaryProvider.getDictionary(context.getStreamDescriptor(), dictionary, dictionarySize);
            dictionary = dictionaryResult.dictionaryBuffer();
            isDictionaryOwner = dictionaryResult.isBufferOwner();
            if (!context.isLowMemory() && rowGroupFilter != null && !context.isNonDeterministicFilter()) {
                dictionaryFilterStatus = ensureCapacity(dictionaryFilterStatus, dictionarySize);
                Arrays.fill(dictionaryFilterStatus, 0, dictionarySize, FILTER_NOT_EVALUATED);
            }
            else {
                dictionaryFilterStatus = null;
            }
        }
        dictionaryOpen = true;

        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(context.isOutputRequired(), "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        return buildOutputBlock(positions, positionCount, context.isNullsAllowed() && presentStream != null);
    }

    @Override
    public BlockLease getBlockView(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(context.isOutputRequired(), "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        return buildOutputBlockView(positions, positionCount, context.isNullsAllowed() && presentStream != null);
    }

    @Override
    public void startStripe(ZoneId timeZone, Stripe stripe)
    {
        dictionaryProvider = stripe.getLongDictionaryProvider();
        dictionarySize = stripe.getColumnEncodings().get(context.getStreamDescriptor().getStreamId())
                .getColumnEncoding(context.getStreamDescriptor().getSequence())
                .getDictionarySize();
        dictionaryOpen = false;

        inDictionaryStreamSource = getBooleanMissingStreamSource();
        presentStreamSource = getBooleanMissingStreamSource();
        dataStreamSource = getLongMissingStreamSource();

        readOffset = 0;

        presentStream = null;
        rowGroupFilter = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(context.getStreamDescriptor(), PRESENT, BooleanInputStream.class);
        inDictionaryStreamSource = dataStreamSources.getInputStreamSource(context.getStreamDescriptor(), IN_DICTIONARY, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(context.getStreamDescriptor(), DATA, LongInputStream.class);

        readOffset = 0;
        presentStream = null;
        rowGroupFilter = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(context.getStreamDescriptor())
                .toString();
    }

    @Override
    public void close()
    {
        values = null;
        nulls = null;
        outputPositions = null;
        dictionary = null;
        dictionaryFilterStatus = null;

        dataStreamSource = null;
        dataStream = null;

        systemMemoryContext.close();
    }

    // The current memory accounting for shared dictionaries is correct because dictionaries
    // are shared only for flatmap stream readers. Flatmap stream readers are destroyed and recreated
    // every stripe, and so are the dictionary providers. Hence, it's impossible to have a reference
    // to shared dictionaries across different stripes at the same time.
    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                (isDictionaryOwner ? sizeOf(dictionary) : 0) +
                sizeOf(dictionaryFilterStatus) +
                super.getRetainedSizeInBytes();
    }
}
