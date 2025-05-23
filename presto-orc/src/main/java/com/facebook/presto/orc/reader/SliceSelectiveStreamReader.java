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
import com.facebook.presto.common.type.Chars;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.Varchars;
import com.facebook.presto.orc.OrcAggregatedMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.Stripe;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SliceSelectiveStreamReader
        implements SelectiveStreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceSelectiveStreamReader.class).instanceSize();

    private final SelectiveReaderContext context;

    @Nullable
    private SliceDirectSelectiveStreamReader directReader;
    @Nullable
    private SliceDictionarySelectiveReader dictionaryReader;
    private SelectiveStreamReader currentReader;

    public SliceSelectiveStreamReader(
            StreamDescriptor streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            OrcAggregatedMemoryContext systemMemoryContext,
            boolean isLowMemory,
            long maxSliceSize,
            boolean resetAllReaders)
    {
        this.context = new SelectiveReaderContext(streamDescriptor, outputType, filter, systemMemoryContext, isLowMemory, maxSliceSize, resetAllReaders);
    }

    public static int computeTruncatedLength(Slice slice, int offset, int length, int maxCodePointCount, boolean isCharType)
    {
        if (isCharType) {
            // truncate the characters and then remove the trailing white spaces
            return Chars.byteCountWithoutTrailingSpace(slice, offset, length, maxCodePointCount);
        }
        if (maxCodePointCount >= 0 && length > maxCodePointCount) {
            return Varchars.byteCount(slice, offset, length, maxCodePointCount);
        }
        return length;
    }

    @Override
    public void startStripe(Stripe stripe)
            throws IOException
    {
        ColumnEncoding.ColumnEncodingKind kind = stripe.getColumnEncodings().get(context.getStreamDescriptor().getStreamId())
                .getColumnEncoding(context.getStreamDescriptor().getSequence())
                .getColumnEncodingKind();
        switch (kind) {
            case DIRECT:
            case DIRECT_V2:
            case DWRF_DIRECT:
                if (directReader == null) {
                    directReader = new SliceDirectSelectiveStreamReader(context);
                }
                currentReader = directReader;
                if (dictionaryReader != null && context.isResetAllReaders()) {
                    dictionaryReader = null;
                    System.setProperty("RESET_SLICE_READER", "RESET_SLICE_READER");
                }
                break;
            case DICTIONARY:
            case DICTIONARY_V2:
                if (dictionaryReader == null) {
                    dictionaryReader = new SliceDictionarySelectiveReader(context);
                }
                currentReader = dictionaryReader;
                if (directReader != null && context.isResetAllReaders()) {
                    directReader = null;
                    System.setProperty("RESET_SLICE_READER", "RESET_SLICE_READER");
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.startStripe(stripe);
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
                .addValue(context.getStreamDescriptor())
                .toString();
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            if (directReader != null) {
                closer.register(directReader::close);
            }
            if (dictionaryReader != null) {
                closer.register(dictionaryReader::close);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                (directReader == null ? 0L : directReader.getRetainedSizeInBytes()) +
                (dictionaryReader == null ? 0L : dictionaryReader.getRetainedSizeInBytes());
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

    @VisibleForTesting
    public void resetDataStream()
    {
        if (directReader != null) {
            directReader.resetDataStream();
        }
        if (dictionaryReader != null) {
            dictionaryReader.resetDataStream();
        }
    }
}
