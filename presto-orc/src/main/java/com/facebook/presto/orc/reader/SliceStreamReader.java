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

import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.stream.InputStreamSources;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.io.Closer;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.facebook.presto.spi.type.Chars.byteCountWithoutTrailingSpace;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Varchars.byteCount;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SliceStreamReader
        implements StreamReader
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceStreamReader.class).instanceSize();

    private final StreamDescriptor streamDescriptor;
    private final SliceDirectStreamReader directReader;
    private final SliceDictionaryStreamReader dictionaryReader;
    private StreamReader currentReader;

    public SliceStreamReader(StreamDescriptor streamDescriptor, AggregatedMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        directReader = new SliceDirectStreamReader(streamDescriptor);
        dictionaryReader = new SliceDictionaryStreamReader(streamDescriptor, systemMemoryContext.newLocalMemoryContext(SliceStreamReader.class.getSimpleName()));
    }

    @Override
    public Block readBlock(Type type)
            throws IOException
    {
        return currentReader.readBlock(type);
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        currentReader.prepareNextRead(batchSize);
    }

    @Override
    public void startStripe(InputStreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind columnEncodingKind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (columnEncodingKind == DIRECT || columnEncodingKind == DIRECT_V2 || columnEncodingKind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (columnEncodingKind == DICTIONARY || columnEncodingKind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + columnEncodingKind);
        }

        currentReader.startStripe(dictionaryStreamSources, encoding);
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

    public static int computeTruncatedLength(Slice slice, int offset, int length, Type type)
    {
        // calculate truncated length
        int truncatedLength = length;
        if (isVarcharType(type)) {
            VarcharType varcharType = (VarcharType) type;
            int codePointCount = varcharType.isUnbounded() ? length : varcharType.getLengthSafe();
            truncatedLength = byteCount(slice, offset, length, codePointCount);
        }
        else if (isCharType(type)) {
            // truncate the characters and then remove the trailing white spaces
            truncatedLength = byteCountWithoutTrailingSpace(slice, offset, length, ((CharType) type).getLength());
        }
        return truncatedLength;
    }

    @Override
    public void close()
    {
        try (Closer closer = Closer.create()) {
            closer.register(() -> directReader.close());
            closer.register(() -> dictionaryReader.close());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + directReader.getRetainedSizeInBytes() + dictionaryReader.getRetainedSizeInBytes();
    }
}
