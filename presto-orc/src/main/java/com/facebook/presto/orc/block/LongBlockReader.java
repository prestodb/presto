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
package com.facebook.presto.orc.block;

import com.facebook.presto.orc.StreamDescriptor;
import com.facebook.presto.orc.metadata.ColumnEncoding;
import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.orc.stream.StreamSources;
import com.facebook.presto.spi.block.BlockBuilder;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LongBlockReader
        implements BlockReader
{
    private final StreamDescriptor streamDescriptor;

    private final LongDirectBlockReader directReader;

    private final LongDictionaryBlockReader dictionaryReader;
    private BlockReader currentReader;

    public LongBlockReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
        directReader = new LongDirectBlockReader(streamDescriptor);
        dictionaryReader = new LongDictionaryBlockReader(streamDescriptor);
    }

    @Override
    public boolean readNextValueInto(BlockBuilder builder, boolean skipNull)
            throws IOException
    {
        return currentReader.readNextValueInto(builder, skipNull);
    }

    @Override
    public void skip(int skipSize)
            throws IOException
    {
        currentReader.skip(skipSize);
    }

    @Override
    public void openStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        ColumnEncodingKind kind = encoding.get(streamDescriptor.getStreamId()).getColumnEncodingKind();
        if (kind == DIRECT || kind == DIRECT_V2 || kind == DWRF_DIRECT) {
            currentReader = directReader;
        }
        else if (kind == DICTIONARY || kind == DICTIONARY_V2) {
            currentReader = dictionaryReader;
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding " + kind);
        }

        currentReader.openStripe(dictionaryStreamSources, encoding);
    }

    @Override
    public void openRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        currentReader.openRowGroup(dataStreamSources);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
