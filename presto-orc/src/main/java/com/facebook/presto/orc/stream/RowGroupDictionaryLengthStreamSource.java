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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.google.common.io.ByteSource;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;

public class RowGroupDictionaryLengthStreamSource
        implements StreamSource<RowGroupDictionaryLengthStream>
{
    private final ByteSource byteSource;
    private final ColumnEncodingKind encoding;
    private final boolean signed;
    private final int valueSkipSize;
    private final int entryCount;

    public RowGroupDictionaryLengthStreamSource(ByteSource byteSource, ColumnEncodingKind encoding, boolean signed, int valueSkipSize, int entryCount)
    {
        this.byteSource = byteSource;
        this.encoding = encoding;
        this.signed = signed;
        this.valueSkipSize = valueSkipSize;
        this.entryCount = entryCount;
    }

    public int getEntryCount()
    {
        return entryCount;
    }

    @Override
    public Class<RowGroupDictionaryLengthStream> getStreamType()
    {
        return RowGroupDictionaryLengthStream.class;
    }

    @Override
    public RowGroupDictionaryLengthStream openStream()
            throws IOException
    {
        RowGroupDictionaryLengthStream stream = new RowGroupDictionaryLengthStream(byteSource.openStream(), signed, entryCount);
        if (valueSkipSize > 0) {
            stream.skip(valueSkipSize);
        }
        return stream;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("byteSource", byteSource)
                .add("encoding", encoding)
                .add("signed", signed)
                .add("valueSkipSize", valueSkipSize)
                .add("entryCount", entryCount)
                .toString();
    }
}
