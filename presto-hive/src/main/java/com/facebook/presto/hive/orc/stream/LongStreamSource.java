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
package com.facebook.presto.hive.orc.stream;

import com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.facebook.presto.hive.orc.metadata.OrcType.OrcTypeKind;
import com.google.common.base.Objects;
import com.google.common.io.ByteSource;

import java.io.IOException;

import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.facebook.presto.hive.orc.metadata.ColumnEncoding.ColumnEncodingKind.DWRF_DIRECT;

public class LongStreamSource
        implements StreamSource<LongStream>
{
    private final ByteSource byteSource;
    private final ColumnEncodingKind encoding;
    private final OrcTypeKind orcTypeKind;
    private final boolean signed;
    private final boolean usesVInt;
    private final int valueSkipSize;

    public LongStreamSource(ByteSource byteSource, ColumnEncodingKind encoding, OrcTypeKind orcTypeKind, boolean signed, boolean usesVInt, int valueSkipSize)
    {
        this.byteSource = byteSource;
        this.encoding = encoding;
        this.orcTypeKind = orcTypeKind;
        this.signed = signed;
        this.usesVInt = usesVInt;
        this.valueSkipSize = valueSkipSize;
    }

    @Override
    public Class<LongStream> getStreamType()
    {
        return LongStream.class;
    }

    @Override
    public LongStream openStream()
            throws IOException
    {
        LongStream integerStream;
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            integerStream = new LongStreamV2(byteSource.openStream(), signed, false);
        }
        else if (encoding == DIRECT || encoding == DICTIONARY) {
            integerStream = new LongStreamV1(byteSource.openStream(), signed);
        }
        else if (encoding == DWRF_DIRECT) {
            integerStream = new LongStreamDwrf(byteSource.openStream(), orcTypeKind, signed, usesVInt);
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
        }
        if (valueSkipSize > 0) {
            integerStream.skip(valueSkipSize);
        }
        return integerStream;
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("byteSource", byteSource)
                .add("encoding", encoding)
                .add("orcTypeKind", orcTypeKind)
                .add("signed", signed)
                .add("usesVInt", usesVInt)
                .add("valueSkipSize", valueSkipSize)
                .toString();
    }
}
