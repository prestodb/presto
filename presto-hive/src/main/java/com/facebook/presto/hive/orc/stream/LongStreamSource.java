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

import com.google.common.io.ByteSource;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.ColumnEncoding.Kind;

import java.io.IOException;

public class LongStreamSource
        implements StreamSource<LongStream>
{
    private final ByteSource byteSource;
    private final ColumnEncoding.Kind encoding;
    private final boolean signed;
    private final int valueSkipSize;

    public LongStreamSource(ByteSource byteSource, ColumnEncoding.Kind encoding, boolean signed, int valueSkipSize)
    {
        this.byteSource = byteSource;
        this.encoding = encoding;
        this.signed = signed;
        this.valueSkipSize = valueSkipSize;
    }

    @Override
    public LongStream openStream()
            throws IOException
    {
        LongStream integerStream;
        if (encoding == Kind.DIRECT_V2 || encoding == Kind.DICTIONARY_V2) {
            integerStream = new LongStreamV2(byteSource.openStream(), signed, false);
        }
        else {
            integerStream = new LongStreamV1(byteSource.openStream(), signed);
        }
        if (valueSkipSize > 0) {
            integerStream.skip(valueSkipSize);
        }
        return integerStream;
    }
}
