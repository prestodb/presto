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
package com.facebook.presto.parquet.dictionary;

import com.facebook.presto.parquet.DictionaryPage;
import io.airlift.slice.Slice;
import org.apache.parquet.io.api.Binary;

import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.bytes.BytesUtils.readIntLittleEndian;

public class BinaryDictionary
        extends Dictionary
{
    private final Binary[] content;

    public BinaryDictionary(DictionaryPage dictionaryPage)
            throws IOException
    {
        this(dictionaryPage, null);
    }

    public BinaryDictionary(DictionaryPage dictionaryPage, Integer length)
            throws IOException
    {
        super(dictionaryPage.getEncoding());
        content = new Binary[dictionaryPage.getDictionarySize()];

        byte[] dictionaryBytes;
        int offset;
        Slice dictionarySlice = dictionaryPage.getSlice();
        if (dictionarySlice.hasByteArray()) {
            dictionaryBytes = dictionarySlice.byteArray();
            offset = dictionarySlice.byteArrayOffset();
        }
        else {
            dictionaryBytes = dictionarySlice.getBytes();
            offset = 0;
        }

        if (length == null) {
            for (int i = 0; i < content.length; i++) {
                int len = readIntLittleEndian(dictionaryBytes, offset);
                offset += Integer.BYTES;
                content[i] = Binary.fromByteArray(dictionaryBytes, offset, len);
                offset += len;
            }
        }
        else {
            checkArgument(length > 0, "Invalid byte array length: %s", length);
            for (int i = 0; i < content.length; i++) {
                content[i] = Binary.fromByteArray(dictionaryBytes, offset, length);
                offset += length;
            }
        }
    }

    @Override
    public Binary decodeToBinary(int id)
    {
        return content[id];
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("content", content)
                .toString();
    }
}
