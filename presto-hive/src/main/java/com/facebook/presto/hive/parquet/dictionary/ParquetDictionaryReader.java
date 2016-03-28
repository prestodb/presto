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
package com.facebook.presto.hive.parquet.dictionary;

import parquet.bytes.BytesUtils;
import parquet.column.values.ValuesReader;
import parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import parquet.io.ParquetDecodingException;
import parquet.io.api.Binary;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;

public class ParquetDictionaryReader
    extends ValuesReader
{
    private final ParquetDictionary dictionary;
    private RunLengthBitPackingHybridDecoder decoder;

    public ParquetDictionaryReader(ParquetDictionary dictionary)
    {
        this.dictionary = dictionary;
    }

    @Override
    public void initFromPage(int valueCount, byte[] page, int offset)
        throws IOException
    {
        checkArgument(page.length > offset, "Attempt to read offset not in the Parquet page");
        ByteArrayInputStream in = new ByteArrayInputStream(page, offset, page.length - offset);
        int bitWidth = BytesUtils.readIntLittleEndianOnOneByte(in);
        decoder = new RunLengthBitPackingHybridDecoder(bitWidth, in);
    }

    @Override
    public int readValueDictionaryId()
    {
        try {
            return decoder.readInt();
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public Binary readBytes()
    {
        try {
            return dictionary.decodeToBinary(decoder.readInt());
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public float readFloat()
    {
        try {
            return dictionary.decodeToFloat(decoder.readInt());
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public double readDouble()
    {
        try {
            return dictionary.decodeToDouble(decoder.readInt());
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public int readInteger()
    {
        try {
            return dictionary.decodeToInt(decoder.readInt());
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public long readLong()
    {
        try {
            return dictionary.decodeToLong(decoder.readInt());
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }

    @Override
    public void skip()
    {
        try {
            decoder.readInt();
        }
        catch (IOException e) {
            throw new ParquetDecodingException(e);
        }
    }
}
