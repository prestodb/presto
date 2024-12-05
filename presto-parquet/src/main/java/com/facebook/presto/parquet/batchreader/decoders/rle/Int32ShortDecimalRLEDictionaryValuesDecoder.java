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

package com.facebook.presto.parquet.batchreader.decoders.rle;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.ShortDecimalValuesDecoder;
import com.facebook.presto.parquet.dictionary.IntegerDictionary;

import java.io.IOException;
import java.io.InputStream;

public class Int32ShortDecimalRLEDictionaryValuesDecoder
        extends Int32RLEDictionaryValuesDecoder
        implements ShortDecimalValuesDecoder
{
    public Int32ShortDecimalRLEDictionaryValuesDecoder(int bitWidth, InputStream in, IntegerDictionary dictionary)
    {
        super(bitWidth, in, dictionary);
    }

    @Override
    public void readNext(long[] values, int offset, int length) throws IOException
    {
        int[] tempValues = new int[length];
        super.readNext(tempValues, 0, length);
        for (int i = 0; i < length; i++) {
            values[offset + i] = tempValues[i];
        }
    }
}
