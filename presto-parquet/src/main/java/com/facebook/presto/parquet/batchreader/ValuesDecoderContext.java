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
package com.facebook.presto.parquet.batchreader;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder;

public class ValuesDecoderContext<V extends ValuesDecoder>
{
    private final V valuesDecoder;
    private final int start;
    private final int end;

    private int nonNullCount;
    private int valueCount;

    public ValuesDecoderContext(V valuesDecoder, int start, int end)
    {
        this.valuesDecoder = valuesDecoder;
        this.start = start;
        this.end = end;
    }

    public V getValuesDecoder()
    {
        return valuesDecoder;
    }

    public int getStart()
    {
        return start;
    }

    public int getEnd()
    {
        return end;
    }

    public int getNonNullCount()
    {
        return nonNullCount;
    }

    public void setNonNullCount(int nonNullCount)
    {
        this.nonNullCount = nonNullCount;
    }

    public int getValueCount()
    {
        return valueCount;
    }

    public void setValueCount(int valueCount)
    {
        this.valueCount = valueCount;
    }
}
