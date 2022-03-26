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
package com.facebook.presto.parquet;

import java.util.Optional;

public abstract class DataPage
        extends Page
{
    protected final int valueCount;
    private final long firstRowIndex;

    public DataPage(int compressedSize, int uncompressedSize, int valueCount)
    {
        this(compressedSize, uncompressedSize, valueCount, -1);
    }

    DataPage(int compressedSize, int uncompressedSize, int valueCount, long firstRowIndex)
    {
        super(compressedSize, uncompressedSize);
        this.valueCount = valueCount;
        this.firstRowIndex = firstRowIndex;
    }

    /**
     * @return the index of the first row in this page if the related data is available (the optional column-index
     *         contains this value)
     */
    public Optional<Long> getFirstRowIndex()
    {
        return firstRowIndex < 0 ? Optional.empty() : Optional.of(firstRowIndex);
    }

    public int getValueCount()
    {
        return valueCount;
    }
}
