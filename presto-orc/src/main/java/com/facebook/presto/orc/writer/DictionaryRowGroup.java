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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

class DictionaryRowGroup
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryRowGroup.class).instanceSize();

    private final byte[] byteIndexes;
    private final short[] shortIndexes;
    private final int[] intIndexes;
    private final ColumnStatistics columnStatistics;
    private final int valueCount;

    public DictionaryRowGroup(int maxIndex, int[] dictionaryIndexes, int valueCount, ColumnStatistics columnStatistics)
    {
        requireNonNull(dictionaryIndexes, "dictionaryIndexes is null");
        checkArgument(valueCount >= 0, "valueCount is negative");
        requireNonNull(columnStatistics, "columnStatistics is null");

        if (maxIndex <= Byte.MAX_VALUE) {
            this.byteIndexes = new byte[valueCount];
            this.shortIndexes = null;
            this.intIndexes = null;

            for (int i = 0; i < valueCount; i++) {
                byteIndexes[i] = (byte) dictionaryIndexes[i];
            }
        }
        else if (maxIndex <= Short.MAX_VALUE) {
            this.shortIndexes = new short[valueCount];
            this.byteIndexes = null;
            this.intIndexes = null;

            for (int i = 0; i < valueCount; i++) {
                shortIndexes[i] = (short) dictionaryIndexes[i];
            }
        }
        else {
            this.intIndexes = Arrays.copyOf(dictionaryIndexes, valueCount);
            this.byteIndexes = null;
            this.shortIndexes = null;
        }
        this.columnStatistics = columnStatistics;
        this.valueCount = valueCount;
    }

    public byte[] getByteIndexes()
    {
        return byteIndexes;
    }

    public short[] getShortIndexes()
    {
        return shortIndexes;
    }

    public int[] getIntegerIndexes()
    {
        return intIndexes;
    }

    public int getValueCount()
    {
        return valueCount;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return columnStatistics;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(byteIndexes) +
                sizeOf(shortIndexes) +
                sizeOf(intIndexes) +
                columnStatistics.getRetainedSizeInBytes();
    }
}
