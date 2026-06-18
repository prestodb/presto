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

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

class DictionaryRowGroup
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DictionaryRowGroup.class).instanceSize();

    private final byte[][] byteSegments;
    private final short[][] shortSegments;
    private final int[][] intSegments;
    private final ColumnStatistics columnStatistics;

    public DictionaryRowGroup(byte[][] byteSegments, short[][] shortSegments, int[][] intSegments, ColumnStatistics columnStatistics)
    {
        requireNonNull(columnStatistics, "columnStatistics is null");
        checkArgument(byteSegments != null || shortSegments != null || intSegments != null, "All segments are null");
        this.byteSegments = byteSegments;
        this.shortSegments = shortSegments;
        this.intSegments = intSegments;
        this.columnStatistics = columnStatistics;
    }

    public byte[][] getByteSegments()
    {
        return byteSegments;
    }

    public short[][] getShortSegments()
    {
        return shortSegments;
    }

    public int[][] getIntSegments()
    {
        return intSegments;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return columnStatistics;
    }

    public long getShallowRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                sizeOf(byteSegments) +
                sizeOf(shortSegments) +
                sizeOf(intSegments) +
                columnStatistics.getRetainedSizeInBytes();
    }
}
