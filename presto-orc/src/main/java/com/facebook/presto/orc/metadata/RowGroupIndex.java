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
package com.facebook.presto.orc.metadata;

import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import org.openjdk.jol.info.ClassLayout;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class RowGroupIndex
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(RowGroupIndex.class).instanceSize();

    private final int[] positions;
    private final ColumnStatistics statistics;

    public RowGroupIndex(int[] positions, ColumnStatistics statistics)
    {
        this.positions = requireNonNull(positions, "positions is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public int[] getPositions()
    {
        return positions;
    }

    public ColumnStatistics getColumnStatistics()
    {
        return statistics;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(positions) + statistics.getRetainedSizeInBytes();
    }
}
