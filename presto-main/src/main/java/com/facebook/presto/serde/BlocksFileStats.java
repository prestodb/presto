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
package com.facebook.presto.serde;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public class BlocksFileStats
{
    private final long rowCount;
    private final long runsCount;
    private final long avgRunLength;
    private final int uniqueCount;

    public BlocksFileStats(long rowCount, long runsCount, long avgRunLength, int uniqueCount)
    {
        this.rowCount = rowCount;
        this.runsCount = runsCount;
        this.avgRunLength = avgRunLength;
        this.uniqueCount = uniqueCount;
    }

    public static void serialize(BlocksFileStats stats, SliceOutput sliceOutput)
    {
        // TODO: add a better way of serializing the stats that is less fragile
        sliceOutput.appendLong(stats.getRowCount())
                .appendLong(stats.getRunsCount())
                .appendLong(stats.getAvgRunLength())
                .appendInt(stats.getUniqueCount());
    }

    public static BlocksFileStats deserialize(Slice slice)
    {
        SliceInput input = slice.getInput();
        return deserialize(input);
    }

    public static BlocksFileStats deserialize(SliceInput input)
    {
        long rowCount = input.readLong();
        long runsCount = input.readLong();
        long avgRunLength = input.readLong();
        int uniqueCount = input.readInt();
        return new BlocksFileStats(rowCount, runsCount, avgRunLength, uniqueCount);
    }

    public long getRowCount()
    {
        return rowCount;
    }

    public long getRunsCount()
    {
        return runsCount;
    }

    public long getAvgRunLength()
    {
        return avgRunLength;
    }

    public int getUniqueCount()
    {
        return uniqueCount;
    }
}
