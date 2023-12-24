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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;

public class CountStatisticsBuilder
        implements StatisticsBuilder
{
    private long nonNullValueCount;
    private long size;
    private long rawSize;

    @Override
    public void addBlock(Type type, Block block)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                nonNullValueCount++;
            }
        }
    }

    @Override
    public void addValue(Type type, Block block, int position)
    {
        if (!block.isNull(position)) {
            nonNullValueCount++;
        }
    }

    public void addValue()
    {
        nonNullValueCount++;
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        return new ColumnStatistics(nonNullValueCount, null);
    }

    @Override
    public void incrementRawSize(long rawSize)
    {
        this.rawSize += rawSize;
    }

    @Override
    public void incrementSize(long size)
    {
        this.size += size;
    }
}
