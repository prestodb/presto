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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockDecoder;
import com.facebook.presto.spi.type.BigintType;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PrecomputedHashGenerator
        implements HashGenerator
{
    private final int hashChannel;

    public PrecomputedHashGenerator(int hashChannel)
    {
        this.hashChannel = hashChannel;
    }

    @Override
    public long hashPosition(int position, Page page)
    {
        return BigintType.BIGINT.getLong(page.getBlock(hashChannel), position);
    }

    @Override
    public void getPartitions(int partitionCount, Page page, BlockDecoder decoder, int[] partitionsOut)
    {
        Block block = page.getBlock(hashChannel);
        decoder.decodeBlock(block);
        int positionCount = block.getPositionCount();
        long[] longs = decoder.getValues(long[].class);
        if (decoder.isIdentityMap()) {
            for (int i = 0; i < positionCount; i++) {
                partitionsOut[i] = (int) ((longs[i] & 0x7fffffffffffL) % partitionCount);
            }
        }
        else {
            int[] map = decoder.getRowNumberMap();
            for (int i = 0; i < positionCount; i++) {
                partitionsOut[i] = (int) ((longs[map[i]] & 0x7fffffffffffL) % partitionCount);
            }
        }
        decoder.release();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("hashChannel", hashChannel)
                .toString();
    }
}
