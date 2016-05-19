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
package com.facebook.presto.raptor;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkArgument;

public class RaptorBucketFunction
        implements BucketFunction
{
    private final int bucketCount;

    public RaptorBucketFunction(int bucketCount)
    {
        checkArgument(bucketCount > 0, "bucketCount must be at least one");
        this.bucketCount = bucketCount;
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            long value = BIGINT.getLong(block, position);
            hash = (hash * 31) + XxHash64.hash(value);
        }
        int value = (int) (hash & Integer.MAX_VALUE);
        return value % bucketCount;
    }
}
