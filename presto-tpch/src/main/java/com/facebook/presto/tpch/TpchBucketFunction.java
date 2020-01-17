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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.Math.toIntExact;

public class TpchBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final long rowsPerBucket;

    public TpchBucketFunction(int bucketCount, long rowsPerBucket)
    {
        this.bucketCount = bucketCount;
        this.rowsPerBucket = rowsPerBucket;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        Block block = page.getBlock(0);
        if (block.isNull(position)) {
            return 0;
        }

        long orderKey = BIGINT.getLong(block, position);
        long rowNumber = rowNumberFromOrderKey(orderKey);
        int bucket = toIntExact(rowNumber / rowsPerBucket);

        // due to rounding, the last bucket has extra rows
        if (bucket >= bucketCount) {
            bucket = bucketCount - 1;
        }
        return bucket;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .add("rowsPerBucket", rowsPerBucket)
                .toString();
    }

    private static long rowNumberFromOrderKey(long orderKey)
    {
        // remove bits 3 and 4
        return (((orderKey & ~(0b11_111)) >>> 2) | orderKey & 0b111) - 1;
    }
}
