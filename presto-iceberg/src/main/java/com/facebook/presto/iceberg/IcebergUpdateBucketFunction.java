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
package com.facebook.presto.iceberg;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.BucketFunction;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class IcebergUpdateBucketFunction
        implements BucketFunction
{
    private final int bucketCount;

    public IcebergUpdateBucketFunction(int bucketCount)
    {
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        Block row = page.getBlock(0).getBlock(position); // TODO #20578: WIP - temporary implementation
        Slice value = VARCHAR.getSlice(row, 0); // file path field of row ID
        return (value.hashCode() & Integer.MAX_VALUE) % bucketCount;
    }
}
