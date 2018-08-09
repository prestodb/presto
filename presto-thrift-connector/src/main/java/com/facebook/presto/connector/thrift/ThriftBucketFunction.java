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
package com.facebook.presto.connector.thrift;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;

public class ThriftBucketFunction
        implements BucketFunction
{
    private int bucketCount;
    private static final int tileQuadkeyColumnNumber = 0;

    public ThriftBucketFunction(int bucketCount)
    {
        this.bucketCount = bucketCount;
    }
    @Override
    public int getBucket(Page page, int position)
    {
        Block singleValueBlock = page.getSingleValuePage(position).getBlock(tileQuadkeyColumnNumber);
        String tileQuadkey = singleValueBlock.getSlice(0, 0, singleValueBlock.getSliceLength(0)).toStringUtf8();
        System.out.println(tileQuadkey);
        // TODO: improve hashing of the tile quadkey
        return tileQuadkey.hashCode() % bucketCount;
    }
}
