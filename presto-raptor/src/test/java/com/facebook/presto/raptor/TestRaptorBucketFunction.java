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
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static org.testng.Assert.assertEquals;

public class TestRaptorBucketFunction
{
    @Test
    public void testBigint()
            throws Exception
    {
        BucketFunction function = bucketFunction(50, BIGINT);
        assertEquals(getBucket(function, createLongsBlock(123456789012L)), 12);
        assertEquals(getBucket(function, createLongsBlock(454345325)), 16);
        assertEquals(getBucket(function, createLongsBlock(365363)), 42);
        assertEquals(getBucket(function, createLongsBlock(45645747)), 41);
        assertEquals(getBucket(function, createLongsBlock(3244)), 29);

        function = bucketFunction(2, BIGINT);
        assertEquals(getBucket(function, createLongsBlock(123456789012L)), 0);
        assertEquals(getBucket(function, createLongsBlock(454345325)), 0);
        assertEquals(getBucket(function, createLongsBlock(365363)), 0);
        assertEquals(getBucket(function, createLongsBlock(45645747)), 1);
        assertEquals(getBucket(function, createLongsBlock(3244)), 1);
    }

    @Test
    public void testInteger()
    {
        BucketFunction function = bucketFunction(50, INTEGER);
        assertEquals(getBucket(function, createIntsBlock(454345325)), 16);
        assertEquals(getBucket(function, createIntsBlock(365363)), 42);
        assertEquals(getBucket(function, createIntsBlock(45645747)), 41);
        assertEquals(getBucket(function, createIntsBlock(3244)), 29);
    }

    @Test
    public void testVarchar()
    {
        BucketFunction function = bucketFunction(50, createUnboundedVarcharType());
        assertEquals(getBucket(function, createStringsBlock("lorem ipsum")), 2);
        assertEquals(getBucket(function, createStringsBlock("lorem")), 26);
        assertEquals(getBucket(function, createStringsBlock("ipsum")), 3);
        assertEquals(getBucket(function, createStringsBlock("hello")), 19);
    }

    @Test
    public void testVarcharBigint()
    {
        BucketFunction function = bucketFunction(50, createUnboundedVarcharType(), BIGINT);
        assertEquals(getBucket(function, createStringsBlock("lorem ipsum"), createLongsBlock(123456789012L)), 24);
        assertEquals(getBucket(function, createStringsBlock("lorem"), createLongsBlock(454345325)), 32);
        assertEquals(getBucket(function, createStringsBlock("ipsum"), createLongsBlock(365363)), 21);
        assertEquals(getBucket(function, createStringsBlock("hello"), createLongsBlock(45645747)), 34);
        assertEquals(getBucket(function, createStringsBlock("world"), createLongsBlock(3244)), 4);
    }

    private static int getBucket(BucketFunction function, Block... blocks)
    {
        return function.getBucket(new Page(blocks), 0);
    }

    private static BucketFunction bucketFunction(int bucketCount, Type... types)
    {
        return new RaptorBucketFunction(bucketCount, ImmutableList.copyOf(types));
    }
}
