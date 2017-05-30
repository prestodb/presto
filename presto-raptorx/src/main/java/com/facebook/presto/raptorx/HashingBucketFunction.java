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
package com.facebook.presto.raptorx;

import com.facebook.presto.spi.BucketFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.XxHash64;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.toUnsignedLong;

public class HashingBucketFunction
        implements BucketFunction
{
    private final HashFunction[] functions;
    private final int bucketCount;

    public HashingBucketFunction(int bucketCount, List<Type> types)
    {
        checkArgument(bucketCount > 0, "bucket count must be at least one");
        checkArgument(!types.isEmpty(), "types must not be empty");
        this.bucketCount = bucketCount;
        this.functions = types.stream()
                .map(HashingBucketFunction::getHashFunction)
                .toArray(HashFunction[]::new);
    }

    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        for (int i = 0; i < page.getChannelCount(); i++) {
            Block block = page.getBlock(i);
            long value = functions[i].hash(block, position);
            hash = (hash * 31) + value;
        }
        return computeBucket(hash, bucketCount);
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    private static int computeBucket(long hash, int count)
    {
        return (int) ((toUnsignedLong(Long.hashCode(hash)) * count) >> 32);
    }

    public static void validateBucketType(Type type)
    {
        getHashFunction(type);
    }

    private static HashFunction getHashFunction(Type type)
    {
        if (type.equals(BIGINT)) {
            return bigintHashFunction();
        }
        if (type.equals(INTEGER)) {
            return intHashFunction();
        }
        if (isVarcharType(type)) {
            return varcharHashFunction();
        }
        throw new PrestoException(NOT_SUPPORTED, "Column type not supported for bucketing: " + type.getDisplayName());
    }

    private static HashFunction bigintHashFunction()
    {
        return (block, position) -> XxHash64.hash(BIGINT.getLong(block, position));
    }

    private static HashFunction intHashFunction()
    {
        return (block, position) -> XxHash64.hash(INTEGER.getLong(block, position));
    }

    private static HashFunction varcharHashFunction()
    {
        return (block, position) -> XxHash64.hash(block.getSlice(position, 0, block.getSliceLength(position)));
    }

    private interface HashFunction
    {
        long hash(Block block, int position);
    }
}
