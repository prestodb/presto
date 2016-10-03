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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.XxHash64;

import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;

public class RaptorBucketFunction
        implements BucketFunction
{
    private final HashFunction[] functions;

    public RaptorBucketFunction(List<Type> types)
    {
        checkArgument(types != null, "types is is null");
        checkArgument(!types.isEmpty(), "types is empty");

        this.functions = new HashFunction[types.size()];
        for (int i = 0; i < types.size(); i++) {
            Type type = types.get(i);
            if (type.equals(BIGINT)) {
                functions[i] = bigintHashFunction();
            }
            else if (type.equals(INTEGER)) {
                functions[i] = intHashFunction();
            }
            else if (isVarcharType(type)) {
                functions[i] = varcharHashFunction();
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, "Bucketing not supported for type: " + type.getDisplayName());
            }
        }
    }

    @SuppressWarnings("NumericCastThatLosesPrecision")
    @Override
    public int getBucket(Page page, int position)
    {
        long hash = 0;
        int channelCount = page.getChannelCount();
        for (int i = 0; i < channelCount; i++) {
            Block block = page.getBlock(i);

            HashFunction function = functions[i];
            hash = (hash * 31) + function.hash(block, position);
        }
        int value = (int) (hash & Integer.MAX_VALUE);
        return value % channelCount;
    }

    public static void checkTypeSupported(Type type)
    {
        if (!type.equals(BIGINT) && !type.equals(INTEGER) && !isVarcharType(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Bucketing is supported for BIGINT, INTEGER and VARCHAR columns");
        }
    }

    @VisibleForTesting
    protected static HashFunction bigintHashFunction()
    {
        return (block, position) -> XxHash64.hash(BIGINT.getLong(block, position));
    }

    @VisibleForTesting
    protected static HashFunction varcharHashFunction()
    {
        return (block, position) -> XxHash64.hash(block.getSlice(position, 0, block.getLength(position)));
    }

    @VisibleForTesting
    protected static HashFunction intHashFunction()
    {
        return (block, position) -> XxHash64.hash(INTEGER.getLong(block, position));
    }

    protected interface HashFunction
    {
        long hash(Block block, int position);
    }
}
