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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.function.Function;

import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class VarcharToIntegerNumberCoercer
        implements Function<Block, Block>
{
    private final Type fromType;
    private final Type toType;

    private final long minValue;
    private final long maxValue;

    public VarcharToIntegerNumberCoercer(Type fromType, Type toType)
    {
        this.fromType = requireNonNull(fromType, "fromType is null");
        this.toType = requireNonNull(toType, "toType is null");

        if (toType.equals(TINYINT)) {
            minValue = Byte.MIN_VALUE;
            maxValue = Byte.MAX_VALUE;
        }
        else if (toType.equals(SMALLINT)) {
            minValue = Short.MIN_VALUE;
            maxValue = Short.MAX_VALUE;
        }
        else if (toType.equals(INTEGER)) {
            minValue = Integer.MIN_VALUE;
            maxValue = Integer.MAX_VALUE;
        }
        else if (toType.equals(BIGINT)) {
            minValue = Long.MIN_VALUE;
            maxValue = Long.MAX_VALUE;
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from from varchar to %s", toType));
        }
    }

    @Override
    public Block apply(Block block)
    {
        BlockBuilder blockBuilder = toType.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
                continue;
            }
            try {
                long value = Long.parseLong(fromType.getSlice(block, i).toStringUtf8());
                if (minValue <= value && value <= maxValue) {
                    toType.writeLong(blockBuilder, value);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            catch (NumberFormatException e) {
                blockBuilder.appendNull();
            }
        }
        return blockBuilder.build();
    }
}
