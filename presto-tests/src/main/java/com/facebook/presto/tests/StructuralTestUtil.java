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
package com.facebook.presto.tests;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.math.BigDecimal;
import java.util.List;

import static com.facebook.presto.type.TypeJsonUtils.appendToBlockBuilder;
import static com.google.common.base.Preconditions.checkArgument;

public final class StructuralTestUtil
{
    private StructuralTestUtil() {}

    public static boolean arrayBlocksEqual(Type elementType, Block block1, Block block2)
    {
        if (block1.getPositionCount() != block2.getPositionCount()) {
            return false;
        }
        for (int i = 0; i < block1.getPositionCount(); i++) {
            if (block1.isNull(i) != block2.isNull(i)) {
                return false;
            }
            if (!block1.isNull(i) && !elementType.equalTo(block1, i, block2, i)) {
                return false;
            }
        }
        return true;
    }

    public static boolean mapBlocksEqual(Type keyType, Type valueType, Block block1, Block block2)
    {
        if (block1.getPositionCount() != block2.getPositionCount()) {
            return false;
        }
        for (int i = 0; i < block1.getPositionCount(); i += 2) {
            if (block1.isNull(i) != block2.isNull(i) || block1.isNull(i + 1) != block2.isNull(i + 1)) {
                return false;
            }
            if (!block1.isNull(i) && !keyType.equalTo(block1, i, block2, i)) {
                return false;
            }
            if (!block1.isNull(i + 1) && !valueType.equalTo(block1, i + 1, block2, i + 1)) {
                return false;
            }
        }
        return true;
    }

    public static Block arrayBlockOf(Type elementType, Object... values)
    {
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), 1024);
        for (Object value : values) {
            appendToBlockBuilder(elementType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Object key, Object value)
    {
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), 1024);
        appendToBlockBuilder(keyType, key, blockBuilder);
        appendToBlockBuilder(valueType, value, blockBuilder);
        return blockBuilder.build();
    }

    public static Block mapBlockOf(Type keyType, Type valueType, Object[] keys, Object[] values)
    {
        checkArgument(keys.length == values.length, "keys/values must have the same length");
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), 1024);
        for (int i = 0; i < keys.length; i++) {
            Object key = keys[i];
            Object value = values[i];
            appendToBlockBuilder(keyType, key, blockBuilder);
            appendToBlockBuilder(valueType, value, blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block rowBlockOf(List<Type> parameterTypes, Object... values)
    {
        InterleavedBlockBuilder blockBuilder = new InterleavedBlockBuilder(parameterTypes, new BlockBuilderStatus(), 1024);
        for (int i = 0; i < values.length; i++) {
            appendToBlockBuilder(parameterTypes.get(i), values[i], blockBuilder);
        }
        return blockBuilder.build();
    }

    public static Block decimalArrayBlockOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return arrayBlockOf(type, longDecimal);
        }
        else {
            Slice sliceDecimal = Decimals.encodeUnscaledValue(decimal.unscaledValue());
            return arrayBlockOf(type, sliceDecimal);
        }
    }

    public static Block decimalMapBlockOf(DecimalType type, BigDecimal decimal)
    {
        if (type.isShort()) {
            long longDecimal = decimal.unscaledValue().longValue();
            return mapBlockOf(type, type, longDecimal, longDecimal);
        }
        else {
            Slice sliceDecimal = Decimals.encodeUnscaledValue(decimal.unscaledValue());
            return mapBlockOf(type, type, sliceDecimal, sliceDecimal);
        }
    }
}
