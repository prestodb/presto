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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.SqlType;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.createBlock;

public final class ArrayFunctions
{
    private ArrayFunctions()
    {
    }

    @ScalarFunction(hidden = true)
    @SqlType("array<unknown>")
    public static Slice arrayConstructor()
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 0);
        return buildStructuralSlice(blockBuilder);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<bigint>") Slice slice, @SqlType(StandardTypes.BIGINT) long value)
    {
        return arrayContains(slice, BigintType.BIGINT, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<boolean>") Slice slice, @SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return arrayContains(slice, BooleanType.BOOLEAN, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<double>") Slice slice, @SqlType(StandardTypes.DOUBLE) double value)
    {
        return arrayContains(slice, DoubleType.DOUBLE, value);
    }

    @Nullable
    @ScalarFunction
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean contains(@SqlType("array<varchar>") Slice slice, @SqlType(StandardTypes.VARCHAR) Slice value)
    {
        return arrayContains(slice, VarcharType.VARCHAR, value);
    }

    private static Boolean arrayContains(Slice slice, Type type, Object value)
    {
        Block block = readStructuralBlock(slice);
        Block valueBlock = createBlock(type, value);

        //TODO: This could be quite slow, it should use parametric equals
        for (int i = 0; i < block.getPositionCount(); i++) {
            if (type.equalTo(block, i, valueBlock, 0)) {
                return true;
            }
        }

        return false;
    }
}
