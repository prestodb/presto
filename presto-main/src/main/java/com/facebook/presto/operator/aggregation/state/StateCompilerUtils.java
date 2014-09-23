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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.google.common.base.Throwables;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.reflect.Method;

public final class StateCompilerUtils
{
    private StateCompilerUtils() {}

    public static Method getBlockGetter(Class<?> type)
    {
        try {
            if (type == long.class) {
                return StateCompilerUtils.class.getMethod("getLongBlock", Block.class, int.class);
            }
            else if (type == double.class) {
                return StateCompilerUtils.class.getMethod("getDoubleBlock", Block.class, int.class);
            }
            else if (type == boolean.class) {
                return StateCompilerUtils.class.getMethod("getBooleanBlock", Block.class, int.class);
            }
            else if (type == byte.class) {
                return StateCompilerUtils.class.getMethod("getByteBlock", Block.class, int.class);
            }
            else if (type == Slice.class) {
                return StateCompilerUtils.class.getMethod("getSliceBlock", Block.class, int.class);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type.getSimpleName());
            }
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Method getBlockBuilderAppend(Class<?> type)
    {
        try {
            if (type == long.class) {
                return StateCompilerUtils.class.getMethod("appendLongBlockBuilder", BlockBuilder.class, long.class);
            }
            else if (type == double.class) {
                return StateCompilerUtils.class.getMethod("appendDoubleBlockBuilder", BlockBuilder.class, double.class);
            }
            else if (type == boolean.class) {
                return StateCompilerUtils.class.getMethod("appendBooleanBlockBuilder", BlockBuilder.class, boolean.class);
            }
            else if (type == byte.class) {
                return StateCompilerUtils.class.getMethod("appendByteBlockBuilder", BlockBuilder.class, byte.class);
            }
            else if (type == Slice.class) {
                return StateCompilerUtils.class.getMethod("appendSliceBlockBuilder", BlockBuilder.class, Slice.class);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type.getSimpleName());
            }
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Method getSliceSetter(Class<?> type)
    {
        try {
            if (type == long.class) {
                return StateCompilerUtils.class.getMethod("setLongSlice", Slice.class, int.class, long.class);
            }
            else if (type == double.class) {
                return StateCompilerUtils.class.getMethod("setDoubleSlice", Slice.class, int.class, double.class);
            }
            else if (type == boolean.class) {
                return StateCompilerUtils.class.getMethod("setBooleanSlice", Slice.class, int.class, boolean.class);
            }
            else if (type == byte.class) {
                return StateCompilerUtils.class.getMethod("setByteSlice", Slice.class, int.class, byte.class);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type.getSimpleName());
            }
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static Method getSliceGetter(Class<?> type)
    {
        try {
            if (type == long.class) {
                return StateCompilerUtils.class.getMethod("getLongSlice", Slice.class, int.class);
            }
            else if (type == double.class) {
                return StateCompilerUtils.class.getMethod("getDoubleSlice", Slice.class, int.class);
            }
            else if (type == boolean.class) {
                return StateCompilerUtils.class.getMethod("getBooleanSlice", Slice.class, int.class);
            }
            else if (type == byte.class) {
                return StateCompilerUtils.class.getMethod("getByteSlice", Slice.class, int.class);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type.getSimpleName());
            }
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
    }

    public static long getLongBlock(Block block, int index)
    {
        return block.getLong(index, 0);
    }

    public static byte getByteBlock(Block block, int index)
    {
        return (byte) block.getLong(index, 0);
    }

    public static double getDoubleBlock(Block block, int index)
    {
        return block.getDouble(index, 0);
    }

    public static boolean getBooleanBlock(Block block, int index)
    {
        return block.getByte(index, 0) != 0;
    }

    public static Slice getSliceBlock(Block block, int index)
    {
        if (block.isNull(index)) {
            return null;
        }
        else {
            return block.getSlice(index, 0, block.getLength(index));
        }
    }

    public static void appendLongBlockBuilder(BlockBuilder builder, long value)
    {
        builder.writeLong(value).closeEntry();
    }

    public static void appendDoubleBlockBuilder(BlockBuilder builder, double value)
    {
        builder.writeDouble(value).closeEntry();
    }

    public static void appendBooleanBlockBuilder(BlockBuilder builder, boolean value)
    {
        builder.writeByte(value ? 1 : 0).closeEntry();
    }

    public static void appendByteBlockBuilder(BlockBuilder builder, byte value)
    {
        builder.writeLong(value).closeEntry();
    }

    public static void appendSliceBlockBuilder(BlockBuilder builder, @Nullable Slice value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else {
            builder.writeBytes(value, 0, value.length()).closeEntry();
        }
    }

    public static void setLongSlice(Slice slice, int offset, long value)
    {
        slice.setLong(offset, value);
    }

    public static void setDoubleSlice(Slice slice, int offset, double value)
    {
        slice.setDouble(offset, value);
    }

    public static void setBooleanSlice(Slice slice, int offset, boolean value)
    {
        slice.setByte(offset, value ? 1 : 0);
    }

    public static void setByteSlice(Slice slice, int offset, byte value)
    {
        slice.setByte(offset, value);
    }

    public static long getLongSlice(Slice slice, int offset)
    {
        return slice.getLong(offset);
    }

    public static double getDoubleSlice(Slice slice, int offset)
    {
        return slice.getDouble(offset);
    }

    public static boolean getBooleanSlice(Slice slice, int offset)
    {
        return slice.getByte(offset) == 1;
    }

    public static byte getByteSlice(Slice slice, int offset)
    {
        return slice.getByte(offset);
    }
}
