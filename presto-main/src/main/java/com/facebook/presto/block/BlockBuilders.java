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
package com.facebook.presto.block;

import com.facebook.presto.type.FixedWidthType;
import com.facebook.presto.type.Type;
import com.facebook.presto.type.VariableWidthType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public final class BlockBuilders
{
    private BlockBuilders()
    {
    }

    public static BlockBuilder createBlockBuilder(Type type)
    {
        return createBlockBuilder(type, BlockBuilder.DEFAULT_MAX_BLOCK_SIZE, BlockBuilder.DEFAULT_INITIAL_BUFFER_SIZE);
    }

    public static BlockBuilder createBlockBuilder(Type type, DataSize maxBlockSize, DataSize initialBufferSize)
    {
        if (type instanceof FixedWidthType) {
            return new FixedWidthBlockBuilder((FixedWidthType) type, maxBlockSize, initialBufferSize);
        }
        else {
            return new VariableWidthBlockBuilder((VariableWidthType) type, maxBlockSize, initialBufferSize);
        }
    }

    public static BlockBuilder createBlockBuilder(Type type, Slice slice)
    {
        if (type instanceof FixedWidthType) {
            return new FixedWidthBlockBuilder((FixedWidthType) type, slice);
        }
        else {
            return new VariableWidthBlockBuilder((VariableWidthType) type, slice);
        }
    }

    public static BlockBuilder createFixedSizeBlockBuilder(Type type, int positionCount)
    {
        checkArgument(type instanceof FixedWidthType, "type is not fixed width");
        FixedWidthType fixedWidthType = (FixedWidthType) type;

        int entrySize = fixedWidthType.getFixedSize() + SIZE_OF_BYTE;
        return new FixedWidthBlockBuilder(fixedWidthType, Slices.allocate(entrySize * positionCount));
    }
}
