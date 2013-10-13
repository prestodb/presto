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

import com.facebook.presto.tuple.FixedWidthTypeInfo;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TypeInfo;
import com.facebook.presto.tuple.VariableWidthTypeInfo;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;

import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public final class BlockBuilders
{
    private BlockBuilders()
    {
    }

    public static BlockBuilder createBlockBuilder(TupleInfo tupleInfo)
    {
        return createBlockBuilder(tupleInfo, BlockBuilder.DEFAULT_MAX_BLOCK_SIZE, BlockBuilder.DEFAULT_INITIAL_BUFFER_SIZE);
    }

    public static BlockBuilder createBlockBuilder(TupleInfo tupleInfo, DataSize maxBlockSize, DataSize initialBufferSize)
    {
        TypeInfo typeInfo = tupleInfo.getTypeInfo();
        if (typeInfo instanceof FixedWidthTypeInfo) {
            return new FixedWidthBlockBuilder((FixedWidthTypeInfo) typeInfo, maxBlockSize, initialBufferSize);
        }
        else {
            return new VariableWidthBlockBuilder((VariableWidthTypeInfo) typeInfo, maxBlockSize, initialBufferSize);
        }
    }

    public static BlockBuilder createFixedSizeBlockBuilder(FixedWidthTypeInfo typeInfo, int positionCount)
    {
        int entrySize = typeInfo.getSize() + SIZE_OF_BYTE;
        return new FixedWidthBlockBuilder(typeInfo, Slices.allocate(entrySize * positionCount));
    }
}
