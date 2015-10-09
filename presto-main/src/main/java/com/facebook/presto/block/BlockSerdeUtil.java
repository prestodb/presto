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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.util.Reflection.methodHandle;

// This class is only used in tests and LiteralInterpreter for magic literal. Most likely, you shouldn't use it from anywhere else.
public final class BlockSerdeUtil
{
    public static final MethodHandle READ_BLOCK = methodHandle(BlockSerdeUtil.class, "readBlock", BlockEncodingSerde.class, Slice.class);

    private BlockSerdeUtil() {}

    public static Block readBlock(BlockEncodingSerde blockEncodingSerde, Slice slice)
    {
        SliceInput input = slice.getInput();
        BlockEncoding blockEncoding = blockEncodingSerde.readBlockEncoding(input);
        return blockEncoding.readBlock(input);
    }

    public static void writeBlock(SliceOutput output, Block block)
    {
        BlockEncoding encoding = block.getEncoding();
        BlockEncodingManager.writeBlockEncodingInternal(output, encoding);
        encoding.writeBlock(output, block);
    }

    public static Slice blockToSlice(Block block)
    {
        SliceOutput sliceOutput = new DynamicSliceOutput(1000);
        writeBlock(sliceOutput, block.copyRegion(0, block.getPositionCount()));
        return sliceOutput.slice();
    }

    public static class TypeAndBlock
    {
        private final Type type;
        private final Block block;

        public TypeAndBlock(Type type, Block block)
        {
            // note to reviewers: we support nulls
            this.type = type;
            this.block = block;
        }

        public Type getType()
        {
            return type;
        }

        public Block getBlock()
        {
            return block;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TypeAndBlock that = (TypeAndBlock) o;

            if (type != null ? !type.equals(that.type) : that.type != null) {
                return false;
            }
            return !(block != null ? !blockToSlice(block).equals(blockToSlice(that.block)) : that.block != null);
        }

        @Override
        public int hashCode()
        {
            int result = type != null ? type.hashCode() : 0;
            result = 31 * result + (block != null ? block.hashCode() : 0);
            return result;
        }
    }
}
