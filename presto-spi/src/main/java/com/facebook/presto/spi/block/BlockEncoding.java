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
package com.facebook.presto.spi.block;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Optional;

public interface BlockEncoding
{
    /**
     * Gets the unique name of this encoding.
     */
    String getName();

    /**
     * Read a block from the specified input.  The returned
     * block should begin at the specified position.
     */
    Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input);

    default Block readBlockReusing(BlockEncodingSerde blockEncodingSerde, SliceInput input, BlockDecoder toReuse)
    {
        throw new UnsupportedOperationException();
    }

    default boolean supportsReadBlockReusing()
    {
        return false;
    }

    /**
     * Write the specified block to the specified output
     */
    void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block);

    /**
     * This method allows the implementor to specify a replacement object that will be serialized instead of the original one.
     */
    default Optional<Block> replacementBlockForWrite(Block block)
    {
        return Optional.empty();
    }

    default int reserveBytesInBuffer(BlockDecoder contents, int numValues, int startInBuffer, EncodingState state)
    {
        throw new UnsupportedOperationException();
    }

    default void addValues(BlockDecoder contents, int[] rows, int firstRow, int numRows, EncodingState state)
            {
        throw new UnsupportedOperationException();
    }

    // Returns the final size of the encoded data. Sets newStartOffset
    // to be the place where finish() will copy the buffered data.
    default int prepareFinish(EncodingState state, int newStartOffset)
    {
        throw new UnsupportedOperationException();
    }

    // Copies the data into the buffer at the position recorded by prepareFinish.
    default void finish(EncodingState state, Slice buffer)
    {
        throw new UnsupportedOperationException();
    }
}

