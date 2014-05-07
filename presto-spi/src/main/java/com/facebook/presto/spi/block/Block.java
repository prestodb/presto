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

import com.facebook.presto.spi.type.Type;

public interface Block
{
    /**
     * Gets the type of this block.
     */
    Type getType();

    /**
     * Returns the number of positions in this block.
     */
    int getPositionCount();

    /**
     * Returns the size of this block in memory.
     */
    int getSizeInBytes();

    /**
     * Gets a cursor over the block.
     */
    BlockCursor cursor();

    /**
     * Get the encoding for this block.
     */
    BlockEncoding getEncoding();

    /**
     * Returns a block starting at the specified position and extends for the
     * specified length.  The specified region must be entirely contained
     * within this block.
     */
    Block getRegion(int positionOffset, int length);

    /**
     * Converts this block to a random access block.
     */
    RandomAccessBlock toRandomAccessBlock();
}
