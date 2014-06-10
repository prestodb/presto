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

public interface BlockBuilder
        extends Block
{
    /**
     * Appends a boolean value to the block.
     */
    BlockBuilder appendBoolean(boolean value);

    /**
     * Appends a long value to the block.
     */
    BlockBuilder appendLong(long value);

    /**
     * Appends a double value to the block.
     */
    BlockBuilder appendDouble(double value);

    /**
     * Appends a Slice value to the block.
     */
    BlockBuilder appendSlice(Slice value);

    /**
     * Appends a range of a Slice value to the block.
     */
    BlockBuilder appendSlice(Slice value, int offset, int length);

    /**
     * Appends a null value to the block.
     */
    BlockBuilder appendNull();

    /**
     * Builds the block. This method can be called multiple times.
     */
    Block build();

    /**
     * Have any values been added to the block?
     */
    boolean isEmpty();

    /**
     * Is this block full? If true no more values should be added to the block.
     */
    boolean isFull();
}
