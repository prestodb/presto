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

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

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
    Block readBlock(SliceInput input);

    /**
     * Write the specified block to the specified output
     */
    void writeBlock(SliceOutput sliceOutput, Block block);

    /**
     * Return associated factory
     */
    BlockEncodingFactory getFactory();
}
