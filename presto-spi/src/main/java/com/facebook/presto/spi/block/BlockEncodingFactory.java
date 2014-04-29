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

import com.facebook.presto.spi.type.TypeManager;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

public interface BlockEncodingFactory<T extends BlockEncoding>
{
    /**
     * Gets the unique name of this encoding.
     */
    String getName();

    /**
     * Reads the encoding from the specified input.
     */
    T readEncoding(TypeManager manager, BlockEncodingSerde serde, SliceInput input);

    /**
     * Writes this encoding to the output stream.
     */
    void writeEncoding(BlockEncodingSerde serde, SliceOutput output, T blockEncoding);
}
