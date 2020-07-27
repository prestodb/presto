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
package com.facebook.presto.kafka.encoder;

import com.facebook.presto.common.block.Block;

import java.io.Closeable;

public interface RowEncoder
        extends Closeable
{
    /**
     * Adds the value from the given block/position to the row being encoded
     */
    void appendColumnValue(Block block, int position);

    /**
     * Returns the encoded values as a byte array, and resets any info needed to prepare for next row
     */
    byte[] toByteArray();
}
