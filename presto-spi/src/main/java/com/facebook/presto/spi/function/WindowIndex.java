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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;

/**
 * A window index contains the sorted values for a window partition.
 * Each window function argument is available as a separate channel.
 */
public interface WindowIndex
{
    /**
     * Gets the number of rows in the partition
     */
    int size();

    /**
     * Check if a value is null.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     * @return if the value is null
     */
    boolean isNull(int channel, int position);

    /**
     * Gets a value as a {@code boolean}.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     * @return value at the specified channel and position
     */
    boolean getBoolean(int channel, int position);

    /**
     * Gets a value as a {@code long}.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     * @return value at the specified channel and position
     */
    long getLong(int channel, int position);

    /**
     * Gets a value as a {@code double}.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     * @return value at the specified channel and position
     */
    double getDouble(int channel, int position);

    /**
     * Gets a value as a {@link Slice}.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     */
    Slice getSlice(int channel, int position);

    /**
     * Outputs a value from the index. This is useful for "value"
     * window functions such as {@code lag} that operate on arbitrary
     * types without caring about the specific contents.
     *
     * @param channel argument number
     * @param position row within the partition, starting at zero
     * @param output the {@link BlockBuilder} to output to
     */
    void appendTo(int channel, int position, BlockBuilder output);
}
