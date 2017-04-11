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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

public interface AccumulatorStateSerializer<T>
{
    Type getSerializedType();

    void serialize(T state, BlockBuilder out);

    /**
     * Deserialize {@code index}-th position in {@code block} into {@code state}.
     * <p>
     * This method may be invoked with a reused dirty {@code state}. Therefore,
     * implementations must not make assumptions about the initial value of
     * {@code state}.
     * <p>
     * Null positions in {@code block} are skipped and ignored. In other words,
     * {@code block.isNull(index)} is guaranteed to return false.
     */
    void deserialize(Block block, int index, T state);
}
