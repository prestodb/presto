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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

public interface TypedHistogram
{
    long getEstimatedSize();

    void serialize(BlockBuilder out);

    void addAll(TypedHistogram other);

    void readAllValues(HistogramValueReader reader);

    void add(int position, Block block, long count);

    Type getType();

    int getExpectedSize();

    // no-op on non-grouped
    default TypedHistogram setGroupId(long groupId) { return this;}

    default void ensureCapacity(long size) {}

    boolean isEmpty();
}
