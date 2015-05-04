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
import com.facebook.presto.spi.type.Type;

/**
 * Set of {@code Type} elements
 */
public interface TypedSet
{
    // Check if contains element at {@code position} of {@code block}
    public boolean contains(final Block block, final int position);

    // Add element at {@code position} of {@code block}
    public void add(final Block block, final int position);

    // Number of elements
    public int size();

    // Element type
    public Type getType();

    // Estimated memory size in bytes
    public long getEstimatedSize();
}
