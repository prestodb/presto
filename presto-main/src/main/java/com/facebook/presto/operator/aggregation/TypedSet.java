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

/**
 * Set of {@code Type} elements
 */
public interface TypedSet
{
    // Check if contains element at {@code position} of {@code block}
    boolean contains(Block block, int position);

    // Add element at {@code position} of {@code block}
    void add(Block block, int position);

    // Number of elements
    int size();

    // Estimated memory size in bytes
    long getEstimatedSize();
}
