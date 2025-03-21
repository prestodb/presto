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
package com.facebook.presto.operator.unnest;

import static java.lang.String.format;

final class UnnestOperatorBlockUtil
{
    private UnnestOperatorBlockUtil() {}

    private static final int DEFAULT_CAPACITY = 64;
    // See java.util.ArrayList for an explanation
    static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

    // Copied from io.prestosql.spi.block.BlockUtil#calculateNewArraySize
    static int calculateNewArraySize(int currentSize)
    {
        // grow array by 50%
        long newSize = (long) currentSize + (currentSize >> 1);

        // verify new size is within reasonable bounds
        if (newSize < DEFAULT_CAPACITY) {
            newSize = DEFAULT_CAPACITY;
        }
        else if (newSize > MAX_ARRAY_SIZE) {
            newSize = MAX_ARRAY_SIZE;
            if (newSize == currentSize) {
                throw new IllegalArgumentException(format("Can not grow array beyond '%s'", MAX_ARRAY_SIZE));
            }
        }
        return (int) newSize;
    }
}
