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
package com.facebook.presto.orc.reader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;

import java.io.IOException;

public interface SelectiveStreamReader
        extends StreamReader
{
    /**
     * Extract values at the specified positions, apply filter and buffer the values that pass
     * the filter.
     *
     * @param positions Monotonically increasing positions to read
     * @param positionCount Number of valid positions in the positions array; may be less than the
     *                      size of the array
     * @return the number of positions that passed the filter
     */
    int read(int offset, int[] positions, int positionCount)
            throws IOException;

    /**
     * @return an array of positions that passed the filter during most recent read(); the return
     *      value of read() is the number of valid entries in this array; the return value is a strict
     *      subset of positions passed into read()
     */
    int[] getReadPositions();

    /**
     * Return a subset of the values extracted during most recent read() for the specified positions
     *
     * Can be called at most once after each read().
     *
     * @param positions Monotonically increasing positions to return; must be a strict subset of both
     *                  the list of positions passed into read() and the list of positions returned
     *                  from getReadPositions()
     * @param positionCount Number of valid positions in the positions array; may be less than the
     *                      size of the array
     */
    Block getBlock(int[] positions, int positionCount);

    /**
     * Like getBlock(), but returns a temporary view of the data.
     *
     * The returned lease must be closed and block usage should be completed before subsequent
     * invocations of getBlockView, getBlock, or read.
     *
     * May be called multiple times after each read(). On repeated invocation, the list of positions
     * must be a strict subset of positions passed in the previous invocation.
     */
    BlockLease getBlockView(int[] positions, int positionCount);

    /**
     * Throws any error that occurred while reading specified positions.
     *
     * Used by list and map readers to raise "subscript out of bounds" error.
     */
    void throwAnyError(int[] positions, int positionCount);
}
