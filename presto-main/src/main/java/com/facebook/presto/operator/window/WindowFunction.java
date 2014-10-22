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
package com.facebook.presto.operator.window;

import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;

public interface WindowFunction
{
    Type getType();

    /**
     * Reset state for a new partition (including the first one).
     *
     * @param partitionStartPosition position of the first row of the partition in the pagesIndex
     * @param partitionRowCount the total number of rows in the new partition
     * @param pagesIndex the pages index which contains sorted values
     */
    void reset(int partitionStartPosition, int partitionRowCount, PagesIndex pagesIndex);

    /**
     * Process a row by outputting the result of the window function.
     * <p/>
     * This method provides information about the ordering peer group. A peer group is all
     * of the rows that are peers within the specified ordering. Rows are peers if they
     * compare equal to each other using the specified ordering expression. The ordering
     * of rows within a peer group is undefined (otherwise they would not be peers).
     *
     * @param newPeerGroup if this row starts a new peer group
     * @param peerGroupCount the total number of rows in this peer group
     */
    void processRow(BlockBuilder output, boolean newPeerGroup, int peerGroupCount);
}
