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
package com.facebook.presto.parquet.selectivereader;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockLease;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import java.io.IOException;

public interface SelectiveColumnReader
{
    void setColumnChunkMetadata(ColumnChunkMetaData metadata);

    int read(int[] positions, int positionCount)
            throws IOException;

    /**
     * @return an array of positions that passed the filter during most recent read(); the return
     * value of read() is the number of valid entries in this array; the return value is a strict
     * subset of positions passed into read()
     */
    int[] getReadPositions();

    /**
     * Return a subset of the values extracted during most recent read() for the specified positions
     * <p>
     * Can be called at most once after each read().
     *
     * @param positions Monotonically increasing positions to return; must be a strict subset of both
     * the list of positions passed into read() and the list of positions returned
     * from getReadPositions()
     * @param positionCount Number of valid positions in the positions array; may be less than the
     * size of the array
     */
    Block getBlock(int[] positions, int positionCount);

    BlockLease getBlockView(int[] positions, int positionCount);

    void setExecutionMode(AbstractSelectiveColumnReader.ExecutionMode executionMode);

    void close();
}
