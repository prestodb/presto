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

package com.facebook.presto.operator.repartition;

import io.airlift.slice.SliceOutput;

public interface BlockEncodingBuffer
{
    /**
     * Pass in the decoded block and positions in this block to copy. Called when a new page is being processed.
     */
    void setupDecodedBlocksAndPositions(DecodedBlockNode decodedBlockNode, int[] positions, int positionCount, int partitionBufferCapacity, long estimatedSerializedPageSize);

    /**
     * Adds serialized row sizes to serializedRowSizes array. Called for top level columns.
     */
    void accumulateSerializedRowSizes(int[] serializedRowSizes);

    /**
     * Set the position offset and batch size on the positions array in the BlockEncodingBuffer. Called before the next batch is being copied
     */
    void setNextBatch(int positionsOffset, int batchSize);

    /**
     * Append the rows at the end of the buffers in BlockEncodingBuffer
     */
    void appendDataInBatch();

    /**
     * Copy the data from the buffers in the BlockEncodingBuffer to the SliceOutput object. Called when the buffers are full and flushing happens.
     */
    void serializeTo(SliceOutput output);

    /**
     * Reset the BlockEncodingBuffer to its initial state. The next incoming page will be appended from the beginning.
     */
    void resetBuffers();

    /**
     * Signals that this is the last batch in this page, so that the internal buffers in BlockEncodingBuffers can recycled.
     */
    void noMoreBatches();

    long getRetainedSizeInBytes();

    long getSerializedSizeInBytes();
}
