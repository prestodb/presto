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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

import static com.facebook.presto.operator.unnest.UnnestOperatorBlockUtil.calculateNewArraySize;
import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * This class manages the low level details of building unnested blocks with a goal of minimizing data copying
 */
class UnnestBlockBuilder
{
    private final Type type;
    private Block source;

    // checks for existence of null element in the source when required
    private final NullElementFinder nullFinder = new NullElementFinder();

    // flag indicating whether we are using copied block or a dictionary block
    private boolean usingCopiedBlock;

    // State for output dictionary block
    private int[] ids;
    private int positionCount;

    // State for output copied block
    private BlockBuilder outputBlockBuilder;
    private PageBuilderStatus pageBuilderStatus;

    public UnnestBlockBuilder(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    /**
     * Replaces input source block with {@code block}. The old data structures for output have to be
     * reset as well, because they are based on the source.
     */
    public void resetInputBlock(Block block)
    {
        this.source = requireNonNull(block, "block is null");
        // output and null-check have to be cleared because the source has changed
        clearCurrentOutput();
        nullFinder.resetCheck();
    }

    /**
     * Resets all data structures used for producing current output block, except for the source block.
     */
    @VisibleForTesting
    void clearCurrentOutput()
    {
        // clear dictionary block state
        ids = new int[0];
        positionCount = 0;
        usingCopiedBlock = false;

        // clear copied block state
        outputBlockBuilder = null;
        pageBuilderStatus = null;
    }

    /**
     * Prepares for a new output block.
     * The caller has to ensure that the source is not null when this method is invoked.
     */
    public void startNewOutput(PageBuilderStatus pageBuilderStatus, int expectedEntries)
    {
        checkState(source != null, "source is null");
        this.pageBuilderStatus = pageBuilderStatus;

        // Prepare for a new dictionary block
        this.ids = new int[expectedEntries];
        this.positionCount = 0;
        this.usingCopiedBlock = false;
    }

    public Block buildOutputAndFlush()
    {
        Block outputBlock;
        if (usingCopiedBlock) {
            outputBlock = outputBlockBuilder.build();
        }
        else {
            outputBlock = new DictionaryBlock(positionCount, source, ids);
        }

        // Flush stored state, so that ids can not be modified after the dictionary has been constructed
        clearCurrentOutput();

        return outputBlock;
    }

    /**
     * Append element at position {@code index} in the source block to output block
     */
    public void appendElement(int index)
    {
        checkState(source != null, "source is null");
        checkElementIndex(index, source.getPositionCount());

        if (usingCopiedBlock) {
            type.appendTo(source, index, outputBlockBuilder);
        }
        else {
            appendId(index);
        }
    }

    /**
     * Append range of elements starting at {@code startPosition} and length {@code length}
     * from the source block to output block.
     *
     * Purpose of this method is to avoid repeated range checks for every invocation of {@link #appendElement}.
     */
    public void appendRange(int startPosition, int length)
    {
        // check range validity
        checkState(source != null, "source is null");
        checkPositionIndexes(startPosition, startPosition + length, source.getPositionCount());

        if (usingCopiedBlock) {
            for (int i = 0; i < length; i++) {
                type.appendTo(source, startPosition + i, outputBlockBuilder);
            }
        }
        else {
            for (int i = 0; i < length; i++) {
                appendId(startPosition + i);
            }
        }
    }

    public void appendNull()
    {
        if (usingCopiedBlock) {
            outputBlockBuilder.appendNull();
        }
        else {
            int nullIndex = nullFinder.getNullElementIndex();
            if (nullIndex == -1) {
                startUsingCopiedBlock();
                appendNull();
            }
            else {
                appendId(nullIndex);
            }
        }
    }

    /**
     * This method is invoked when transition from dictionary block to copied block happens.
     * - outputBlockBuilder is created and existing elements as indicated by ids[] are copied over.
     * - ids[] and positionCount are reset, as they are not used anymore.
     */
    private void startUsingCopiedBlock()
    {
        requireNonNull(pageBuilderStatus, "pageBuilderStatus is null");
        // initialize state for copied block
        outputBlockBuilder = type.createBlockBuilder(pageBuilderStatus.createBlockBuilderStatus(), ids.length);

        // Transfer elements from ids[] to output page builder
        for (int i = 0; i < positionCount; i++) {
            type.appendTo(source, ids[i], outputBlockBuilder);
        }

        // reset state for dictionary block
        ids = new int[0];
        positionCount = 0;

        usingCopiedBlock = true;
    }

    /**
     * This method appends {@code sourcePosition} to ids array.
     *
     * The caller of this method has to ensure the following:
     * - {@code sourcePosition} is a valid index in the source
     * - {@code usingCopiedBlock} is false
     */
    private void appendId(int sourcePosition)
    {
        if (positionCount == ids.length) {
            // grow capacity
            int newSize = calculateNewArraySize(ids.length);
            ids = Arrays.copyOf(ids, newSize);
        }
        ids[positionCount] = sourcePosition;
        positionCount++;
    }

    /**
     * Get number of positions in the output block being prepared
     */
    public int getPositionCount()
    {
        if (usingCopiedBlock) {
            return outputBlockBuilder.getPositionCount();
        }
        return positionCount;
    }

    /**
     * This class checks for the presence of a non-null element in {@code source}, and stores its position.
     * The result is cached with the first invocation of {@link #getNullElementIndex} after the reset. The
     * cache can be invalidated by invoking {@link #resetCheck}.
     */
    private class NullElementFinder
    {
        private boolean checkedForNull;
        private int nullElementPosition = -1;

        void resetCheck()
        {
            this.checkedForNull = false;
            this.nullElementPosition = -1;
        }

        private int getNullElementIndex()
        {
            if (checkedForNull) {
                return nullElementPosition;
            }
            checkForNull();
            return nullElementPosition;
        }

        private void checkForNull()
        {
            nullElementPosition = -1;

            for (int i = 0; i < source.getPositionCount(); i++) {
                if (source.isNull(i)) {
                    nullElementPosition = i;
                    break;
                }
            }

            checkedForNull = true;
        }
    }
}
