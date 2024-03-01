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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.DictionaryBlock;

import javax.annotation.Nullable;

import static com.facebook.presto.operator.unnest.UnnestBlockBuilder.NullElementFinder.NULL_NOT_FOUND;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

/**
 * This class manages the low level details of building unnested blocks with a goal of minimizing data copying
 */
class UnnestBlockBuilder
{
    // checks for existence of null element in the source when required
    private final NullElementFinder nullFinder = new NullElementFinder();

    private Block source;
    private int sourcePosition;
    private Block nullCheckBlock;
    private int nullCheckBlockPosition;

    /**
     * Replaces input source block with {@code block}. The old data structures for output have to be
     * reset as well, because they are based on the source.
     */
    public void resetInputBlock(Block block)
    {
        resetInputBlock(block, null);
    }

    public void resetInputBlock(Block block, @Nullable Block nullCheckBlock)
    {
        this.source = requireNonNull(block, "block is null");
        this.nullFinder.resetCheck(block);
        this.sourcePosition = 0;
        this.nullCheckBlock = nullCheckBlock;
        this.nullCheckBlockPosition = 0;
    }

    public Block buildOutputBlockWithoutNulls(int outputPositionCount)
    {
        Block output = source.getRegion(sourcePosition, outputPositionCount);
        sourcePosition += outputPositionCount;
        return output;
    }

    public Block buildOutputBlockWithNulls(int[] maxLengths, int offset, int length, int outputPositionCount, int[] lengths)
    {
        if (nullFinder.getNullElementIndex() == NULL_NOT_FOUND) {
            source = source.appendNull();
            nullFinder.setNullElementIndex(source.getPositionCount() - 1);
        }

        return buildOutputBlockWithNullsByDictionary(maxLengths, offset, length, outputPositionCount, nullFinder.getNullElementIndex(), lengths);
    }

    protected Block buildOutputBlockWithNullsByDictionary(int[] maxLengths, int offset, int length, int outputPositionCount, int nullIndex, int[] lengths)
    {
        verify(nullIndex != NULL_NOT_FOUND, "nullIndex is -1");

        int position = 0;
        int[] ids = new int[outputPositionCount];

        for (int i = 0; i < length; i++) {
            int entryCount = lengths[offset + i];

            if (nullCheckBlock == null) {
                for (int j = 0; j < entryCount; j++) {
                    ids[position++] = sourcePosition++;
                }
            }
            else {
                for (int j = 0; j < entryCount; j++) {
                    if (nullCheckBlock.isNull(nullCheckBlockPosition++)) {
                        ids[position++] = nullIndex;
                    }
                    else {
                        ids[position++] = sourcePosition++;
                    }
                }
            }

            int maxEntryCount = maxLengths[offset + i];
            for (int j = entryCount; j < maxEntryCount; j++) {
                ids[position++] = nullIndex;
            }
        }

        return new DictionaryBlock(outputPositionCount, source, ids);
    }

    /**
     * This class checks for the presence of a non-null element in {@code source}, and stores its position.
     * The result is cached with the first invocation of {@link #getNullElementIndex} after the reset. The
     * cache can be invalidated by invoking {@link #resetCheck}.
     */
    static class NullElementFinder
    {
        static final int NULL_NOT_FOUND = -1;

        private boolean checkedForNull;
        private int nullElementPosition = NULL_NOT_FOUND;
        private Block source;

        void resetCheck(Block source)
        {
            this.checkedForNull = false;
            this.nullElementPosition = NULL_NOT_FOUND;
            this.source = requireNonNull(source);
        }

        public int getNullElementIndex()
        {
            if (checkedForNull) {
                return nullElementPosition;
            }
            checkForNull();
            return nullElementPosition;
        }

        public void setNullElementIndex(int nullElementPosition)
        {
            this.nullElementPosition = nullElementPosition;
            this.checkedForNull = true;
        }

        private void checkForNull()
        {
            nullElementPosition = NULL_NOT_FOUND;
            checkedForNull = true;

            if (!source.mayHaveNull()) {
                return;
            }

            for (int i = 0; i < source.getPositionCount(); i++) {
                if (source.isNull(i)) {
                    nullElementPosition = i;
                    break;
                }
            }
        }
    }
}
