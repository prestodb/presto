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
package com.facebook.presto.common.block;

import java.util.Arrays;

import static com.facebook.presto.common.block.ClosingBlockLease.newLease;
import static java.util.Objects.requireNonNull;

public class BlockFlattener
{
    private final ArrayAllocator allocator;

    public BlockFlattener(ArrayAllocator allocator)
    {
        this.allocator = requireNonNull(allocator, "allocator is null");
    }

    /**
     * Flattens {@code RunLengthEncodedBlock} and {@code DictionaryBlock} into the top-level type
     * such that the inner block is not itself a run length encoded block or dictionary block.
     * For example, a dictionary block which consists of another dictionary block would be flattened
     * to just a dictionary block which has an inner data block of the child dictionary, with all of
     * the ids remapped accordingly.
     */
    public BlockLease flatten(Block block)
    {
        requireNonNull(block, "block is null");
        if (block instanceof DictionaryBlock) {
            return flattenDictionaryBlock((DictionaryBlock) block);
        }
        if (block instanceof RunLengthEncodedBlock) {
            return flattenRunLengthEncodedBlock((RunLengthEncodedBlock) block);
        }
        return newLease(block);
    }

    private BlockLease flattenDictionaryBlock(DictionaryBlock dictionaryBlock)
    {
        Block dictionary = dictionaryBlock.getDictionary();
        int positionCount = dictionaryBlock.getPositionCount();
        int[] currentRemappedIds = dictionaryBlock.getRawIds();
        int currentIdsOffset = dictionaryBlock.getOffsetBase();

        // Initially, the below variable is null.  After the first pass of the loop, it will be a borrowed array from the allocator,
        // and it will have reference equality with currentRemappedIds
        int[] newRemappedIds = null;

        while (true) {
            if (dictionary instanceof DictionaryBlock) {
                dictionaryBlock = (DictionaryBlock) dictionary;
                int[] ids = dictionaryBlock.getRawIds();

                if (newRemappedIds == null) {
                    newRemappedIds = allocator.borrowIntArray(positionCount);
                }

                for (int i = 0; i < positionCount; ++i) {
                    newRemappedIds[i] = ids[currentRemappedIds[i + currentIdsOffset] + dictionaryBlock.getOffsetBase()];
                }

                currentRemappedIds = newRemappedIds;
                currentIdsOffset = 0;
                dictionary = dictionaryBlock.getDictionary();
            }
            else if (dictionary instanceof RunLengthEncodedBlock) {
                RunLengthEncodedBlock rle = (RunLengthEncodedBlock) dictionary;
                if (newRemappedIds == null) {
                    newRemappedIds = allocator.borrowIntArray(positionCount);
                }
                Arrays.fill(newRemappedIds, 0, positionCount, 0);
                currentRemappedIds = newRemappedIds;
                currentIdsOffset = 0;
                dictionary = rle.getValue();
            }
            else {
                // We have reached a data block, break to return the flattened dictionary
                break;
            }
        }

        if (newRemappedIds == null) {
            return newLease(dictionaryBlock);
        }

        dictionary = new DictionaryBlock(positionCount, dictionary, currentRemappedIds);
        int[] leasedMapToReturn = newRemappedIds; // effectively final
        return newLease(dictionary, () -> allocator.returnArray(leasedMapToReturn));
    }

    private static BlockLease flattenRunLengthEncodedBlock(RunLengthEncodedBlock rleBLock)
    {
        Block block = rleBLock;
        while (true) {
            if (block instanceof RunLengthEncodedBlock) {
                RunLengthEncodedBlock rle = (RunLengthEncodedBlock) block;
                block = rle.getValue();
            }
            else if (block instanceof DictionaryBlock) {
                DictionaryBlock dictionaryBlock = (DictionaryBlock) block;
                block = dictionaryBlock.getDictionary().copyRegion(dictionaryBlock.getId(0), 1);
            }
            else {
                // We have reached a data block, break to return the flattened run length encoded block
                break;
            }
        }
        return newLease(new RunLengthEncodedBlock(block, rleBLock.getPositionCount()));
    }
}
