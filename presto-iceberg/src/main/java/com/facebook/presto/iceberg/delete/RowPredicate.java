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
package com.facebook.presto.iceberg.delete;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;

import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public interface RowPredicate
{
    boolean test(Page page, int position);

    default RowPredicate and(RowPredicate other)
    {
        requireNonNull(other, "other is null");
        return (page, position) -> test(page, position) && other.test(page, position);
    }

    default Page filterPage(Page page)
    {
        int positionCount = page.getPositionCount();
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (test(page, position)) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount == positionCount) {
            return page;
        }
        return page.getPositions(retained, 0, retainedCount);
    }

    default Page markPage(Page page, int deletedDelegateColumnId)
    {
        int positionCount = page.getPositionCount();

        boolean allSameValues = true;
        Boolean firstValue = null;
        BlockBuilder blockBuilder = BOOLEAN.createFixedSizeBlockBuilder(positionCount);
        for (int position = 0; position < positionCount; position++) {
            boolean deleted = !test(page, position);
            if (firstValue == null) {
                firstValue = deleted;
            }
            if (deleted != firstValue) {
                allSameValues = false;
            }
            BOOLEAN.writeBoolean(blockBuilder, deleted);
        }

        Block block;
        if (allSameValues) {
            block = RunLengthEncodedBlock.create(BOOLEAN, firstValue != null ? firstValue : false, positionCount);
        }
        else {
            block = blockBuilder.build();
        }

        return page.replaceColumn(deletedDelegateColumnId, block);
    }
}
