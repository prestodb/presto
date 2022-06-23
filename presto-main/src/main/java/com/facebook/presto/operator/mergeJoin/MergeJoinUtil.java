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
package com.facebook.presto.operator.mergeJoin;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;

import java.util.List;

public class MergeJoinUtil
{
    private MergeJoinUtil()
    {
    }

    public static boolean consumeNulls(List<Integer> leftJoinChannels, Page left, int leftPosition, Runnable advanceLeft, Runnable advanceRight)
    {
        boolean joinKeyIsNull = leftJoinChannels.stream().anyMatch(channel -> left.getBlock(channel).isNull(leftPosition));
        if (joinKeyIsNull) {
            advanceLeft.run();
            advanceRight.run();
            return true;
        }
        return false;
    }

    public static int compare(List<Type> types, List<Integer> joinChannels, Page batch, int position, List<Integer> otherJoinChannels, Page otherBatch, int otherPosition)
    {
        for (int i = 0; i < joinChannels.size(); i++) {
            int channel = joinChannels.get(i);
            int otherChannel = otherJoinChannels.get(i);
            int compare = types.get(channel).compareTo(batch.getBlock(channel), position, otherBatch.getBlock(otherChannel), otherPosition);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    public static int findMatchingRightPositions(List<Type> rightTypes, List<Integer> rightJoinChannels, RightPageSource rightPageSource)
    {
        int endRightPosition = rightPageSource.getCurrentPageOffset() + 1;
        while (endRightPosition < rightPageSource.getCurrentPage().getPositionCount()
                && compareRight(rightTypes, rightJoinChannels, rightPageSource, endRightPosition) == 0) {
            ++endRightPosition;
        }
        return endRightPosition;
    }

    // compare two rows on the right page: rightPosition and position
    private static int compareRight(List<Type> rightTypes, List<Integer> rightJoinChannels, RightPageSource rightPageSource, int position)
    {
        return compare(rightTypes, rightJoinChannels, rightPageSource.getCurrentPage(), rightPageSource.getCurrentPageOffset(), rightJoinChannels, rightPageSource.getCurrentPage(), position);
    }
}
