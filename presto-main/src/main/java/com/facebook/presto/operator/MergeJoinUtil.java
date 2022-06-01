package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;

import java.util.List;

public class MergeJoinUtil
{
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

    public static boolean advanceRightToEndOfMatch(MergeJoinSource mergeJoinSource, MergeJoinRightPages mergeJoinRightPages, boolean noMoreRightInput)
    {
        if (mergeJoinSource.peekPage() != null) {
            int endPosition = mergeJoinRightPages.findEndOfMatch(mergeJoinSource.peekPage());
            if (endPosition == mergeJoinSource.peekPage().getPositionCount()) {
                // continue looking for end of the match
                mergeJoinSource.releasePage();
                return false;
            }
            if (endPosition != 0) {
                mergeJoinSource.updatePosition(endPosition);
            }
        }
        else if (noMoreRightInput) {
            mergeJoinRightPages.complete();
        }
        else {
            // needs more input
            return false;
        }
        return true;
    }

    public static int compareLeftAndRightPosition(List<Type> types, List<Integer> joinChannels, Page batch, int position, List<Integer> otherJoinChannels, Page otherBatch, int otherPosition)
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

    public static int findMatchingRightPositions(List<Type> rightTypes, List<Integer> rightJoinChannels, MergeJoinSource mergeJoinSource)
    {
        int endRightPosition = mergeJoinSource.getCurrentPosition() + 1;
        while (endRightPosition < mergeJoinSource.peekPage().getPositionCount()
                && compareRight(rightTypes, rightJoinChannels, mergeJoinSource, endRightPosition) == 0) {
            ++endRightPosition;
        }
        return endRightPosition;
    }

    // compare two rows on the right page: rightPosition and position
    private static int compareRight(List<Type> rightTypes, List<Integer> rightJoinChannels, MergeJoinSource mergeJoinSource, int position)
    {
        return compareLeftAndRightPosition(rightTypes, rightJoinChannels, mergeJoinSource.peekPage(), mergeJoinSource.getCurrentPosition(), rightJoinChannels, mergeJoinSource.peekPage(), position);
    }
}
