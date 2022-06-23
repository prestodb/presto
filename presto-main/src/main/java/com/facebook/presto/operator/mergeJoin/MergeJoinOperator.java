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
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.mergeJoin.MergeJoinUtil.compare;
import static java.util.Objects.requireNonNull;

public class MergeJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> leftTypes;
    private final List<Type> rightTypes;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final MergeJoinOperators.MergeJoiner mergeJoiner;
    private final MergeJoinPageBuilder pageBuilder;
    private final RightPageSource rightPageSource;
    private final RightMatchingData rightMatchingData;

    private Page leftPage;
    private int leftPosition;
    private boolean finishing;
    private boolean noMoreRightInput;

    public MergeJoinOperator(
            OperatorContext operatorContext,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            RightPageSource rightPageSource,
            MergeJoinOperators.MergeJoiner mergeJoiner)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.leftTypes = requireNonNull(leftTypes, "leftTypes is null");
        this.rightTypes = requireNonNull(rightTypes, "rightTypes is null");
        this.leftJoinChannels = requireNonNull(leftJoinChannels, "leftJoinChannels is null");
        this.rightJoinChannels = requireNonNull(rightJoinChannels, "rightJoinChannels is null");
        requireNonNull(leftOutputChannels, "leftOutputChannels is null");
        requireNonNull(rightOutputChannels, "rightOutputChannels is null");
        this.mergeJoiner = requireNonNull(mergeJoiner, "mergeJoiner is null");
        this.pageBuilder = new MergeJoinPageBuilder(leftTypes, leftOutputChannels, rightTypes, rightOutputChannels);
        this.rightMatchingData = new RightMatchingData(rightTypes, rightJoinChannels, rightOutputChannels);
        this.rightPageSource = requireNonNull(rightPageSource, "mergeJoinSource is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && leftPage == null;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return rightPageSource.getConsumerFuture();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        leftPage = page;
        leftPosition = 0;
    }

    @Override
    public boolean isFinished()
    {
        return finishing && leftPage == null && pageBuilder.isEmpty();
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public void close()
    {
        // rightMatchingData.close();
        rightPageSource.close();
        // pageBuilder.close();
    }

    @Override
    public Page getOutput()
    {
        if (!rightMatchingData.isComplete()) {
            findNextMatch();
        }
        else {
            Page output = join();
            if (output != null) {
                // todo rightMatchingData.clear();
                return output;
            }
        }
        return null;
    }

    private void findNextMatch() {
        while (leftPage != null && rightPageSource.hasData()) {
            int compareResult = compare(leftTypes, leftJoinChannels, leftPage, leftPosition, rightJoinChannels, rightPageSource.getCurrentPage(), rightPageSource.getCurrentPageOffset());
            if (compareResult < 0) {
                // todo improvement: move step by step would be slow
                mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.empty(), -1);
                leftPosition++;
                if (leftPosition == leftPage.getPositionCount()) {
                    leftPage = null;
                    return;
                }
            }
            else if (compareResult > 0) {
                mergeJoiner.joinRow(pageBuilder, Optional.empty(), -1, Optional.of(rightPageSource.getCurrentPage()), rightPageSource.getCurrentPageOffset());
                if (!rightPageSource.hashNext()) {
                    return;
                }
            }
            else {
                int anchorOffset = rightPageSource.getCurrentPageOffset();
                boolean foundMatchingRightEndPosition = rightPageSource.locateEndPosition(rightTypes, rightJoinChannels, anchorOffset);
                rightMatchingData.addPage(rightPageSource.getCurrentPage(), anchorOffset, rightPageSource.getCurrentPageOffset());
                if (foundMatchingRightEndPosition) {
                    rightMatchingData.complete();
                    break;
                }
                if (!rightPageSource.hashNext()) {
                    return;
                }
            }
        }
    }

// compare
// resetLeft
// advanceLeft
// they’re only one liner logic, no need to make them function

    private Page join()
    {
        while (rightMatchingData.isPositionMatch(leftPage, leftPosition)) {
            Page result = joinRight();
            if (result != null) {
                return result;
            }
            leftPosition++;
            if (leftPosition == leftPage.getPositionCount()) {
                leftPage = null;
                return null;
            }
        }
        rightMatchingData.clear();
        return pageBuilder.buildAndReset();
    }

    private Page joinRight()
    {
        while (rightMatchingData.hasNextPage()) {
            Page page = rightMatchingData.getNextPage();
            // todo improvement: move step by step would be slow: mergeJoiner.join(pageBuilder, leftPage, leftPosition, page);
            for (int i = 0; i < page.getPositionCount(); i++) {
                mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.of(page), i);
            }

            if (pageBuilder.isFull()) {
                return pageBuilder.buildAndReset();
            }
        }
        rightMatchingData.reset();
        return null;
    }
}
