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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.operator.MergeJoinUtil.advanceRightToEndOfMatch;
import static com.facebook.presto.operator.MergeJoinUtil.compareLeftAndRightPosition;
import static com.facebook.presto.operator.MergeJoinUtil.consumeNulls;
import static com.facebook.presto.operator.MergeJoinUtil.findMatchingRightPositions;
import static java.util.Objects.requireNonNull;

public class MergeJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> leftTypes;
    private final List<Type> rightTypes;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final MergeJoinSource mergeJoinSource;
    private final MergeJoinOperators.MergeJoiner mergeJoiner;
    private final MergeJoinRightPages mergeJoinRightPages;
    private final MergeJoinPageBuilder pageBuilder;
    private Iterator<Page> rightPagesIterator;
    private Page leftPage;
    private boolean finishing;
    private boolean noMoreRightInput;
    private int leftPosition;

    public MergeJoinOperator(
            OperatorContext operatorContext,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            MergeJoinSource mergeJoinSource,
            MergeJoinOperators.MergeJoiner mergeJoiner)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.leftTypes = requireNonNull(leftTypes, "leftTypes is null");
        this.rightTypes = requireNonNull(rightTypes, "rightTypes is null");
        this.leftJoinChannels = requireNonNull(leftJoinChannels, "leftJoinChannels is null");
        this.rightJoinChannels = requireNonNull(rightJoinChannels, "rightJoinChannels is null");
        requireNonNull(leftOutputChannels, "leftOutputChannels is null");
        requireNonNull(rightOutputChannels, "rightOutputChannels is null");
        this.pageBuilder = new MergeJoinPageBuilder(leftTypes, leftOutputChannels, rightTypes, rightOutputChannels);
        this.mergeJoinRightPages = new MergeJoinRightPages(rightTypes, rightJoinChannels, rightOutputChannels);
        this.mergeJoinSource = requireNonNull(mergeJoinSource, "mergeJoinSource is null");
        this.mergeJoiner = requireNonNull(mergeJoiner, "mergeJoiner is null");
        this.rightPagesIterator = null;
        this.leftPosition = 0;
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
        return mergeJoinSource.getConsumerFuture();
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
        pageBuilder.reset();
        mergeJoinSource.close();
    }

    @Override
    public Page getOutput()
    {
        // Make sure to have isBlocked or needsInput as valid when returning null output, else driver
        // assumes operator is finished

        // finishing is the no-more-inputs-on-the-left indicator, noMoreRightInput is for the right side
        while (true) {
            Page output = doGetOutput();
            if (output != null) {
                return output;
            }

            // check if we need more right side input
            if (!noMoreRightInput && this.mergeJoinSource.getConsumerFuture().isDone() && mergeJoinSource.peekPage() == null) {
                if (!mergeJoinSource.isFinishing()) {
                    return null;
                }
                noMoreRightInput = true;
                continue;
            }
            return null;
        }
    }

    private Page doGetOutput()
    {
        // ran out of space during previous output
        if (rightPagesIterator != null) {
            Page output = joinAllLeftMatches();
            if (output != null) {
                return output;
            }
        }

        // in the middle of right match, find more matches in next right page
        if (!mergeJoinRightPages.isEmpty() && !advanceRightToEndOfMatch(mergeJoinSource, mergeJoinRightPages, noMoreRightInput)) {
            // needs more input to find end of match
            return null;
        }

        // no output in progress, but the leftPosition can match right side
        if (leftPage != null && !mergeJoinRightPages.isEmpty()) {
            Page output = joinAllLeftMatches();
            if (output != null) {
                return output;
            }
            else if (leftPage == null) {
                return null;
            }
        }

        // if at end, consume all remaining positions
        if (leftPage == null || mergeJoinSource.peekPage() == null) {
            if (leftPage != null && noMoreRightInput) {
                return joinRemainingLeftPositions();
            }
            if (finishing && !pageBuilder.isEmpty()) {
                return buildOutput();
            }
            return null;
        }

        // iterate more here to reduce number of returns
        while (leftPage != null) {
            // look for a new match starting at leftPosition on the left and rightPosition on the right
            int compareResult = compare();

            while (compareResult < 0) {
                mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.empty(), -1);
                if (advanceLeft() == leftPage.getPositionCount()) {
                    resetLeft();
                    return null;
                }
                compareResult = compare();
            }

            while (compareResult > 0) {
                mergeJoiner.joinRow(pageBuilder, Optional.empty(), -1, Optional.of(mergeJoinSource.peekPage()), mergeJoinSource.getCurrentPosition());
                if (mergeJoinSource.advancePosition() == mergeJoinSource.peekPage().getPositionCount()) {
                    mergeJoinSource.releasePage();
                    return null;
                }
                compareResult = compare();
            }

            // advance through any join key matches that have nulls
            while (consumeNulls(leftJoinChannels, leftPage, leftPosition, this::advanceLeft, mergeJoinSource::advancePosition)) {
                compareResult = compare();
            }

            if (compareResult == 0) {
                // find all matching right positions
                mergeJoinRightPages.startMatch(mergeJoinSource.peekPage(), mergeJoinSource.getCurrentPosition());
                int endRightMatchPosition = findMatchingRightPositions(rightTypes, rightJoinChannels, mergeJoinSource);
                mergeJoinRightPages.addPage(mergeJoinSource.peekPage(), mergeJoinSource.getCurrentPosition(), endRightMatchPosition);

                if (!mergeJoinRightPages.isComplete()) {
                    mergeJoinSource.releasePage();
                    return null;
                }

                mergeJoinSource.updatePosition(endRightMatchPosition);
                Page output = joinAllLeftMatches();
                if (output != null) {
                    return output;
                }
            }
        }
        return null;
    }

    private void resetLeft()
    {
        leftPage = null;
    }

    private int advanceLeft()
    {
        leftPosition++;
        return leftPosition;
    }

    private Page joinRemainingLeftPositions()
    {
        while (true) {
            if (pageBuilder.isFull()) {
                return buildOutput();
            }
            mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.empty(), -1);
            if (advanceLeft() == leftPage.getPositionCount()) {
                resetLeft();
                return null;
            }
        }
    }

    private Page joinAllLeftMatches()
    {
        while (mergeJoinRightPages.isPositionMatch(leftPage, leftPosition)) {
            if (rightPagesIterator == null) {
                rightPagesIterator = mergeJoinRightPages.getPages();
            }
            for (Iterator<Page> it = rightPagesIterator; it.hasNext(); ) {
                Page page = it.next();
                for (int i = 0; i < page.getPositionCount(); i++) {
                    mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.of(page), i);
                }
                if (pageBuilder.isFull()) {
                    return buildOutput();
                }
            }
            rightPagesIterator = null;

            if (pageBuilder.isFull()) {
                return buildOutput();
            }
            if (advanceLeft() == leftPage.getPositionCount()) {
                resetLeft();
                return null;
            }
        }
        return null;
    }

    private Page buildOutput()
    {
        Page output = pageBuilder.build();
        pageBuilder.reset();
        return output;
    }

    // compare rows on the left and right at leftPosition and rightPosition respectively
    private int compare()
    {
        return compareLeftAndRightPosition(leftTypes, leftJoinChannels, leftPage, leftPosition, rightJoinChannels, mergeJoinSource.peekPage(), mergeJoinSource.getCurrentPosition());
    }
}
