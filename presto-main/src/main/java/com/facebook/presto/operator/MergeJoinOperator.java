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
import static com.facebook.presto.operator.MergeJoinUtil.comparePositions;
import static com.facebook.presto.operator.MergeJoinUtil.consumeNulls;
import static java.util.Objects.requireNonNull;

public class MergeJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> leftTypes;
    private final List<Type> rightTypes;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final List<Integer> rightOutputChannels;
    private final MergeJoinSource mergeJoinSource;
    private final MergeJoinOperators.MergeJoiner mergeJoiner;
    private final MergeJoinPageBuilder pageBuilder;
    private Optional<MergeJoinRightMatch> rightMatch;
    private Optional<Iterator<Page>> rightMatchPageCursor;
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
        this.rightOutputChannels = rightOutputChannels;
        this.pageBuilder = new MergeJoinPageBuilder(leftTypes, leftOutputChannels, rightTypes, rightOutputChannels);
        this.rightMatch = Optional.empty();
        this.mergeJoinSource = requireNonNull(mergeJoinSource, "mergeJoinSource is null");
        this.mergeJoiner = requireNonNull(mergeJoiner, "mergeJoiner is null");
        this.rightMatchPageCursor = Optional.empty();
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
        Page output = doGetOutput();
        if (output != null) {
            return output;
        }

        if (!noMoreRightInput
                && mergeJoinSource.getConsumerFuture().isDone()
                && mergeJoinSource.getCurrentPage() == null
                && mergeJoinSource.isFinishing()) {
            noMoreRightInput = true;
        }
        return null;
    }

    private Page doGetOutput()
    {
        // in the middle of finding right match, or ready for output
        if (rightMatch.isPresent()) {
            if (!advanceRightToEndOfMatch(mergeJoinSource, rightMatch.get(), noMoreRightInput)) {
                // needs more input to find end of match
                return null;
            }
            else if (leftPage != null) {
                Page output = joinMatches();
                if (output != null) {
                    return output;
                }
            }
        }

        // if at end, emit all remaining positions
        if (leftPage == null || mergeJoinSource.getCurrentPage() == null) {
            if (leftPage != null && noMoreRightInput) {
                joinLeftPositions();
                return null;
            }
            if (mergeJoinSource.getCurrentPage() != null && finishing) {
                joinRightPositions();
                return null;
            }
            if (finishing && !pageBuilder.isEmpty()) {
                return buildOutput();
            }
            return null;
        }

        // find matching positions and emit join rows
        while (leftPage != null) {
            if (!findMatch()) {
                return null;
            }
            Page output = joinMatches();
            if (output != null) {
                return output;
            }
        }

        return null;
    }

    private boolean findMatch()
    {
        int compareResult = compare();
        while (compareResult < 0) {
            if (joinLeftPositions()) {
                return false;
            }
            compareResult = compare();
        }
        while (compareResult > 0) {
            if (joinRightPositions()) {
                return false;
            }
            compareResult = compare();
        }
        // advance through any join key matches that have nulls
        while (consumeNulls(leftJoinChannels, leftPage, leftPosition, () -> leftPosition++, mergeJoinSource::advancePosition)) {
            compareResult = compare();
        }
        if (compareResult == 0) {
            int startPosition = mergeJoinSource.getCurrentPosition();
            rightMatch = Optional.of(new MergeJoinRightMatch(rightTypes, rightJoinChannels, rightOutputChannels, mergeJoinSource.getCurrentPage().getSingleValuePage(mergeJoinSource.getCurrentPosition())));
            boolean foundEndPosition = mergeJoinSource.locateEndPosition(rightTypes, rightJoinChannels, startPosition);
            rightMatch.get().addPage(mergeJoinSource.getCurrentPage(), startPosition, mergeJoinSource.getCurrentPosition());

            if (!foundEndPosition) {
                mergeJoinSource.releasePage();
                return false;
            }
        }
        return true;
    }

    private Page joinMatches()
    {
        if (!rightMatch.isPresent()) {
            return null;
        }
        while (rightMatch.get().isPositionMatch(leftPage, leftPosition)) {
            Page output = emitMatchedRows();
            if (output != null) {
                return output;
            }
            leftPosition++;
            if (leftPosition == leftPage.getPositionCount()) {
                leftPage = null;
                return null;
            }
        }
        return null;
    }

    private Page emitMatchedRows()
    {
        if (!rightMatchPageCursor.isPresent()) {
            rightMatchPageCursor = Optional.of(rightMatch.get().getPages());
        }
        for (Iterator<Page> it = rightMatchPageCursor.get(); it.hasNext(); ) {
            Page page = it.next();
            for (int i = 0; i < page.getPositionCount(); i++) {
                mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.of(page), i);
            }
            if (pageBuilder.isFull()) {
                return buildOutput();
            }
        }
        rightMatchPageCursor = Optional.empty();
        return null;
    }

    private boolean joinLeftPositions()
    {
        mergeJoiner.joinRow(pageBuilder, Optional.of(leftPage), leftPosition, Optional.empty(), -1);
        leftPosition++;
        if (leftPosition == leftPage.getPositionCount()) {
            leftPage = null;
            return true;
        }
        return false;
    }

    private boolean joinRightPositions()
    {
        mergeJoiner.joinRow(pageBuilder, Optional.empty(), -1, Optional.of(mergeJoinSource.getCurrentPage()), mergeJoinSource.getCurrentPosition());
        mergeJoinSource.advancePosition();
        if (mergeJoinSource.getCurrentPosition() == mergeJoinSource.getCurrentPage().getPositionCount()) {
            mergeJoinSource.releasePage();
            return true;
        }
        return false;
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
        return comparePositions(leftTypes, leftJoinChannels, leftPage, leftPosition, rightJoinChannels, mergeJoinSource.getCurrentPage(), mergeJoinSource.getCurrentPosition());
    }
}
