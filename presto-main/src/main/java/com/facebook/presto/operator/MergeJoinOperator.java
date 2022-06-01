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
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MergeJoinOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final List<Type> leftTypes;
    private final List<Type> leftOutputTypes;
    private final List<Integer> leftOutputChannels;
    private final List<Type> rightTypes;
    private final List<Type> rightOutputTypes;
    private final List<Integer> rightOutputChannels;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;

    final MergeJoinSource mergeJoinSource;
    private final JoinNode.Type joinType;

    private MergeJoinPageBuilder outputPageBuilder;
    private Page leftPage;
    private Page rightPage;

    Optional<Match> leftMatch;
    Optional<Match> rightMatch;

    private boolean finishing;
    private boolean noMoreRightInput;
    private int leftIndex;
    private int rightIndex;

    public MergeJoinOperator(
            OperatorContext operatorContext,
            List<Type> leftTypes,
            List<Type> leftOutputTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Type> rightOutputTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            JoinNode.Type joinType,
            MergeJoinSource mergeJoinSource)
    {
        this.operatorContext = operatorContext;
        this.leftTypes = ImmutableList.copyOf(leftTypes);
        this.leftOutputTypes = ImmutableList.copyOf(leftOutputTypes);
        this.leftOutputChannels = ImmutableList.copyOf(leftOutputChannels);
        this.rightTypes = ImmutableList.copyOf(rightTypes);
        this.rightOutputTypes = ImmutableList.copyOf(rightOutputTypes);
        this.rightOutputChannels = ImmutableList.copyOf(rightOutputChannels);
        this.leftJoinChannels = ImmutableList.copyOf(leftJoinChannels);
        this.rightJoinChannels = ImmutableList.copyOf(rightJoinChannels);
        this.joinType = requireNonNull(joinType);
        this.leftMatch = Optional.empty();
        this.rightMatch = Optional.empty();
        this.leftIndex = 0;
        this.rightIndex = 0;
        this.mergeJoinSource = mergeJoinSource;
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
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        leftPage = page;
        leftIndex = 0;
    }

    private int compare(
            List<Type> types,
            List<Integer> joinChannels,
            Page batch,
            int index,
            List<Type> otherTypes,
            List<Integer> otherJoinChannels,
            Page otherBatch,
            int otherIndex)
    {
        for (int i = 0; i < joinChannels.size(); i++) {
            int channel = joinChannels.get(i);
            int otherChannel = otherJoinChannels.get(i);
            int compare = types.get(channel).compareTo(batch.getBlock(channel), index, otherBatch.getBlock(otherChannel), otherIndex);
            if (compare != 0) {
                return compare;
            }
        }
        return 0;
    }

    // compare two rows from the left side
    private int compareLeft(Page batch, int index, Page otherBatch, int otherIndex)
    {
        return compare(leftTypes, leftJoinChannels, batch, index, leftTypes, leftJoinChannels, otherBatch, otherIndex);
    }

    // compare two rows from the right side
    private int compareRight(Page batch, int index, Page otherBatch, int otherIndex)
    {
        return compare(rightTypes, rightJoinChannels, batch, index, rightTypes, rightJoinChannels, otherBatch, otherIndex);
    }

    // compare rows on the left and right at leftIndex and rightIndex respectively
    private int compare()
    {
        return compare(leftTypes, leftJoinChannels, leftPage, leftIndex, rightTypes, rightJoinChannels, rightPage, rightIndex);
    }

    // compare two rows on the left page: leftIndex and index
    private int compareLeft(int index)
    {
        return compare(leftTypes, leftJoinChannels, leftPage, leftIndex, leftTypes, leftJoinChannels, leftPage, index);
    }

    // compare two rows on the right page: rightIndex and index
    private int compareRight(int index)
    {
        return compare(rightTypes, rightJoinChannels, rightPage, rightIndex, rightTypes, rightJoinChannels, rightPage, index);
    }

    private boolean findEndOfMatch(Match match, Page input, List<Type> types, List<Integer> joinChannels)
    {
        if (match.complete) {
            return true;
        }

        Page prevInput = match.inputs.get(match.inputs.size() - 1);
        int prevIndex = prevInput.getPositionCount() - 1;

        int numInput = input.getPositionCount();

        int endIndex = 0;
        while (endIndex < numInput && compare(types, joinChannels, input, endIndex, types, joinChannels, prevInput, prevIndex) == 0) {
            ++endIndex;
        }

        if (endIndex == numInput) {
            match.inputs.add(input);
            match.endIndex = endIndex;
            return false;
        }

        if (endIndex > 0) {
            match.inputs.add(input);
            match.endIndex = endIndex;
        }

        match.complete = true;
        return true;
    }

    @Override
    public Page getOutput()
    {
        // Make sure to have isBlocked or needsInput as valid when returning null output, else driver
        // assumes operator is finished

        // finishing is the no-more-inputs-on-the-left indicator, noMoreRightInput_ is for the right side
        while (true) {
            Page output = doGetOutput();
            if (output != null) {
                return output;
            }

            // check if we need more right side input
            if (!noMoreRightInput && this.mergeJoinSource.getConsumerFuture().isDone() && rightPage == null) {
                rightPage = mergeJoinSource.nextPage();

                if (rightPage == null && !mergeJoinSource.isFinishing()) {
                    return null;
                }

                if (rightPage != null) {
                    rightIndex = 0;
                }
                else {
                    noMoreRightInput = true;
                }
                continue;
            }

            return null;
        }
    }

    private Page doGetOutput()
    {
        // check if we ran out of space in the output page in the middle of the match
        if (leftMatch.isPresent() && leftMatch.get().getCursor().isPresent()) {
            checkState(rightMatch.isPresent() && rightMatch.get().getCursor().isPresent());

            // continue producing results from the current match
            if (addToOutput()) {
                return buildOutputPage();
            }
        }

        // no output-in-progress match, but there could be incomplete match
        if (leftMatch.isPresent()) {
            checkState(rightMatch.isPresent());

            if (leftPage != null) {
                // look for continuation of match on the left and/or right sides

                if (!findEndOfMatch(leftMatch.get(), leftPage, leftTypes, leftJoinChannels)) {
                    // continue looking for end of the match
                    leftPage = null;
                    return null;
                }

                if (leftMatch.get().inputs.get(leftMatch.get().inputs.size() - 1) == leftPage) {
                    leftIndex = leftMatch.get().endIndex;
                }
            }
            else if (finishing) {
                leftMatch.get().complete = true;
            }
            else {
                // needs more input
                return null;
            }

            if (rightPage != null) {
                if (!findEndOfMatch(rightMatch.get(), rightPage, rightTypes, rightJoinChannels)) {
                    // continue looking for end of the match
                    rightPage = null;
                    return null;
                }

                if (rightMatch.get().inputs.get(rightMatch.get().inputs.size() - 1) == rightPage) {
                    rightIndex = rightMatch.get().endIndex;
                }
            }
            else if (noMoreRightInput) {
                rightMatch.get().complete = true;
            }
            else {
                // needs more input
                return null;
            }
        }

        // there is no output-in-progress match, but there can be a complete match ready for output
        if (leftMatch.isPresent()) {
            checkState(leftMatch.get().complete);
            checkState(rightMatch.isPresent() && rightMatch.get().complete);

            if (addToOutput()) {
                return buildOutputPage();
            }
        }

        if (leftPage == null || rightPage == null) {
            if (joinType == JoinNode.Type.LEFT) {
                if (leftPage != null && noMoreRightInput) {
                    prepareOutput();
                    while (true) {
                        if (outputPageBuilder.isFull()) {
                            return buildOutputPage();
                        }

                        addOutputRowForLeftJoin();

                        ++leftIndex;
                        if (leftIndex == leftPage.getPositionCount()) {
                            // ran out of rows on the left side
                            leftPage = null;
                            return null;
                        }
                    }
                }

                if (finishing && outputPageBuilder != null) {
                    return buildOutputPage();
                }
            }
            else {
                if (finishing || noMoreRightInput) {
                    if (outputPageBuilder != null) {
                        return buildOutputPage();
                    }
                    leftPage = null;
                }
            }

            return null;
        }

        // look for a new match starting with the leftIndex row on the left and rightIndex row on the right
        int compareResult = compare();

        while (true) {
            // catch up leftPage with rightPage
            while (compareResult < 0) {
                if (joinType == JoinNode.Type.LEFT) {
                    prepareOutput();
                    if (outputPageBuilder.isFull()) {
                        return buildOutputPage();
                    }
                    addOutputRowForLeftJoin();
                }

                ++leftIndex;
                if (leftIndex == leftPage.getPositionCount()) {
                    // ran out of rows on the left side
                    leftPage = null;
                    return null;
                }
                compareResult = compare();
            }

            // catchup rightPage with leftPage
            while (compareResult > 0) {
                ++rightIndex;
                if (rightIndex == rightPage.getPositionCount()) {
                    // ran out of rows on the right side
                    rightPage = null;
                    return null;
                }
                compareResult = compare();
            }

            if (compareResult == 0) {
                // found match. identify all rows on the left and right that have matching keys
                int endIndex = leftIndex + 1;
                while (endIndex < leftPage.getPositionCount() && compareLeft(endIndex) == 0) {
                    ++endIndex;
                }
                leftMatch = Optional.of(new Match(leftPage, leftIndex, endIndex, endIndex < leftPage.getPositionCount(), Optional.empty()));

                int endRightIndex = rightIndex + 1;
                while (endRightIndex < rightPage.getPositionCount() && compareRight(endRightIndex) == 0) {
                    ++endRightIndex;
                }
                rightMatch = Optional.of(new Match(rightPage, rightIndex, endRightIndex, endRightIndex < rightPage.getPositionCount(), Optional.empty()));

                if (!leftMatch.get().complete || !rightMatch.get().complete) {
                    if (!leftMatch.get().complete) {
                        // need to continue looking for end of match
                        leftPage = null;
                    }
                    if (!rightMatch.get().complete) {
                        // need to continue looking for end of match
                        rightPage = null;
                    }
                    return null;
                }

                leftIndex = endIndex;
                rightIndex = endRightIndex;

                if (addToOutput()) {
                    return buildOutputPage();
                }
                compareResult = compare();
            }
        }
        // unreachable
    }

    private boolean addToOutput()
    {
        prepareOutput();

        int firstLeftBatch;
        int leftStartIndex;

        if (leftMatch.get().getCursor().isPresent()) {
            firstLeftBatch = leftMatch.get().getCursor().get().batchIndex;
            leftStartIndex = leftMatch.get().getCursor().get().index;
        }
        else {
            firstLeftBatch = 0;
            leftStartIndex = leftMatch.get().startIndex;
        }

        int numLefts = leftMatch.get().inputs.size();
        for (int l = firstLeftBatch; l < numLefts; ++l) {
            Page left = leftMatch.get().inputs.get(l);
            int leftStart = (l == firstLeftBatch) ? leftStartIndex : 0;
            int leftEnd = (l == numLefts - 1) ? leftMatch.get().endIndex : left.getPositionCount();

            for (int i = leftStart; i < leftEnd; ++i) {
                int firstRightBatch = (l == firstLeftBatch && i == leftStart && rightMatch.get().getCursor().isPresent()) ? rightMatch.get().getCursor().get().batchIndex : 0;
                int rightStartIndex = (l == firstLeftBatch && i == leftStart && rightMatch.get().getCursor().isPresent()) ? rightMatch.get().getCursor().get().index : rightMatch.get().startIndex;

                int numRights = rightMatch.get().inputs.size();
                for (int r = firstRightBatch; r < numRights; ++r) {
                    Page right = rightMatch.get().inputs.get(r);
                    int rightStart = (r == firstRightBatch) ? rightStartIndex : 0;
                    int rightEnd = (r == numRights - 1) ? rightMatch.get().endIndex : right.getPositionCount();

                    for (int j = rightStart; j < rightEnd; ++j) {
                        if (outputPageBuilder.isFull()) {
                            leftMatch.get().setCursor(l, i);
                            rightMatch.get().setCursor(r, j);
                            return true;
                        }
                        addOutputRow(left, i, right, j);
                    }
                }
            }
        }

        leftMatch = Optional.empty();
        rightMatch = Optional.empty();

        return outputPageBuilder.isFull();
    }

    private void prepareOutput()
    {
        if (outputPageBuilder == null) {
            outputPageBuilder = new MergeJoinPageBuilder(leftOutputTypes, rightOutputTypes);
        }
    }

    private void addOutputRow(Page left, int leftIndex, Page right, int rightIndex)
    {
        outputPageBuilder.addRow(left, leftOutputChannels, leftIndex, right, rightOutputChannels, rightIndex);
    }

    private void addOutputRowForLeftJoin()
    {
        outputPageBuilder.addRowForLeftJoin(leftPage, leftOutputChannels, leftIndex);
    }

    private Page buildOutputPage()
    {
        Page outputPage = outputPageBuilder.build();
        outputPageBuilder = null;
        return outputPage;
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        boolean noOutputRows = outputPageBuilder == null || outputPageBuilder.isEmpty();
        return this.finishing && leftPage == null && noOutputRows;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (mergeJoinSource.getConsumerFuture() != null) {
            return mergeJoinSource.getConsumerFuture();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void close()
    {
        outputPageBuilder = null;
        mergeJoinSource.close();
    }

    private static class Match
    {
        List<Page> inputs;
        int startIndex;
        int endIndex;
        boolean complete;
        Optional<Cursor> cursor;

        public Match(Page input, int startIndex, int endIndex, boolean complete, Optional<Cursor> cursor)
        {
            requireNonNull(input, "input is null");
            this.inputs = new ArrayList<>();
            inputs.add(input);
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.complete = complete;
            this.cursor = requireNonNull(cursor, "cursor is null");
        }

        public void setCursor(int batchIndex, int index)
        {
            cursor = Optional.of(new Cursor(batchIndex, index));
        }

        public Optional<Cursor> getCursor()
        {
            return cursor;
        }

        private static class Cursor
        {
            private final int batchIndex;
            private final int index;

            public Cursor(int batchIndex, int index)
            {
                this.batchIndex = batchIndex;
                this.index = index;
            }

            public int getBatchIndex()
            {
                return batchIndex;
            }

            public int getIndex()
            {
                return index;
            }
        }
    }
}
