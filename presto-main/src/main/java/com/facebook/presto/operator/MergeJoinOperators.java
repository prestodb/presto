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
import com.facebook.presto.spi.plan.PlanNodeId;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public class MergeJoinOperators
{
    @Inject
    public MergeJoinOperators()
    {
    }

    public OperatorFactory innerJoin(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinSourceManager mergeJoinSourceManager,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels)
    {
        return createMergeJoinFactory(
                operatorId,
                planNodeId,
                mergeJoinSourceManager,
                leftTypes,
                leftOutputChannels,
                rightTypes,
                rightOutputChannels,
                leftJoinChannels,
                rightJoinChannels,
                new InnerJoiner());
    }

    public OperatorFactory leftJoin(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinSourceManager mergeJoinSourceManager,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels)
    {
        return createMergeJoinFactory(
                operatorId,
                planNodeId,
                mergeJoinSourceManager,
                leftTypes,
                leftOutputChannels,
                rightTypes,
                rightOutputChannels,
                leftJoinChannels,
                rightJoinChannels,
                new LeftJoiner());
    }

    private OperatorFactory createMergeJoinFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinSourceManager mergeJoinSourceManager,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            MergeJoiner mergeJoiner)
    {
        return new MergeJoinOperatorFactory(
                operatorId,
                planNodeId,
                mergeJoinSourceManager,
                leftTypes,
                leftOutputChannels,
                rightTypes,
                rightOutputChannels,
                leftJoinChannels,
                rightJoinChannels,
                mergeJoiner);
    }

    public interface MergeJoiner
    {
        default void joinRow(MergeJoinPageBuilder pageBuilder, Optional<Page> leftPage, int leftPosition, Optional<Page> rightPage, int rightPosition) {
            checkArgument(leftPage.isPresent() || rightPage.isPresent(), "leftPage and rightPage can't be empty at the same time for Merge join");
        }
    }

    public static class InnerJoiner
            implements MergeJoiner
    {
        @Override
        public void joinRow(MergeJoinPageBuilder pageBuilder, Optional<Page> leftPage, int leftPosition, Optional<Page> rightPage, int rightPosition)
        {
            if (leftPage.isPresent() && rightPage.isPresent()) {
                pageBuilder.appendRow(leftPage, leftPosition, rightPage, rightPosition);
            }
        }
    }

    public static class LeftJoiner
            implements MergeJoiner
    {
        @Override
        public void joinRow(MergeJoinPageBuilder pageBuilder, Optional<Page> leftPage, int leftPosition, Optional<Page> rightPage, int rightPosition)
        {
            if (leftPage.isPresent()) {
                pageBuilder.appendRow(leftPage, leftPosition, rightPage, rightPosition);
            }
        }
    }
}
