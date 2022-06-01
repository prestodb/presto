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
                new MergeInnerJoiner());
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
                new MergeLeftJoiner());
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
        void joinInnerRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition, Page right, int rightPosition);

        void joinLeftRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition);

        void joinRightRow(MergeJoinPageBuilder pageBuilder, Page right, int rightPosition);
    }

    public static class MergeInnerJoiner
            implements MergeJoiner
    {
        @Override
        public void joinInnerRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition, Page right, int rightPosition)
        {
            pageBuilder.appendRow(left, leftPosition, right, rightPosition);
        }

        @Override
        public void joinLeftRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition)
        {
        }

        @Override
        public void joinRightRow(MergeJoinPageBuilder pageBuilder, Page right, int rightPosition)
        {
        }
    }

    public static class MergeLeftJoiner
            implements MergeJoiner
    {
        @Override
        public void joinInnerRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition, Page right, int rightPosition)
        {
            pageBuilder.appendRow(left, leftPosition, right, rightPosition);
        }

        @Override
        public void joinLeftRow(MergeJoinPageBuilder pageBuilder, Page left, int leftPosition)
        {
            pageBuilder.appendRowWithNullRight(left, leftPosition);
        }

        @Override
        public void joinRightRow(MergeJoinPageBuilder pageBuilder, Page right, int rightPosition)
        {
        }
    }
}
