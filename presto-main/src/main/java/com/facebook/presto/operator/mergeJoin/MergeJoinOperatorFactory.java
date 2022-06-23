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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MergeJoinOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> leftTypes;
    private final List<Integer> leftOutputChannels;
    private final List<Type> rightTypes;
    private final List<Integer> rightOutputChannels;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final MergeJoinSourceManager mergeJoinSourceManager;
    private final MergeJoinOperators.MergeJoiner mergeJoiner;
    private boolean closed;

    public MergeJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinSourceManager mergeJoinSourceManager,
            List<Type> leftTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            MergeJoinOperators.MergeJoiner mergeJoiner)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.leftTypes = ImmutableList.copyOf(requireNonNull(leftTypes, "leftTypes is null"));
        this.leftOutputChannels = ImmutableList.copyOf(requireNonNull(leftOutputChannels, "leftOutputChannels is null"));
        this.rightTypes = ImmutableList.copyOf(requireNonNull(rightTypes, "rightTypes is null"));
        this.rightOutputChannels = ImmutableList.copyOf(requireNonNull(rightOutputChannels, "rightOutputChannels is null"));
        this.leftJoinChannels = ImmutableList.copyOf(requireNonNull(leftJoinChannels, " is null"));
        this.rightJoinChannels = ImmutableList.copyOf(requireNonNull(rightJoinChannels, " is null"));
        this.mergeJoinSourceManager = requireNonNull(mergeJoinSourceManager, "mergeJoinSourceManager is null");
        this.mergeJoiner = requireNonNull(mergeJoiner, "mergeJoiner is null");
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MergeJoinOperator.class.getSimpleName());
        RightPageSource rightPageSource = mergeJoinSourceManager.getMergeJoinSource(driverContext.getLifespan(), null);
        return new MergeJoinOperator(
                operatorContext,
                leftTypes,
                leftOutputChannels,
                rightTypes,
                rightOutputChannels,
                leftJoinChannels,
                rightJoinChannels,
                rightPageSource,
                mergeJoiner);
    }

    @Override
    public void noMoreOperators()
    {
        checkState(!closed);
        closed = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        throw new UnsupportedOperationException("MergeJoin can not be duplicated.");
    }
}
