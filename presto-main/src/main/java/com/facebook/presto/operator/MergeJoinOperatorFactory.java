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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class MergeJoinOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> leftTypes;
    private final List<Type> leftOutputTypes;
    private final List<Integer> leftOutputChannels;
    private final List<Type> rightTypes;
    private final List<Type> rightOutputTypes;
    private final List<Integer> rightOutputChannels;
    private final List<Integer> leftJoinChannels;
    private final List<Integer> rightJoinChannels;
    private final JoinNode.Type mergeJoinType;
    private final MergeJoinSourceManager mergeJoinSourceManager;

    private boolean closed;

    public MergeJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            MergeJoinSourceManager mergeJoinSourceManager,
            List<Type> leftTypes,
            List<Type> leftOutputTypes,
            List<Integer> leftOutputChannels,
            List<Type> rightTypes,
            List<Type> rightOutputTypes,
            List<Integer> rightOutputChannels,
            List<Integer> leftJoinChannels,
            List<Integer> rightJoinChannels,
            JoinNode.Type mergeJoinType)
    {
        this.operatorId = operatorId;
        this.planNodeId = planNodeId;
        this.leftTypes = leftTypes;
        this.leftOutputTypes = leftOutputTypes;
        this.leftOutputChannels = leftOutputChannels;
        this.rightTypes = rightTypes;
        this.rightOutputTypes = rightOutputTypes;
        this.rightOutputChannels = rightOutputChannels;
        this.mergeJoinType = mergeJoinType;
        this.mergeJoinSourceManager = mergeJoinSourceManager;
        this.leftJoinChannels = leftJoinChannels;
        this.rightJoinChannels = rightJoinChannels;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, MergeJoinOperator.class.getSimpleName());
        MergeJoinSource mergeJoinSource = mergeJoinSourceManager.getMergeJoinSource(driverContext.getLifespan(), null);
        return new MergeJoinOperator(operatorContext, leftTypes, leftOutputTypes, leftOutputChannels, rightTypes, rightOutputTypes, rightOutputChannels, leftJoinChannels, rightJoinChannels, mergeJoinType, mergeJoinSource);
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
        return null;
    }
}
