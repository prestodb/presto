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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.util.array.BooleanBigArray;
import com.facebook.presto.util.array.DoubleBigArray;

import static com.google.common.base.Preconditions.checkArgument;

public final class GroupedNullableDoubleState
        implements NullableDoubleState, GroupedAccumulatorState
{
    private final BooleanBigArray isNotNull;
    private final DoubleBigArray values;
    private final double defaultDouble;
    private long groupId;

    public GroupedNullableDoubleState(NullableDoubleState defaultState)
    {
        checkArgument(!defaultState.isNotNull(), "true default for boolean not supported");
        this.isNotNull = new BooleanBigArray();
        this.values = new DoubleBigArray(defaultState.getDouble());
        this.defaultDouble = defaultState.getDouble();
    }

    @Override
    public void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    @Override
    public double getDouble()
    {
        return values.get(groupId);
    }

    @Override
    public void setDouble(double value)
    {
        this.values.set(groupId, value);
    }

    @Override
    public boolean isNotNull()
    {
        return isNotNull.get(groupId);
    }

    @Override
    public void setNotNull(boolean value)
    {
        this.isNotNull.set(groupId, value);
    }

    @Override
    public long getEstimatedSize()
    {
        return values.sizeOf() + isNotNull.sizeOf();
    }

    @Override
    public void ensureCapacity(long size)
    {
        values.ensureCapacity(size, defaultDouble);
        isNotNull.ensureCapacity(size);
    }
}
