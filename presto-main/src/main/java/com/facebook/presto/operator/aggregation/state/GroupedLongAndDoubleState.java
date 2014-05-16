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

import com.facebook.presto.util.array.DoubleBigArray;
import com.facebook.presto.util.array.LongBigArray;

public final class GroupedLongAndDoubleState
        implements LongAndDoubleState, GroupedAccumulatorState
{
    private final LongBigArray longValues;
    private final DoubleBigArray doubleValues;
    private final long defaultLong;
    private final double defaultDouble;
    private long groupId;

    public GroupedLongAndDoubleState(LongAndDoubleState defaultState)
    {
        this.doubleValues = new DoubleBigArray(defaultState.getDouble());
        this.defaultDouble = defaultState.getDouble();
        this.longValues = new LongBigArray(defaultState.getLong());
        this.defaultLong = defaultState.getLong();
    }

    @Override
    public void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    @Override
    public long getLong()
    {
        return longValues.get(groupId);
    }

    @Override
    public void setLong(long value)
    {
        longValues.set(groupId, value);
    }

    @Override
    public double getDouble()
    {
        return doubleValues.get(groupId);
    }

    @Override
    public void setDouble(double value)
    {
        doubleValues.set(groupId, value);
    }

    @Override
    public long getEstimatedSize()
    {
        return doubleValues.sizeOf() + longValues.sizeOf();
    }

    @Override
    public void ensureCapacity(long size)
    {
        doubleValues.ensureCapacity(size, defaultDouble);
        longValues.ensureCapacity(size, defaultLong);
    }
}
