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
import com.facebook.presto.util.array.LongBigArray;

import static com.google.common.base.Preconditions.checkArgument;

public final class GroupedNullableLongState
        implements NullableLongState, GroupedAccumulatorState
{
    private final BooleanBigArray isNull;
    private final LongBigArray values;
    private final long defaultLong;
    private long groupId;

    public GroupedNullableLongState(NullableLongState defaultState)
    {
        checkArgument(!defaultState.isNotNull(), "true default for isNull not supported");
        this.isNull = new BooleanBigArray();
        this.values = new LongBigArray(defaultState.getLong());
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
        return values.get(groupId);
    }

    @Override
    public void setLong(long value)
    {
        this.values.set(groupId, value);
    }

    @Override
    public boolean isNotNull()
    {
        return isNull.get(groupId);
    }

    @Override
    public void setNotNull(boolean value)
    {
        this.isNull.set(groupId, value);
    }

    @Override
    public long getEstimatedSize()
    {
        return values.sizeOf() + isNull.sizeOf();
    }

    @Override
    public void ensureCapacity(long size)
    {
        values.ensureCapacity(size, defaultLong);
        isNull.ensureCapacity(size);
    }
}
