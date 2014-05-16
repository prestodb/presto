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

import org.openjdk.jol.info.ClassLayout;

public final class SingleNullableDoubleState
        implements NullableDoubleState
{
    public static final int STATE_SIZE = ClassLayout.parseClass(SingleNullableDoubleState.class).instanceSize();

    private double value;
    private boolean isNotNull;

    @Override
    public double getDouble()
    {
        return value;
    }

    @Override
    public void setDouble(double value)
    {
        this.value = value;
    }

    @Override
    public boolean isNotNull()
    {
        return isNotNull;
    }

    @Override
    public void setNotNull(boolean value)
    {
        this.isNotNull = value;
    }

    @Override
    public long getEstimatedSize()
    {
        return STATE_SIZE;
    }

    @Override
    public void ensureCapacity(long size)
    {
    }
}
