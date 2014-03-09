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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.FixedWidthType;
import io.airlift.slice.Slice;

import java.util.Objects;

public class FixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private final Slice slice;
    private final int positionCount;

    public FixedWidthBlock(FixedWidthType type, int positionCount, Slice slice)
    {
        super(type);

        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        this.slice = Objects.requireNonNull(slice, "slice is null");
    }

    @Override
    protected Slice getRawSlice()
    {
        return slice;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("FixedWidthBlock{");
        sb.append("positionCount=").append(positionCount);
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }
}
