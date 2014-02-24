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
package com.facebook.presto.block.uncompressed;

import com.facebook.presto.type.FixedWidthType;
import com.google.common.base.Objects;
import io.airlift.slice.Slice;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FixedWidthBlock
        extends AbstractFixedWidthBlock
{
    private final Slice slice;
    private final int positionCount;

    public FixedWidthBlock(FixedWidthType type, int positionCount, Slice slice)
    {
        super(type);

        checkArgument(positionCount >= 0, "positionCount is negative");
        this.positionCount = positionCount;

        this.slice = checkNotNull(slice, "slice is null");
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
        return Objects.toStringHelper(this)
                .add("positionCount", positionCount)
                .add("slice", slice)
                .toString();
    }
}
