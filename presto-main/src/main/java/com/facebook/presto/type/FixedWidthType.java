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
package com.facebook.presto.type;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

public interface FixedWidthType
        extends Type
{
    int getFixedSize();

    BlockBuilder createFixedSizeBlockBuilder(int positionCount);

    boolean getBoolean(Slice slice, int offset);

    void setBoolean(SliceOutput sliceOutput, boolean value);

    long getLong(Slice slice, int offset);

    void setLong(SliceOutput sliceOutput, long value);

    double getDouble(Slice slice, int offset);

    void setDouble(SliceOutput sliceOutput, double value);

    Slice getSlice(Slice slice, int offset);

    void setSlice(SliceOutput sliceOutput, Slice value, int offset);

    boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor);

    int hashCode(Slice slice, int offset);

    int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    void appendTo(Slice slice, int offset, BlockBuilder blockBuilder);

    void appendTo(Slice slice, int offset, SliceOutput sliceOutput);
}
