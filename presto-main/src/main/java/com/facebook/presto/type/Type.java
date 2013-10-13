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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.spi.ColumnType;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

public interface Type
{
    String getName();

    Object getObjectValue(Slice slice, int offset);

    boolean equals(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    boolean equals(Slice leftSlice, int leftOffset, BlockCursor rightCursor);

    int hashCode(Slice slice, int offset);

    int compareTo(Slice leftSlice, int leftOffset, Slice rightSlice, int rightOffset);

    void appendTo(Slice slice, int offset, BlockBuilder blockBuilder);

    void appendTo(Slice slice, int offset, SliceOutput sliceOutput);

    ColumnType toColumnType();
}
