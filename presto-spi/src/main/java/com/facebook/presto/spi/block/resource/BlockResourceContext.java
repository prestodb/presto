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
package com.facebook.presto.spi.block.resource;

import com.facebook.presto.spi.block.array.BooleanArray;
import com.facebook.presto.spi.block.array.IntArray;
import com.facebook.presto.spi.block.array.LongArrayList;
import io.airlift.slice.Slice;

public interface BlockResourceContext
{

    BooleanArray newBooleanArray(int size);

    BooleanArray copyOfRangeBooleanArray(BooleanArray booleanArray, int positionOffset, int length);

    IntArray newIntArray(int size);

    IntArray copyOfRangeIntArray(IntArray intArray, int positionOffset, int length);

    Slice newSlice(int length);

    Slice copyOfSlice(Slice slice, int offset, int length);

    Slice copyOfSlice(Slice slice);

    LongArrayList newLongArrayList(int expectedPositions);

}
