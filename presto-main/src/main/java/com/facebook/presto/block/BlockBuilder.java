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
package com.facebook.presto.block;

import com.facebook.presto.tuple.TupleInfo;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

public interface BlockBuilder
        extends RandomAccessBlock
{
    DataSize DEFAULT_MAX_BLOCK_SIZE = new DataSize(64, Unit.KILOBYTE);
    DataSize DEFAULT_INITIAL_BUFFER_SIZE = new DataSize(DEFAULT_MAX_BLOCK_SIZE.toBytes() * 1.2, Unit.BYTE).convertToMostSuccinctDataSize();

    BlockBuilder append(boolean value);

    BlockBuilder append(long value);

    BlockBuilder append(double value);

    BlockBuilder append(byte[] value);

    BlockBuilder append(String value);

    BlockBuilder append(Slice value);

    BlockBuilder append(Slice value, int offset, int length);

    BlockBuilder appendNull();

    BlockBuilder appendObject(Object value);

    BlockBuilder appendTuple(Slice slice, int offset, int length);

    RandomAccessBlock build();

    int getPositionCount();

    TupleInfo getTupleInfo();

    boolean isEmpty();

    boolean isFull();

    int size();

    int writableBytes();
}
