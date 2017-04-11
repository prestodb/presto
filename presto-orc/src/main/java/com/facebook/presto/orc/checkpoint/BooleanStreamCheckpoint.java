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
package com.facebook.presto.orc.checkpoint;

import com.facebook.presto.orc.checkpoint.Checkpoints.ColumnPositionsList;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class BooleanStreamCheckpoint
        implements StreamCheckpoint
{
    private final int offset;
    private final ByteStreamCheckpoint byteStreamCheckpoint;

    public BooleanStreamCheckpoint(int offset, ByteStreamCheckpoint byteStreamCheckpoint)
    {
        this.offset = offset;
        this.byteStreamCheckpoint = requireNonNull(byteStreamCheckpoint, "byteStreamCheckpoint is null");
    }

    public BooleanStreamCheckpoint(boolean compressed, ColumnPositionsList positionsList)
    {
        byteStreamCheckpoint = new ByteStreamCheckpoint(compressed, positionsList);
        offset = positionsList.nextPosition();
    }

    public int getOffset()
    {
        return offset;
    }

    public ByteStreamCheckpoint getByteStreamCheckpoint()
    {
        return byteStreamCheckpoint;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("offset", offset)
                .add("byteStreamCheckpoint", byteStreamCheckpoint)
                .toString();
    }
}
