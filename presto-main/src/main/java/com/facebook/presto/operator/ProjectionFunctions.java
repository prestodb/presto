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
package com.facebook.presto.operator;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.Set;

import static java.util.Objects.requireNonNull;

public final class ProjectionFunctions
{
    private ProjectionFunctions() {}

    public static ProjectionFunction singleColumn(Type columnType, int channelIndex)
    {
        return new SingleColumnProjection(columnType, channelIndex);
    }

    private static class SingleColumnProjection
            implements ProjectionFunction
    {
        private final Type columnType;
        private final int channelIndex;

        public SingleColumnProjection(Type columnType, int channelIndex)
        {
            requireNonNull(columnType, "columnType is null");
            Preconditions.checkArgument(channelIndex >= 0, "channelIndex is negative");

            this.columnType = columnType;
            this.channelIndex = channelIndex;
        }

        @Override
        public Type getType()
        {
            return columnType;
        }

        @Override
        public void project(int position, Block[] blocks, BlockBuilder output)
        {
            if (blocks[channelIndex].isNull(position)) {
                output.appendNull();
            }
            else {
                columnType.appendTo(blocks[channelIndex], position, output);
            }
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            // record cursors have each value in a separate field
            if (cursor.isNull(channelIndex)) {
                output.appendNull();
            }
            else {
                Class<?> javaType = columnType.getJavaType();
                if (javaType == boolean.class) {
                    columnType.writeBoolean(output, cursor.getBoolean(channelIndex));
                }
                else if (javaType == long.class) {
                    columnType.writeLong(output, cursor.getLong(channelIndex));
                }
                else if (javaType == double.class) {
                    columnType.writeDouble(output, cursor.getDouble(channelIndex));
                }
                else if (javaType == Slice.class) {
                    Slice slice = cursor.getSlice(channelIndex);
                    columnType.writeSlice(output, slice, 0, slice.length());
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented: " + javaType);
                }
            }
        }

        @Override
        public Set<Integer> getInputChannels()
        {
            return ImmutableSet.of(channelIndex);
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }
    }
}
