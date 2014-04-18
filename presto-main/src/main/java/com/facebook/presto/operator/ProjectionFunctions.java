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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.Input;
import com.google.common.base.Preconditions;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public final class ProjectionFunctions
{
    private ProjectionFunctions() {}

    public static ProjectionFunction singleColumn(Type columnType, int channelIndex)
    {
        return new SingleColumnProjection(columnType, channelIndex);
    }

    public static ProjectionFunction singleColumn(Type columnType, Input input)
    {
        return new SingleColumnProjection(columnType, input.getChannel());
    }

    private static class SingleColumnProjection
            implements ProjectionFunction
    {
        private final Type columnType;
        private final int channelIndex;

        public SingleColumnProjection(Type columnType, int channelIndex)
        {
            Preconditions.checkNotNull(columnType, "columnType is null");
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
        public void project(BlockCursor[] cursors, BlockBuilder output)
        {
            if (cursors[channelIndex].isNull()) {
                output.appendNull();
            }
            else {
                cursors[channelIndex].appendTo(output);
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
                    output.appendBoolean(cursor.getBoolean(channelIndex));
                }
                else if (javaType == long.class) {
                    output.appendLong(cursor.getLong(channelIndex));
                }
                else if (javaType == double.class) {
                    output.appendDouble(cursor.getDouble(channelIndex));
                }
                else if (javaType == Slice.class) {
                    output.appendSlice(Slices.wrappedBuffer(cursor.getString(channelIndex)));
                }
            }
        }
    }
}
