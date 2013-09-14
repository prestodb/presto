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

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class ProjectionFunctions
{
    public static ProjectionFunction singleColumn(Type columnType, int channelIndex, int fieldIndex)
    {
        return new SingleColumnProjection(columnType, channelIndex, fieldIndex);
    }

    public static ProjectionFunction singleColumn(Type columnType, Input input)
    {
        return new SingleColumnProjection(columnType, input.getChannel(), input.getField());
    }

    public static List<TupleInfo> toTupleInfos(List<ProjectionFunction> projections)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (ProjectionFunction projection : projections) {
            tupleInfos.add(projection.getTupleInfo());
        }
        return tupleInfos.build();
    }

    private static class SingleColumnProjection
            implements ProjectionFunction
    {
        private final Type columnType;
        private final int channelIndex;
        private final int fieldIndex;
        private final TupleInfo info;

        public SingleColumnProjection(Type columnType, int channelIndex, int fieldIndex)
        {
            Preconditions.checkNotNull(columnType, "columnType is null");
            Preconditions.checkArgument(channelIndex >= 0, "channelIndex is negative");
            Preconditions.checkArgument(fieldIndex >= 0, "fieldIndex is negative");

            this.columnType = columnType;
            this.channelIndex = channelIndex;
            this.fieldIndex = fieldIndex;
            this.info = new TupleInfo(columnType);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return info;
        }

        @Override
        public void project(TupleReadable[] cursors, BlockBuilder output)
        {
            if (cursors[channelIndex].isNull(fieldIndex)) {
                output.appendNull();
            }
            else {
                switch (columnType) {
                    case BOOLEAN:
                        output.append(cursors[channelIndex].getBoolean(fieldIndex));
                        return;
                    case FIXED_INT_64:
                        output.append(cursors[channelIndex].getLong(fieldIndex));
                        return;
                    case VARIABLE_BINARY:
                        output.append(cursors[channelIndex].getSlice(fieldIndex));
                        return;
                    case DOUBLE:
                        output.append(cursors[channelIndex].getDouble(fieldIndex));
                        return;
                }
                throw new IllegalStateException("Unsupported type info " + info);
            }
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            // record cursors have each value in a separate field
            Preconditions.checkArgument(fieldIndex == 0, "field must be 0 for a record cursor projection");
            if (cursor.isNull(channelIndex)) {
                output.appendNull();
            }
            else {
                switch (columnType) {
                    case BOOLEAN:
                        output.append(cursor.getBoolean(channelIndex));
                        break;
                    case FIXED_INT_64:
                        output.append(cursor.getLong(channelIndex));
                        break;
                    case VARIABLE_BINARY:
                        output.append(cursor.getString(channelIndex));
                        break;
                    case DOUBLE:
                        output.append(cursor.getDouble(channelIndex));
                        break;
                }
            }
        }
    }

    public static ProjectionFunction concat(ProjectionFunction... projectionFunctions)
    {
        return concat(ImmutableList.copyOf(projectionFunctions));
    }

    public static ProjectionFunction concat(Iterable<ProjectionFunction> projections)
    {
        return new ConcatProjection(projections);
    }

    private static class ConcatProjection
            implements ProjectionFunction
    {
        private final List<ProjectionFunction> projections;
        private final TupleInfo tupleInfo;

        private ConcatProjection(Iterable<ProjectionFunction> projections)
        {
            this.projections = ImmutableList.copyOf(projections);

            ImmutableList.Builder<Type> builder = ImmutableList.builder();
            for (ProjectionFunction projection : projections) {
                builder.addAll(projection.getTupleInfo().getTypes());
            }
            this.tupleInfo = new TupleInfo(builder.build());
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public void project(TupleReadable[] cursors, BlockBuilder output)
        {
            for (ProjectionFunction projection : projections) {
                projection.project(cursors, output);
            }
        }

        @Override
        public void project(RecordCursor cursor, BlockBuilder output)
        {
            for (ProjectionFunction projection : projections) {
                projection.project(cursor, output);
            }
        }
    }
}
