/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.block.BlockBuilder;
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

    private static class SingleColumnProjection implements ProjectionFunction
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
    }

    public static ProjectionFunction concat(ProjectionFunction... projectionFunctions)
    {
        return concat(ImmutableList.copyOf(projectionFunctions));
    }

    public static ProjectionFunction concat(Iterable<ProjectionFunction> projections)
    {
        return new ConcatProjection(projections);
    }

    private static class ConcatProjection implements ProjectionFunction
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
    }
}
