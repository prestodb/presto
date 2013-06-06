package com.facebook.presto.util;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MaterializedResult
{
    private static final int DEFAULT_PRECISION = 5;

    private final List<MaterializedTuple> tuples;
    private final TupleInfo tupleInfo;

    public MaterializedResult(List<Tuple> tuples, TupleInfo tupleInfo, final int precision)
    {
        this.tuples = Lists.transform(checkNotNull(tuples, "tuples is null"), new Function<Tuple, MaterializedTuple>()
        {
            @Override
            public MaterializedTuple apply(Tuple input)
            {
                return new MaterializedTuple(input, precision);
            }
        });
        this.tupleInfo = checkNotNull(tupleInfo, "tupleInfo is null");
    }

    public MaterializedResult(List<Tuple> tuples, TupleInfo tupleInfo)
    {
        this(tuples, tupleInfo, DEFAULT_PRECISION);
    }

    public List<MaterializedTuple> getMaterializedTuples()
    {
        return tuples;
    }

    public TupleInfo getTupleInfo()
    {
        return tupleInfo;
    }

    public static MaterializedResult materialize(Operator operator)
    {
        FilterAndProjectOperator filterAndProjectOperator = new FilterAndProjectOperator(
                operator,
                FilterFunctions.TRUE_FUNCTION,
                new Concat(operator.getTupleInfos()));
        return new MaterializedResult(getTuples(filterAndProjectOperator), Iterables.getOnlyElement(filterAndProjectOperator.getTupleInfos()));
    }

    private static List<Tuple> getTuples(Operator operator)
    {
        ImmutableList.Builder<Tuple> output = ImmutableList.builder();
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            checkState(page.getChannelCount() == 1, "Expected result to produce 1 channel");

            BlockCursor cursor = Iterables.getOnlyElement(Arrays.asList(page.getBlocks())).cursor();
            while (cursor.advanceNextPosition()) {
                output.add(cursor.getTuple());
            }
        }

        return output.build();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        MaterializedResult o = (MaterializedResult) obj;
        return Objects.equal(tupleInfo, o.tupleInfo) &&
                Objects.equal(tuples, o.tuples);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tuples, tupleInfo);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tuples", tuples)
                .add("tupleInfo", tupleInfo)
                .toString();
    }

    public static Builder resultBuilder(TupleInfo.Type... types)
    {
        return resultBuilder(new TupleInfo(types));
    }

    public static Builder resultBuilder(TupleInfo tupleInfo)
    {
        return new Builder(tupleInfo);
    }

    public static class Builder
    {
        private final BlockBuilder builder;

        Builder(TupleInfo tupleInfo)
        {
            this.builder = new BlockBuilder(tupleInfo);
        }

        public Builder row(Object... values)
        {
            for (Object value : values) {
                append(value);
            }
            return this;
        }

        public MaterializedResult build()
        {
            return materialize(createOperator(new Page(builder.build())));
        }

        private void append(Object value)
        {
            if (value instanceof Boolean) {
                builder.append((Boolean) value);
            }
            else if ((value instanceof Long) || (value instanceof Integer)) {
                builder.append(((Number) value).longValue());
            }
            else if (value instanceof Double) {
                builder.append((Double) value);
            }
            else if (value instanceof String) {
                builder.append((String) value);
            }
            else {
                throw new IllegalArgumentException("bad value: " + value.getClass().getName());
            }
        }
    }

    private static class Concat
            implements ProjectionFunction
    {
        private final TupleInfo tupleInfo;

        public Concat(List<TupleInfo> infos)
        {
            List<TupleInfo.Type> types = new ArrayList<>();
            for (TupleInfo info : infos) {
                types.addAll(info.getTypes());
            }

            this.tupleInfo = new TupleInfo(types);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return tupleInfo;
        }

        @Override
        public void project(TupleReadable[] cursors, BlockBuilder output)
        {
            for (TupleReadable cursor : cursors) {
                output.append(cursor.getTuple());
            }
        }
    }
}
