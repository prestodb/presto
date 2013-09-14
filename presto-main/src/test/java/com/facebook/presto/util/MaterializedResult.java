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
package com.facebook.presto.util;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;

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

    public static Builder resultBuilder(TupleInfo... tupleInfos)
    {
        return resultBuilder(ImmutableList.copyOf(tupleInfos));
    }

    public static Builder resultBuilder(List<TupleInfo> tupleInfos)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (TupleInfo sourceTupleInfo : checkNotNull(tupleInfos, "sourceTupleInfos is null")) {
            types.addAll(sourceTupleInfo.getTypes());
        }
        return new Builder(new TupleInfo(types.build()));
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

        public Builder page(Page page)
        {
            checkNotNull(page, "page is null");

            List<BlockCursor> cursors = new ArrayList<>();
            for (Block block : page.getBlocks()) {
                cursors.add(block.cursor());
            }

            while (true) {
                boolean hasResults = false;
                for (BlockCursor cursor : cursors) {
                    if (cursor.advanceNextPosition()) {
                        builder.append(cursor.getTuple());
                        hasResults = true;
                    }
                    else {
                        checkState(!hasResults, "unaligned cursors");
                    }
                }
                if (!hasResults) {
                    return this;
                }
            }
        }

        public MaterializedResult build()
        {
            ImmutableList.Builder<Tuple> tuples = ImmutableList.builder();
            if (!builder.isEmpty()) {
                BlockCursor cursor = builder.build().cursor();
                while (cursor.advanceNextPosition()) {
                    tuples.add(cursor.getTuple());
                }
            }
            return new MaterializedResult(tuples.build(), builder.getTupleInfo());
        }

        private void append(Object value)
        {
            if (value == null) {
                builder.appendNull();
            }
            else if (value instanceof Boolean) {
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
}
