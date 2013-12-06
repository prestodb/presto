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
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class MaterializedResult
{
    public static final int DEFAULT_PRECISION = 5;

    private final List<MaterializedTuple> tuples;
    private final List<TupleInfo> tupleInfos;

    public MaterializedResult(List<MaterializedTuple> tuples, List<TupleInfo> tupleInfos)
    {
        this.tuples = ImmutableList.copyOf(checkNotNull(tuples, "tuples is null"));
        this.tupleInfos = ImmutableList.copyOf(checkNotNull(tupleInfos, "tupleInfos is null"));
    }

    public List<MaterializedTuple> getMaterializedTuples()
    {
        return tuples;
    }

    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
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
        return Objects.equal(tupleInfos, o.tupleInfos) &&
                Objects.equal(tuples, o.tuples);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(tuples, tupleInfos);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("tuples", tuples)
                .add("types", tupleInfos)
                .toString();
    }

    public static Builder resultBuilder(Type... types)
    {
        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (Type type : types) {
            tupleInfos.add(new TupleInfo(type));
        }
        return resultBuilder(tupleInfos.build());
    }

    public static Builder resultBuilder(TupleInfo... tupleInfos)
    {
        return resultBuilder(ImmutableList.copyOf(tupleInfos));
    }

    public static Builder resultBuilder(List<TupleInfo> types)
    {
        return new Builder(ImmutableList.copyOf(types));
    }

    public static class Builder
    {
        private final List<TupleInfo> tupleInfos;
        private final ImmutableList.Builder<MaterializedTuple> tuples = ImmutableList.builder();

        Builder(List<TupleInfo> tupleInfos)
        {
            this.tupleInfos = ImmutableList.copyOf(tupleInfos);
        }

        public Builder row(Object... values)
        {
            tuples.add(new MaterializedTuple(DEFAULT_PRECISION, values));
            return this;
        }

        public Builder page(Page page)
        {
            checkNotNull(page, "page is null");
            checkArgument(page.getChannelCount() == tupleInfos.size(), "Expected a page with %s columns, but got %s columns", page.getChannelCount(), tupleInfos.size());

            List<BlockCursor> cursors = new ArrayList<>();
            for (Block block : page.getBlocks()) {
                cursors.add(block.cursor());
            }

            while (true) {
                List<Object> values = new ArrayList<>(tupleInfos.size());
                for (BlockCursor cursor : cursors) {
                    if (cursor.advanceNextPosition()) {
                        values.add(Iterables.getOnlyElement(cursor.getTuple().toValues()));
                    }
                    else {
                        checkState(values.isEmpty(), "unaligned cursors");
                    }
                }
                if (values.isEmpty()) {
                    return this;
                }
                tuples.add(new MaterializedTuple(DEFAULT_PRECISION, values));
            }
        }

        public MaterializedResult build()
        {
            return new MaterializedResult(tuples.build(), tupleInfos);
        }
    }
}
