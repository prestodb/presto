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
package io.prestosql.tests.tpch;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.type.Type;

import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

// TODO merge this with MaterializedResult
class MaterializedTupleRecordSet
        implements RecordSet
{
    private final Iterable<MaterializedTuple> tuples;
    private final List<Type> types;

    public MaterializedTupleRecordSet(Iterable<MaterializedTuple> tuples, List<Type> types)
    {
        this.tuples = requireNonNull(tuples, "tuples is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return types;
    }

    @Override
    public RecordCursor cursor()
    {
        return new MaterializedTupleRecordCursor(tuples.iterator(), types);
    }

    private static class MaterializedTupleRecordCursor
            implements RecordCursor
    {
        private final Iterator<MaterializedTuple> iterator;
        private final List<Type> types;
        private MaterializedTuple outputTuple;

        private MaterializedTupleRecordCursor(Iterator<MaterializedTuple> iterator, List<Type> types)
        {
            this.iterator = requireNonNull(iterator, "iterator is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return types.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (!iterator.hasNext()) {
                return false;
            }
            outputTuple = iterator.next();
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return (Boolean) outputTuple.getValues().get(field);
        }

        @Override
        public long getLong(int field)
        {
            return (Long) outputTuple.getValues().get(field);
        }

        @Override
        public double getDouble(int field)
        {
            return (Double) outputTuple.getValues().get(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return Slices.utf8Slice((String) outputTuple.getValues().get(field));
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            return outputTuple.getValues().get(field) == null;
        }

        @Override
        public void close()
        {
        }
    }
}
