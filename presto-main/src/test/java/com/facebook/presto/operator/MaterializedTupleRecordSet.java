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
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class MaterializedTupleRecordSet
            implements RecordSet
    {
        private final Iterable<MaterializedTuple> tuples;
        private final List<Type> types;

        public MaterializedTupleRecordSet(Iterable<MaterializedTuple> tuples, List<Type> types)
        {
            this.tuples = checkNotNull(tuples, "tuples is null");
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
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
                this.iterator = checkNotNull(iterator, "iterator is null");
                this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            }

            @Override
            public long getTotalBytes()
            {
                return 0;
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
            public byte[] getString(int field)
            {
                return ((String) outputTuple.getValues().get(field)).getBytes(StandardCharsets.UTF_8);
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
