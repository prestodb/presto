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
package com.facebook.presto.operator.index;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Only retains rows that have identical values for each respective fieldSet.
 */
public class FieldSetFilteringRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final List<Set<Integer>> fieldSets;

    public FieldSetFilteringRecordSet(RecordSet delegate, List<Set<Integer>> fieldSets)
    {
        this.delegate = checkNotNull(delegate, "delegate is null");
        this.fieldSets = ImmutableList.copyOf(checkNotNull(fieldSets, "fieldSets is null"));
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return delegate.getColumnTypes();
    }

    @Override
    public RecordCursor cursor()
    {
        return new FieldSetFilteringRecordCursor(delegate.cursor(), fieldSets);
    }

    private static class FieldSetFilteringRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final List<Set<Integer>> fieldSets;

        private FieldSetFilteringRecordCursor(RecordCursor delegate, List<Set<Integer>> fieldSets)
        {
            this.delegate = delegate;
            this.fieldSets = fieldSets;
        }

        @Override
        public long getTotalBytes()
        {
            return delegate.getTotalBytes();
        }

        @Override
        public long getCompletedBytes()
        {
            return delegate.getCompletedBytes();
        }

        @Override
        public long getReadTimeNanos()
        {
            return delegate.getReadTimeNanos();
        }

        @Override
        public Type getType(int field)
        {
            return delegate.getType(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (delegate.advanceNextPosition()) {
                if (fieldSetsEqual(delegate, fieldSets)) {
                    return true;
                }
            }
            return false;
        }

        private static boolean fieldSetsEqual(RecordCursor cursor, List<Set<Integer>> fieldSets)
        {
            for (Set<Integer> fieldSet : fieldSets) {
                if (!fieldsEquals(cursor, fieldSet)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean fieldsEquals(RecordCursor cursor, Set<Integer> fields)
        {
            if (fields.size() < 2) {
                return true; // Nothing to compare
            }
            Iterator<Integer> fieldIterator = fields.iterator();
            int firstField = fieldIterator.next();
            while (fieldIterator.hasNext()) {
                if (!fieldEquals(cursor, firstField, fieldIterator.next())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean fieldEquals(RecordCursor cursor, int field1, int field2)
        {
            checkArgument(cursor.getType(field1).equals(cursor.getType(field2)), "Should only be comparing fields of the same type");

            if (cursor.isNull(field1) || cursor.isNull(field2)) {
                return false;
            }

            Class<?> javaType = cursor.getType(field1).getJavaType();
            if (javaType == long.class) {
                return cursor.getLong(field1) == cursor.getLong(field2);
            }
            else if (javaType == double.class) {
                return cursor.getDouble(field1) == cursor.getDouble(field2);
            }
            else if (javaType == boolean.class) {
                return cursor.getBoolean(field1) == cursor.getBoolean(field2);
            }
            else if (javaType == Slice.class) {
                return cursor.getSlice(field1).equals(cursor.getSlice(field2));
            }
            else {
                throw new IllegalArgumentException("Unknown java type: " + javaType);
            }
        }

        @Override
        public boolean getBoolean(int field)
        {
            return delegate.getBoolean(field);
        }

        @Override
        public long getLong(int field)
        {
            return delegate.getLong(field);
        }

        @Override
        public double getDouble(int field)
        {
            return delegate.getDouble(field);
        }

        @Override
        public Slice getSlice(int field)
        {
            return delegate.getSlice(field);
        }

        @Override
        public boolean isNull(int field)
        {
            return delegate.isNull(field);
        }

        @Override
        public void close()
        {
            delegate.close();
        }
    }
}
