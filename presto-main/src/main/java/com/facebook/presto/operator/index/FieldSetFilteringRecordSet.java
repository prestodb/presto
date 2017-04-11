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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Only retains rows that have identical values for each respective fieldSet.
 */
public class FieldSetFilteringRecordSet
        implements RecordSet
{
    private final RecordSet delegate;
    private final List<Set<Field>> fieldSets;

    public FieldSetFilteringRecordSet(FunctionRegistry functionRegistry, RecordSet delegate, List<Set<Integer>> fieldSets)
    {
        requireNonNull(functionRegistry, "functionRegistry is null");
        this.delegate = requireNonNull(delegate, "delegate is null");

        ImmutableList.Builder<Set<Field>> fieldSetsBuilder = ImmutableList.builder();
        List<Type> columnTypes = delegate.getColumnTypes();
        for (Set<Integer> fieldSet : requireNonNull(fieldSets, "fieldSets is null")) {
            ImmutableSet.Builder<Field> fieldSetBuilder = ImmutableSet.builder();
            for (int field : fieldSet) {
                fieldSetBuilder.add(new Field(
                        field,
                        functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(columnTypes.get(field), columnTypes.get(field)))).getMethodHandle()));
            }
            fieldSetsBuilder.add(fieldSetBuilder.build());
        }
        this.fieldSets = fieldSetsBuilder.build();
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

    private static class Field
    {
        private final int field;
        private final MethodHandle equalsMethodHandle;

        public Field(int field, MethodHandle equalsMethodHandle)
        {
            this.field = field;
            this.equalsMethodHandle = requireNonNull(equalsMethodHandle, "equalsMethodHandle is null");
        }

        public int getField()
        {
            return field;
        }

        public MethodHandle getEqualsMethodHandle()
        {
            return equalsMethodHandle;
        }
    }

    private static class FieldSetFilteringRecordCursor
            implements RecordCursor
    {
        private final RecordCursor delegate;
        private final List<Set<Field>> fieldSets;

        private FieldSetFilteringRecordCursor(RecordCursor delegate, List<Set<Field>> fieldSets)
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

        private static boolean fieldSetsEqual(RecordCursor cursor, List<Set<Field>> fieldSets)
        {
            for (Set<Field> fieldSet : fieldSets) {
                if (!fieldsEquals(cursor, fieldSet)) {
                    return false;
                }
            }
            return true;
        }

        private static boolean fieldsEquals(RecordCursor cursor, Set<Field> fields)
        {
            if (fields.size() < 2) {
                return true; // Nothing to compare
            }
            Iterator<Field> fieldIterator = fields.iterator();
            Field firstField = fieldIterator.next();
            while (fieldIterator.hasNext()) {
                if (!fieldEquals(cursor, firstField, fieldIterator.next())) {
                    return false;
                }
            }
            return true;
        }

        private static boolean fieldEquals(RecordCursor cursor, Field field1, Field field2)
        {
            checkArgument(cursor.getType(field1.getField()).equals(cursor.getType(field2.getField())), "Should only be comparing fields of the same type");

            if (cursor.isNull(field1.getField()) || cursor.isNull(field2.getField())) {
                return false;
            }

            Class<?> javaType = cursor.getType(field1.getField()).getJavaType();
            try {
                if (javaType == long.class) {
                    return (boolean) field1.getEqualsMethodHandle().invokeExact(cursor.getLong(field1.getField()), cursor.getLong(field2.getField()));
                }
                else if (javaType == double.class) {
                    return (boolean) field1.getEqualsMethodHandle().invokeExact(cursor.getDouble(field1.getField()), cursor.getDouble(field2.getField()));
                }
                else if (javaType == boolean.class) {
                    return (boolean) field1.getEqualsMethodHandle().invokeExact(cursor.getBoolean(field1.getField()), cursor.getBoolean(field2.getField()));
                }
                else if (javaType == Slice.class) {
                    return (boolean) field1.getEqualsMethodHandle().invokeExact(cursor.getSlice(field1.getField()), cursor.getSlice(field2.getField()));
                }
                else {
                    return (boolean) field1.getEqualsMethodHandle().invoke(cursor.getObject(field1.getField()), cursor.getObject(field2.getField()));
                }
            }
            catch (Throwable throwable) {
                throw Throwables.propagate(throwable);
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
        public Object getObject(int field)
        {
            // equals is super expensive for the currently supported types: Block
            throw new UnsupportedOperationException();
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
