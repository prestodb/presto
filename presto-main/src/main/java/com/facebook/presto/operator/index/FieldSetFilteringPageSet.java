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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageSet;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.lang.invoke.MethodHandle;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class FieldSetFilteringPageSet
        implements PageSet
{
    private final PageSet delegate;
    private final List<Set<FieldSetFilteringPageSet.Field>> fieldSets;
    private final List<Page> pages;

    public FieldSetFilteringPageSet(FunctionRegistry functionRegistry, PageSet delegate, List<Set<Integer>> fieldSets)
    {
        requireNonNull(functionRegistry, "functionRegistry is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
        List<Type> columnTypes = delegate.getColumnTypes();
        ImmutableList.Builder<Set<FieldSetFilteringPageSet.Field>> fieldSetsBuilder = ImmutableList.builder();
        for (Set<Integer> fieldSet : requireNonNull(fieldSets, "fieldSets is null")) {
            ImmutableSet.Builder<FieldSetFilteringPageSet.Field> fieldSetBuilder = ImmutableSet.builder();
            for (int field : fieldSet) {
                fieldSetBuilder.add(new FieldSetFilteringPageSet.Field(
                        field,
                        functionRegistry.getScalarFunctionImplementation(internalOperator(OperatorType.EQUAL, BooleanType.BOOLEAN, ImmutableList.of(columnTypes.get(field), columnTypes.get(field)))).getMethodHandle()));
            }
            fieldSetsBuilder.add(fieldSetBuilder.build());
        }
        this.fieldSets = fieldSetsBuilder.build();
        this.pages = delegate.getPages().stream().map(page -> getPage(page)).collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return delegate.getColumnTypes();
    }

    @Override
    public List<Page> getPages()
    {
        return pages;
    }

    private Page getPage(Page page)
    {
        IntList positionMask = new IntArrayList();
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (fieldSetsEqual(delegate.getColumnTypes(), page, i, fieldSets)) {
                positionMask.add(i);
            }
        }
        return page.getPositions(positionMask.toIntArray());
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

    private static boolean fieldSetsEqual(List<Type> types, Page page, int row, List<Set<FieldSetFilteringPageSet.Field>> fieldSets)
    {
        for (Set<FieldSetFilteringPageSet.Field> fieldSet : fieldSets) {
            if (!fieldsEquals(types, page, row, fieldSet)) {
                return false;
            }
        }
        return true;
    }

    private static boolean fieldsEquals(List<Type> types, Page page, int row, Set<FieldSetFilteringPageSet.Field> fields)
    {
        if (fields.size() < 2) {
            return true; // Nothing to compare
        }
        Iterator<FieldSetFilteringPageSet.Field> fieldIterator = fields.iterator();
        FieldSetFilteringPageSet.Field firstField = fieldIterator.next();
        while (fieldIterator.hasNext()) {
            if (!fieldEquals(types, page, row, firstField, fieldIterator.next())) {
                return false;
            }
        }
        return true;
    }

    private static boolean fieldEquals(List<Type> types, Page page, int position, FieldSetFilteringPageSet.Field field1, FieldSetFilteringPageSet.Field field2)
    {
        Type type1 = types.get(field1.getField());
        Type type2 = types.get(field2.getField());
        checkArgument(type1.equals(type2), "Should only be comparing fields of the same type");
        Block block1 = page.getBlock(field1.getField());
        Block block2 = page.getBlock(field2.getField());

        if (block1.isNull(position) || block2.isNull(position)) {
            return false;
        }
        try {
            Class<?> javaType = type1.getJavaType();
            if (javaType == long.class) {
                return (boolean) field1.getEqualsMethodHandle().invokeExact(type1.getLong(block1, position), type2.getLong(block2, position));
            }
            else if (javaType == double.class) {
                return (boolean) field1.getEqualsMethodHandle().invokeExact(type1.getDouble(block1, position), type2.getDouble(block2, position));
            }
            else if (javaType == boolean.class) {
                return (boolean) field1.getEqualsMethodHandle().invokeExact(type1.getBoolean(block1, position), type2.getBoolean(block2, position));
            }
            else if (javaType == Slice.class) {
                return (boolean) field1.getEqualsMethodHandle().invokeExact(type1.getSlice(block1, position), type2.getSlice(block2, position));
            }
            return (boolean) field1.getEqualsMethodHandle().invoke(type1.getObject(block1, position), type2.getObject(block2, position));
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }
}
