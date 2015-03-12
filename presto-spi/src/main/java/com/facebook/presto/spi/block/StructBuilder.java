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
package com.facebook.presto.spi.block;

import com.facebook.presto.spi.type.Type;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;

public class StructBuilder
{
    private final BlockBuilder builder;
    private final List<Type> parameterTypes;

    private int elementCount;

    public static StructBuilder arrayBuilder(Type elementType)
    {
        return new StructBuilder(asList(elementType));
    }

    public static StructBuilder mapBuilder(Type keyType, Type valueType)
    {
        return new StructBuilder(asList(keyType, valueType));
    }

    public static StructBuilder rowBuilder(List<Type> parameterTypes)
    {
        return new StructBuilder(parameterTypes);
    }

    public static StructBuilder builder(int expectedSize, List<Type> parameterTypes)
    {
        return new StructBuilder(expectedSize, parameterTypes);
    }

    private StructBuilder(List<Type> parameterTypes)
    {
        this(1024, parameterTypes);
    }

    private StructBuilder(int expectedSize, List<Type> parameterTypes)
    {
        this(new BlockBuilderStatus(expectedSize, expectedSize), parameterTypes);
    }

    private StructBuilder(BlockBuilderStatus builderStatus, List<Type> parameterTypes)
    {
        if (builderStatus == null) {
            throw new NullPointerException("builderStatus is null");
        }
        if (parameterTypes == null) {
            throw new NullPointerException("parameterTypes is null");
        }
        if (parameterTypes.isEmpty()) {
            throw new IllegalArgumentException("parameterTypes is empty");
        }
        ArrayList<Type> types = new ArrayList<>();
        for (Type type : parameterTypes) {
            if (type == null) {
                throw new NullPointerException("type element is null");
            }
            types.add(type);
        }

        this.builder = new VariableWidthBlockBuilder(builderStatus);
        this.parameterTypes = unmodifiableList(types);
    }

    public static Block readBlock(SliceInput input)
    {
        return new VariableWidthBlockEncoding().readBlock(input);
    }

    public Slice build()
    {
        if ((elementCount % parameterTypes.size()) != 0) {
            throw new IllegalStateException(String.format("Element count(%d) do not conform to the type size(%d)", elementCount, parameterTypes.size()));
        }

        BlockEncoding encoding = builder.getEncoding();
        Block block = builder.build();
        DynamicSliceOutput output = new DynamicSliceOutput(encoding.getEstimatedSize(block));
        encoding.writeBlock(output, block);

        return output.slice();
    }

    public StructBuilder addAll(Object... values)
    {
        for (Object value : values) {
            add(value);
        }

        return this;
    }

    public StructBuilder addAll(List<?> values)
    {
        for (Object value : values) {
            add(value);
        }

        return this;
    }

    public StructBuilder add(Block block, int position)
    {
        if (block.isNull(position)) {
            builder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), builder);
            builder.closeEntry();
        }
        return this;
    }

    public StructBuilder addAll(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            add(block, i);
        }

        return this;
    }

    public StructBuilder add(Object value)
    {
        Type type = parameterTypes.get(elementCount++ % parameterTypes.size());

        if (value == null) {
            builder.appendNull();
            return this;
        }

        if (type.getJavaType() == boolean.class) {
            type.writeBoolean(builder, (boolean) value);
        }
        else if (type.getJavaType() == long.class) {
            type.writeLong(builder, ((Number) value).longValue());
        }
        else if (type.getJavaType() == double.class) {
            type.writeDouble(builder, ((Number) value).doubleValue());
        }
        else if (type.getJavaType() == Slice.class) {
            type.writeSlice(builder, toSlice(type, value));
        }
        else {
            throw new UnsupportedOperationException("Unsupported type " + type);
        }

        return this;
    }

    private Slice toSlice(Type type, Object value)
    {
        Slice slice;
        if (value instanceof String) {
            slice = Slices.utf8Slice((String) value);
        }
        else if (value instanceof Slice) {
            slice = (Slice) value;
        }
        else if (value instanceof List) {
            StructBuilder structBuilder = new StructBuilder(type.getTypeParameters());
            ((List<?>) value).forEach(structBuilder::add);
            slice = structBuilder.build();
        }
        else if (value instanceof Map) {
            StructBuilder structBuilder = new StructBuilder(type.getTypeParameters());
            ((Map<?, ?>) value).forEach((k, v) -> structBuilder.add(k).add(v));

            slice = structBuilder.build();
        }
        else if (value instanceof byte[]) {
            slice = Slices.wrappedBuffer((byte[]) value);
        }
        else {
            throw new UnsupportedOperationException("Unsupported type " + value.getClass());
        }

        return slice;
    }
}
