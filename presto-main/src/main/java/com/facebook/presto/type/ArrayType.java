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
package com.facebook.presto.type;

import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class ArrayType
        extends AbstractVariableWidthType
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final Set<Type> PASSTHROUGH_TYPES = ImmutableSet.<Type>of(BIGINT, DOUBLE, BOOLEAN, VARCHAR);

    private final Type elementType;

    public ArrayType(Type elementType)
    {
        super(format("array<%s>", elementType.getName()), Slice.class);
        this.elementType = checkNotNull(elementType, "elementType is null");
    }

    public Type getElementType()
    {
        return elementType;
    }

    /**
     * Takes a list of stack types and converts them to the stack representation of an array
     */
    public static Slice toStackRepresentation(List<?> values)
    {
        try {
            return Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(values));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        String jsonString = block.getSlice(position, 0, block.getLength(position)).toStringUtf8();
        return fromStackRepresentation(session, jsonString);
    }

    private List<Object> fromStackRepresentation(ConnectorSession session, String stackRepresentation)
    {
        Class<?> elementJsonType;
        if (getElementType().getJavaType() == boolean.class) {
            elementJsonType = Boolean.class;
        }
        else if (getElementType().getJavaType() == long.class) {
            elementJsonType = Long.class;
        }
        else if (getElementType().getJavaType() == double.class) {
            elementJsonType = Double.class;
        }
        else if (getElementType().getJavaType() == Slice.class) {
            elementJsonType = String.class;
        }
        else if (getElementType().getJavaType() == void.class) {
            elementJsonType = Void.class;
        }
        else {
            throw new UnsupportedOperationException(format("Unsupported stack type: %s", getElementType().getJavaType()));
        }

        JavaType listType = OBJECT_MAPPER.getTypeFactory().constructParametricType(List.class, elementJsonType);
        List<Object> jsonElements;
        try {
            jsonElements = OBJECT_MAPPER.readValue(stackRepresentation, listType);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        // Fast path for BIGINT, DOUBLE, BOOLEAN, and VARCHAR
        if (PASSTHROUGH_TYPES.contains(getElementType())) {
            return jsonElements;
        }

        // Convert stack types to objects
        List<Object> objectElements = new ArrayList<>();
        for (Object value : jsonElements) {
            objectElements.add(getElementObjectValue(session, value));
        }

        return objectElements;
    }

    private Object getElementObjectValue(ConnectorSession session, Object value)
    {
        if (value == null) {
            return null;
        }

        if (getElementType() instanceof ArrayType) {
            // Recurse into elements, if this is an array of arrays
            ArrayType arrayType = checkType(getElementType(), ArrayType.class, "elementType");
            return arrayType.fromStackRepresentation(session, checkType(value, String.class, "value"));
        }

        BlockBuilder blockBuilder = getElementType().createBlockBuilder(new BlockBuilderStatus());
        if (getElementType().getJavaType() == boolean.class) {
            getElementType().writeBoolean(blockBuilder, checkType(value, Boolean.class, "value"));
        }
        else if (getElementType().getJavaType() == long.class) {
            getElementType().writeLong(blockBuilder, checkType(value, Long.class, "value"));
        }
        else if (getElementType().getJavaType() == double.class) {
            getElementType().writeDouble(blockBuilder, checkType(value, Double.class, "value"));
        }
        else if (getElementType().getJavaType() == Slice.class) {
            getElementType().writeSlice(blockBuilder, checkType(value, Slice.class, "value"));
        }
        return getElementType().getObjectValue(session, blockBuilder.build(), 0);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(blockBuilderStatus);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(getElementType());
    }
}
