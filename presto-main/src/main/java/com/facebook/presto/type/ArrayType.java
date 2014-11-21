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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.facebook.presto.type.TypeJsonUtils.createBlock;
import static com.facebook.presto.type.TypeJsonUtils.getObjectList;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.google.common.base.Preconditions.checkNotNull;

public class ArrayType
        extends AbstractVariableWidthType
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    private static final ObjectMapper RAW_SLICE_OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new RawSliceSerializer()));

    private final Type elementType;

    public ArrayType(Type elementType)
    {
        super(parameterizedTypeName("array", elementType.getTypeSignature()), Slice.class);
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

    /**
     * Takes a list of json encoded slices and converts them to the stack representation of an array
     */
    public static Slice rawSlicesToStackRepresentation(List<Slice> values)
    {
        try {
            return Slices.utf8Slice(RAW_SLICE_OBJECT_MAPPER.writeValueAsString(values));
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return elementType.isOrderable();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return compareTo(leftBlock, leftPosition, rightBlock, rightPosition) == 0;
    }

    @Override
    public int hash(Block block, int position)
    {
        Slice value = getSlice(block, position);
        List<Object> array = getObjectList(value);
        List<Integer> hashArray = new ArrayList<Integer>();
        for (Object element : array) {
            checkElementNotNull(element);
            hashArray.add(elementType.hash(createBlock(elementType, element), 0));
        }
        return Objects.hash(hashArray);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Slice leftSlice = getSlice(leftBlock, leftPosition);
        Slice rightSlice = getSlice(rightBlock, rightPosition);
        List<Object> leftArray = getObjectList(leftSlice);
        List<Object> rightArray = getObjectList(rightSlice);

        int len = Math.min(leftArray.size(), rightArray.size());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.get(index));
            checkElementNotNull(rightArray.get(index));
            int comparison = elementType.compareTo(createBlock(elementType, leftArray.get(index)), 0,
                    createBlock(elementType, rightArray.get(index)), 0);
            if (comparison != 0) {
                return comparison;
            }
            index++;
        }

        if (index == len) {
            return leftArray.size() - rightArray.size();
        }

        return 0;
    }

    private static void checkElementNotNull(Object element)
    {
        if (element == null) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "ARRAY comparison not supported for arrays with null elements");
        }
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getLength(position));
        return stackRepresentationToObject(session, slice, this);
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
