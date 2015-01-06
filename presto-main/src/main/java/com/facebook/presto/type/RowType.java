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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.type.TypeJsonUtils.createBlock;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractVariableWidthType
{
    private final List<RowField> fields;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final CollectionType COLLECTION_TYPE = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, Object.class);

    public RowType(List<Type> fieldTypes, Optional<List<String>> fieldNames)
    {
        super(new TypeSignature(
                        "row",
                        Lists.transform(fieldTypes, Type::getTypeSignature),
                        fieldNames.orElse(ImmutableList.of()).stream()
                                .map(Object.class::cast)
                                .collect(toImmutableList())),
                Slice.class);

        ImmutableList.Builder<RowField> builder = ImmutableList.builder();
        for (int i = 0; i < fieldTypes.size(); i++) {
            final int index = i;
            builder.add(new RowField(fieldTypes.get(i), fieldNames.map((names) -> names.get(index))));
        }
        fields = builder.build();
    }

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        List<String> fields = new ArrayList<>();
        for (int i = 0; i < this.fields.size(); i++) {
            RowField field = this.fields.get(i);
            if (field.getName().isPresent()) {
                fields.add(field.getName() + " " + field.getType().getDisplayName());
            }
            else {
                fields.add(field.getType().getDisplayName());
            }
        }
        return "ROW(" + Joiner.on(", ").join(fields) + ")";
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
        return fields.stream()
                .map(RowField::getType)
                .collect(toImmutableList());
    }

    public List<RowField> getFields()
    {
        return fields;
    }

    public static class RowField
    {
        private final Type type;
        private final Optional<String> name;

        public RowField(Type type, Optional<String> name)
        {
            this.type = checkNotNull(type, "type is null");
            this.name = checkNotNull(name, "name is null");
        }

        public Type getType()
        {
            return type;
        }

        public Optional<String> getName()
        {
            return name;
        }
    }

    @Override
    public boolean isComparable()
    {
        return Iterables.all(fields, new Predicate<RowField>()
        {
            @Override
            public boolean apply(RowField field)
            {
                return field.getType().isComparable();
            }
        });
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        List<Object> leftRow = extractElements(leftBlock, leftPosition);
        List<Object> rightRow = extractElements(rightBlock, rightPosition);

        int nFields = leftRow.size();
        for (int i = 0; i < nFields; i++) {
            Object leftElement = leftRow.get(i);
            Object rightElement = rightRow.get(i);
            checkElementNotNull(leftElement);
            checkElementNotNull(rightElement);
            Type fieldType = fields.get(i).getType();
            if (!fieldType.equalTo(createBlock(fieldType, leftElement), 0, createBlock(fieldType, rightElement), 0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hash(Block block, int position)
    {
        List<Object> elements = extractElements(block, position);
        int result = 1;
        int nFields = elements.size();
        for (int i = 0; i < nFields; i++) {
            Object element = elements.get(i);
            checkElementNotNull(element);
            Type elementType = fields.get(i).getType();
            result = 31 * result + elementType.hash(createBlock(elementType, element), 0);
        }
        return result;
    }

    private static void checkElementNotNull(Object element)
    {
        if (element == null) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for rows with null elements");
        }
    }

    private List<Object> extractElements(Block block, int position)
    {
        Slice value = getSlice(block, position);
        try {
            return OBJECT_MAPPER.readValue(value.getBytes(), COLLECTION_TYPE);
        }
        catch (IOException e) {
            throw new PrestoException(INTERNAL_ERROR, format("Bad native value, '%s'", value.toStringUtf8()), e);
        }
    }
}
