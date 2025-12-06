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
package com.facebook.presto.common.type;

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.RowBlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeUtils.containsDistinctType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private final List<Field> fields;
    private final List<Type> fieldTypes;
    private final Optional<TypeSignature> typeSignature;

    private RowType(List<Field> fields)
    {
        this(fields, fields.stream()
                .map(Field::getType)
                .collect(toList()));
    }

    private RowType(List<Field> fields, List<Type> fieldTypes)
    {
        this(fields, fieldTypes, containsDistinctType(fieldTypes) ? Optional.empty() : Optional.of(makeSignature(fields)));
    }

    @JsonCreator
    public RowType(List<Field> fields,
                   List<Type> fieldTypes,
                   @JsonProperty("typeSignature") Optional<TypeSignature> typeSignature)
    {
        super(Block.class);
        this.fields = fields;
        this.fieldTypes = fieldTypes;
        this.typeSignature = typeSignature;
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(fields);
    }

    public static RowType anonymous(List<Type> types)
    {
        List<Field> fields = types.stream()
                .map(type -> new Field(Optional.empty(), type))
                .collect(toList());

        return new RowType(fields);
    }

    public static RowType withDefaultFieldNames(List<Type> types)
    {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            fields.add(new Field(Optional.of("field" + i), types.get(i)));
        }
        return new RowType(fields);
    }

    public static Field field(String name, Type type)
    {
        return new Field(Optional.of(name), type);
    }

    public static Field field(Type type)
    {
        return new Field(Optional.empty(), type);
    }

    public static Field field(String name, Type type, boolean delimited)
    {
        return new Field(Optional.of(name), type, delimited);
    }

    private static TypeSignature makeSignature(List<Field> fields)
    {
        int size = fields.size();
        if (size == 0) {
            throw new IllegalArgumentException("Row type must have at least 1 field");
        }

        List<TypeSignatureParameter> parameters = fields.stream()
                .map(field -> TypeSignatureParameter.of(new NamedTypeSignature(field.getName().map(name -> new RowFieldName(name, field.isDelimited())), field.getType().getTypeSignature())))
                .collect(toList());

        return new TypeSignature(ROW, parameters);
    }

    @Override
    public TypeSignature getTypeSignature()
    {
        return typeSignature.orElseGet(() -> makeSignature(fields));
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new RowBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new RowBlockBuilder(getTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @JsonProperty
    public List<Field> getFields()
    {
        return fields;
    }

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        StringBuilder result = new StringBuilder();
        result.append(ROW).append('(');
        for (Field field : fields) {
            String typeDisplayName = field.getType().getDisplayName();
            if (field.getName().isPresent()) {
                result.append("\"").append(field.getName().get()).append("\"").append(' ').append(typeDisplayName);
            }
            else {
                result.append(typeDisplayName);
            }
            result.append(", ");
        }
        result.setLength(result.length() - 2);
        result.append(')');
        return result.toString();
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Block arrayBlock = getObject(block, position);
        List<Object> values = new ArrayList<>(arrayBlock.getPositionCount());

        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            values.add(fields.get(i).getType().getObjectValue(properties, arrayBlock, i));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writePositionTo(position, blockBuilder);
        }
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getBlock(position);
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        return block.getBlockUnchecked(internalPosition);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        blockBuilder.appendStructure((Block) value);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return fieldTypes;
    }

    public static class Field
    {
        private final Type type;
        private final Optional<String> name;
        private final boolean delimited;

        public Field(@JsonProperty("name") Optional<String> name, @JsonProperty("type") Type type) {
            this(name, type, false);
        }

        public Field(Optional<String> name, Type type, boolean delimited)
        {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
            this.delimited = delimited;
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public Optional<String> getName()
        {
            return name;
        }

        public boolean isDelimited()
        {
            return delimited;
        }
    }

    @Override
    public boolean isComparable()
    {
        return fields.stream().allMatch(field -> field.getType().isComparable());
    }

    @Override
    public boolean isOrderable()
    {
        return fields.stream().allMatch(field -> field.getType().isOrderable());
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftRow = leftBlock.getBlock(leftPosition);
        Block rightRow = rightBlock.getBlock(rightPosition);

        for (int i = 0; i < leftRow.getPositionCount(); i++) {
            checkElementNotNull(leftRow.isNull(i));
            checkElementNotNull(rightRow.isNull(i));
            Type fieldType = fields.get(i).getType();
            if (!fieldType.equalTo(leftRow, i, rightRow, i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftRow = leftBlock.getBlock(leftPosition);
        Block rightRow = rightBlock.getBlock(rightPosition);

        for (int i = 0; i < leftRow.getPositionCount(); i++) {
            checkElementNotNull(leftRow.isNull(i));
            checkElementNotNull(rightRow.isNull(i));
            Type fieldType = fields.get(i).getType();
            if (!fieldType.isOrderable()) {
                throw new UnsupportedOperationException(fieldType.getTypeSignature() + " type is not orderable");
            }
            int compareResult = fieldType.compareTo(leftRow, i, rightRow, i);
            if (compareResult != 0) {
                return compareResult;
            }
        }

        return 0;
    }

    @Override
    public long hash(Block block, int position)
    {
        Block arrayBlock = block.getBlock(position);
        long result = 1;
        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            Type elementType = fields.get(i).getType();
            result = 31 * result + TypeUtils.hashPosition(elementType, arrayBlock, i);
        }
        return result;
    }

    private static void checkElementNotNull(boolean isNull)
    {
        if (isNull) {
            throw new NotSupportedException("ROW comparison not supported for fields with null elements");
        }
    }
}
