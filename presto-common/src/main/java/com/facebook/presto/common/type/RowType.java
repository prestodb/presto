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
import com.facebook.presto.common.type.semantic.SemanticType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.StandardTypes.ROW;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private final List<Field> fields;
    private final List<SemanticType> fieldSemanticTypes;
    private final List<Type> fieldTypes;

    private RowType(TypeSignature typeSignature, List<Field> fields)
    {
        super(typeSignature, Block.class);

        this.fields = fields;
        this.fieldSemanticTypes = fields.stream()
                .map(Field::getSemanticType)
                .collect(toList());
        this.fieldTypes = unmodifiableList(fieldSemanticTypes.stream().map(SemanticType::getType).collect(toList()));
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(makeSignature(fields), fields);
    }

    public static RowType anonymous(List<? extends Type> types)
    {
        List<Field> fields = types.stream()
                .map(type -> type instanceof SemanticType ? new Field(Optional.empty(), (SemanticType) type) : new Field(Optional.empty(), SemanticType.from(type)))
                .collect(toList());

        return new RowType(makeSignature(fields), fields);
    }

    public static RowType withDefaultFieldNames(List<Type> types)
    {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            SemanticType semanticType;
            if (types.get(i) instanceof SemanticType) {
                semanticType = (SemanticType) types.get(i);
            }
            else {
                semanticType = SemanticType.from(types.get(i));
            }
            fields.add(new Field(Optional.of("field" + i), semanticType));
        }
        return new RowType(makeSignature(fields), fields);
    }

    // Only RowParametricType.createType should call this method
    public static RowType createWithTypeSignature(TypeSignature typeSignature, List<Field> fields)
    {
        return new RowType(typeSignature, fields);
    }

    public static Field field(String name, SemanticType semanticType)
    {
        return new Field(Optional.of(name), semanticType);
    }

    public static Field field(String name, Type type)
    {
        if (type instanceof SemanticType) {
            return field(name, (SemanticType) type);
        }
        return new Field(Optional.of(name), SemanticType.from(type));
    }

    public static Field field(Type type)
    {
        if (type instanceof SemanticType) {
            return new Field(Optional.empty(), (SemanticType) type);
        }
        return new Field(Optional.empty(), SemanticType.from(type));
    }

    private static TypeSignature makeSignature(List<Field> fields)
    {
        int size = fields.size();
        if (size == 0) {
            throw new IllegalArgumentException("Row type must have at least 1 field");
        }

        List<TypeSignatureParameter> parameters = fields.stream()
                .map(field -> TypeSignatureParameter.of(new NamedTypeSignature(field.getName().map(name -> new RowFieldName(name, false)), field.getSemanticType().getTypeSignature())))
                .collect(toList());

        return new TypeSignature(ROW, parameters);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new RowBlockBuilder(getPhysicalTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return new RowBlockBuilder(getPhysicalTypeParameters(), blockBuilderStatus, expectedEntries);
    }

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        StringBuilder result = new StringBuilder();
        result.append(ROW).append('(');
        for (Field field : fields) {
            String typeDisplayName = field.getSemanticType().getDisplayName();
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

        return unmodifiableList(values);
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

    @Override
    public List<SemanticType> getSemanticTypeParameters()
    {
        return fieldSemanticTypes;
    }

    public List<Field> getFields()
    {
        return fields;
    }

    public static class Field
    {
        private final SemanticType semanticType;
        private final Optional<String> name;

        public Field(Optional<String> name, SemanticType semanticType)
        {
            this.semanticType = requireNonNull(semanticType, "type is null");
            this.name = requireNonNull(name, "name is null");
        }

        public SemanticType getSemanticType()
        {
            return semanticType;
        }

        public Type getType()
        {
            return semanticType.getType();
        }

        public Optional<String> getName()
        {
            return name;
        }
    }

    @Override
    public boolean isComparable()
    {
        return fields.stream().allMatch(field -> field.getSemanticType().isComparable());
    }

    @Override
    public boolean isOrderable()
    {
        return fields.stream().allMatch(field -> field.getSemanticType().isOrderable());
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

    private List<Type> getPhysicalTypeParameters()
    {
        return fieldTypes;
    }

    private static void checkElementNotNull(boolean isNull)
    {
        if (isNull) {
            throw new NotSupportedException("ROW comparison not supported for fields with null elements");
        }
    }
}
