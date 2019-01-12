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
package io.prestosql.spi.type;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.block.RowBlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.spi.type.StandardTypes.ROW;
import static java.util.Objects.requireNonNull;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private final List<Field> fields;
    private final List<Type> fieldTypes;

    private RowType(TypeSignature typeSignature, List<Field> fields)
    {
        super(typeSignature, Block.class);

        this.fields = fields;
        this.fieldTypes = fields.stream()
                .map(Field::getType)
                .collect(Collectors.toList());
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(makeSignature(fields), fields);
    }

    public static RowType anonymous(List<Type> types)
    {
        List<Field> fields = types.stream()
                .map(type -> new Field(Optional.empty(), type))
                .collect(Collectors.toList());

        return new RowType(makeSignature(fields), fields);
    }

    // Only RowParametricType.createType should call this method
    public static RowType createWithTypeSignature(TypeSignature typeSignature, List<Field> fields)
    {
        return new RowType(typeSignature, fields);
    }

    public static Field field(String name, Type type)
    {
        return new Field(Optional.of(name), type);
    }

    public static Field field(Type type)
    {
        return new Field(Optional.empty(), type);
    }

    private static TypeSignature makeSignature(List<Field> fields)
    {
        int size = fields.size();
        if (size == 0) {
            throw new IllegalArgumentException("Row type must have at least 1 field");
        }

        List<TypeSignatureParameter> parameters = fields.stream()
                .map(field -> TypeSignatureParameter.of(new NamedTypeSignature(field.getName().map(name -> new RowFieldName(name, false)), field.getType().getTypeSignature())))
                .collect(Collectors.toList());

        return new TypeSignature(ROW, parameters);
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

    @Override
    public String getDisplayName()
    {
        // Convert to standard sql name
        StringBuilder result = new StringBuilder();
        result.append(ROW).append('(');
        for (Field field : fields) {
            String typeDisplayName = field.getType().getDisplayName();
            if (field.getName().isPresent()) {
                result.append(field.getName().get()).append(' ').append(typeDisplayName);
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
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Block arrayBlock = getObject(block, position);
        List<Object> values = new ArrayList<>(arrayBlock.getPositionCount());

        for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
            values.add(fields.get(i).getType().getObjectValue(session, arrayBlock, i));
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
        return block.getObject(position, Block.class);
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

    public List<Field> getFields()
    {
        return fields;
    }

    public static class Field
    {
        private final Type type;
        private final Optional<String> name;

        public Field(Optional<String> name, Type type)
        {
            this.type = requireNonNull(type, "type is null");
            this.name = requireNonNull(name, "name is null");
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
        Block leftRow = leftBlock.getObject(leftPosition, Block.class);
        Block rightRow = rightBlock.getObject(rightPosition, Block.class);

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
        Block leftRow = leftBlock.getObject(leftPosition, Block.class);
        Block rightRow = rightBlock.getObject(rightPosition, Block.class);

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
        Block arrayBlock = block.getObject(position, Block.class);
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
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "ROW comparison not supported for fields with null elements");
        }
    }
}
