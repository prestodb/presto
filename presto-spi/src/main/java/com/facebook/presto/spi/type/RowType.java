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
package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RowBlockBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static java.util.Objects.requireNonNull;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private final List<RowField> fields;
    private final List<Type> fieldTypes;

    public RowType(List<Type> fieldTypes, Optional<List<String>> fieldNames)
    {
        super(toTypeSignature(fieldTypes, fieldNames), Block.class);

        List<RowField> fields = new ArrayList<>();
        for (int i = 0; i < fieldTypes.size(); i++) {
            int index = i;
            fields.add(new RowField(fieldTypes.get(i), fieldNames.map((names) -> names.get(index))));
        }
        this.fields = fields;
        this.fieldTypes = fieldTypes;
    }

    private static TypeSignature toTypeSignature(List<Type> fieldTypes, Optional<List<String>> fieldNames)
    {
        int size = fieldTypes.size();
        if (size == 0) {
            throw new IllegalArgumentException("Row type must have at least 1 field");
        }

        List<TypeSignature> elementTypeSignatures = new ArrayList<>();
        List<String> literalParameters = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            elementTypeSignatures.add(fieldTypes.get(i).getTypeSignature());
            if (fieldNames.isPresent()) {
                literalParameters.add(fieldNames.get().get(i));
            }
        }
        return new TypeSignature(ROW, elementTypeSignatures, literalParameters);
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
        for (RowField field : fields) {
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
            blockBuilder.writeObject(block.getObject(position, Block.class));
            blockBuilder.closeEntry();
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
        blockBuilder.writeObject(value).closeEntry();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return fieldTypes;
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
