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
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.OperatorMethodHandle;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static com.facebook.presto.common.type.StandardTypes.ROW;
import static com.facebook.presto.common.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * As defined in ISO/IEC FCD 9075-2 (SQL 2011), section 4.8
 */
public class RowType
        extends AbstractType
{
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);

    private static final MethodHandle EQUAL;
    private static final MethodHandle CHAIN_EQUAL;
    private static final MethodHandle HASH_CODE;
    private static final MethodHandle CHAIN_HASH_CODE;
    private static final MethodHandle DISTINCT_FROM;
    private static final MethodHandle CHAIN_DISTINCT_FROM_START;
    private static final MethodHandle CHAIN_DISTINCT_FROM;
    private static final MethodHandle INDETERMINATE;
    private static final MethodHandle CHAIN_INDETERMINATE;
    private static final MethodHandle COMPARISON;
    private static final MethodHandle CHAIN_COMPARISON;
    private static final int MEGAMORPHIC_FIELD_COUNT = 64;

    // this field is used in double checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration typeOperatorDeclaration;

    static {
        try {
            Lookup lookup = lookup();
            EQUAL = lookup.findStatic(RowType.class, "megamorphicEqualOperator", methodType(Boolean.class, List.class, Block.class, Block.class));
            CHAIN_EQUAL = lookup.findStatic(RowType.class, "chainEqual", methodType(Boolean.class, Boolean.class, int.class, MethodHandle.class, Block.class, Block.class));
            HASH_CODE = lookup.findStatic(RowType.class, "megamorphicHashCodeOperator", methodType(long.class, List.class, Block.class));
            CHAIN_HASH_CODE = lookup.findStatic(RowType.class, "chainHashCode", methodType(long.class, long.class, int.class, MethodHandle.class, Block.class));
            DISTINCT_FROM = lookup.findStatic(RowType.class, "megamorphicDistinctFromOperator", methodType(boolean.class, List.class, Block.class, Block.class));
            CHAIN_DISTINCT_FROM_START = lookup.findStatic(RowType.class, "chainDistinctFromStart", methodType(boolean.class, MethodHandle.class, Block.class, Block.class));
            CHAIN_DISTINCT_FROM = lookup.findStatic(RowType.class, "chainDistinctFrom", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, Block.class, Block.class));
            INDETERMINATE = lookup.findStatic(RowType.class, "megamorphicIndeterminateOperator", methodType(boolean.class, List.class, Block.class));
            CHAIN_INDETERMINATE = lookup.findStatic(RowType.class, "chainIndeterminate", methodType(boolean.class, boolean.class, int.class, MethodHandle.class, Block.class));
            COMPARISON = lookup.findStatic(RowType.class, "megamorphicComparisonOperator", methodType(long.class, List.class, Block.class, Block.class));
            CHAIN_COMPARISON = lookup.findStatic(RowType.class, "chainComparison", methodType(long.class, long.class, int.class, MethodHandle.class, Block.class, Block.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private final List<Field> fields;
    private final List<Type> fieldTypes;

    private RowType(TypeSignature typeSignature, List<Field> fields)
    {
        super(typeSignature, Block.class);

        this.fields = fields;
        this.fieldTypes = fields.stream()
                .map(Field::getType)
                .collect(toList());
    }

    public static RowType from(List<Field> fields)
    {
        return new RowType(makeSignature(fields), fields);
    }

    public static RowType anonymous(List<Type> types)
    {
        List<Field> fields = types.stream()
                .map(type -> new Field(Optional.empty(), type))
                .collect(toList());

        return new RowType(makeSignature(fields), fields);
    }

    public static RowType withDefaultFieldNames(List<Type> types)
    {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < types.size(); i++) {
            fields.add(new Field(Optional.of("field" + i), types.get(i)));
        }
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
                .collect(toList());

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
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return typeOperatorDeclaration;
    }

    private synchronized void generateTypeOperators(TypeOperators typeOperators)
    {
        if (typeOperatorDeclaration != null) {
            return;
        }
        typeOperatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, fields))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, fields))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, fields))
                .addDistinctFromOperators(getDistinctFromOperatorInvokers(typeOperators, fields))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, fields))
                .addComparisonOperators(getComparisonOperatorInvokers(typeOperators, fields))
                .build();
    }

    private static List<OperatorMethodHandle> getEqualOperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> equalOperators = Collections.unmodifiableList(fields.stream()
                    .map(field -> typeOperators.getEqualOperator(field.getType(), simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION)))
                    .collect(toList()));
            return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(equalOperators)));
        }

        // (Block, Block):Boolean
        MethodHandle equal = dropArguments(constant(Boolean.class, TRUE), 0, Block.class, Block.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (Block, Block, int, MethodHandle, Block, Block):Boolean
            equal = collectArguments(
                    CHAIN_EQUAL,
                    0,
                    equal);

            // field equal
            MethodHandle fieldEqualOperator = typeOperators.getEqualOperator(field.getType(), simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));

            // (Block, Block, Block, Block):Boolean
            equal = insertArguments(equal, 2, fieldId, fieldEqualOperator);

            // (Block, Block):Boolean
            equal = permuteArguments(equal, methodType(Boolean.class, Block.class, Block.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, equal));
    }

    private static Boolean megamorphicEqualOperator(List<MethodHandle> equalOperators, Block leftRow, Block rightRow)
            throws Throwable
    {
        boolean unknown = false;
        for (int fieldIndex = 0; fieldIndex < equalOperators.size(); fieldIndex++) {
            if (leftRow.isNull(fieldIndex) || rightRow.isNull(fieldIndex)) {
                unknown = true;
                continue;
            }
            MethodHandle equalOperator = equalOperators.get(fieldIndex);
            Boolean result = (Boolean) equalOperator.invokeExact(leftRow, fieldIndex, rightRow, fieldIndex);
            if (result == null) {
                unknown = true;
            }
            else if (!result) {
                return false;
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static Boolean chainEqual(Boolean previousFieldsEqual, int currentFieldIndex, MethodHandle currentFieldEqual, Block rightRow, Block leftRow)
            throws Throwable
    {
        if (previousFieldsEqual == FALSE) {
            return FALSE;
        }

        if (leftRow.isNull(currentFieldIndex) || rightRow.isNull(currentFieldIndex)) {
            return null;
        }

        Boolean result = (Boolean) currentFieldEqual.invokeExact(rightRow, currentFieldIndex, leftRow, currentFieldIndex);
        if (result == TRUE) {
            // this field is equal, so result is either true or unknown depending on the previous fields
            return previousFieldsEqual;
        }
        // this field is either not equal or unknown, which is the result
        return result;
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, List<Field> fields)
    {
        return getHashCodeOperatorMethodHandles(fields, type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(List<Field> fields, Function<Type, MethodHandle> getHashOperator)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> hashCodeOperators = Collections.unmodifiableList(fields.stream()
                    .map(field -> getHashOperator.apply(field.getType()))
                    .collect(toList()));
            return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(hashCodeOperators)));
        }

        // (Block):long
        MethodHandle hashCode = dropArguments(constant(long.class, 1), 0, Block.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (Block, int, MethodHandle, Block):long
            hashCode = collectArguments(
                    CHAIN_HASH_CODE,
                    0,
                    hashCode);

            // field hash code
            MethodHandle fieldHashCodeOperator = getHashOperator.apply(field.getType());

            // (Block, Block):long
            hashCode = insertArguments(hashCode, 1, fieldId, fieldHashCodeOperator);

            // (Block):long
            hashCode = permuteArguments(hashCode, methodType(long.class, Block.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, hashCode));
    }

    private static long megamorphicHashCodeOperator(List<MethodHandle> hashCodeOperators, Block rowBlock)
            throws Throwable
    {
        long result = 1;
        for (int fieldIndex = 0; fieldIndex < hashCodeOperators.size(); fieldIndex++) {
            long fieldHashCode = NULL_HASH_CODE;
            if (!rowBlock.isNull(fieldIndex)) {
                MethodHandle hashCodeOperator = hashCodeOperators.get(fieldIndex);
                fieldHashCode = (long) hashCodeOperator.invokeExact(rowBlock, fieldIndex);
            }
            result = 31 * result + fieldHashCode;
        }
        return result;
    }

    private static long chainHashCode(long previousFieldHashCode, int currentFieldIndex, MethodHandle currentFieldHashCodeOperator, Block row)
            throws Throwable
    {
        long fieldHashCode = NULL_HASH_CODE;
        if (!row.isNull(currentFieldIndex)) {
            fieldHashCode = (long) currentFieldHashCodeOperator.invokeExact(row, currentFieldIndex);
        }
        return 31 * previousFieldHashCode + fieldHashCode;
    }

    private static List<OperatorMethodHandle> getDistinctFromOperatorInvokers(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> distinctFromOperators = Collections.unmodifiableList(fields.stream()
                    .map(field -> typeOperators.getDistinctFromOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                    .collect(toList()));
            return singletonList(new OperatorMethodHandle(DISTINCT_FROM_CONVENTION, DISTINCT_FROM.bindTo(distinctFromOperators)));
        }

        // (Block, Block):boolean
        MethodHandle distinctFrom = dropArguments(constant(boolean.class, false), 0, Block.class, Block.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (Block, Block, int, MethodHandle, Block, Block):boolean
            distinctFrom = collectArguments(
                    CHAIN_DISTINCT_FROM,
                    0,
                    distinctFrom);

            // field distinctFrom
            MethodHandle fieldDistinctFromOperator = typeOperators.getDistinctFromOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

            // (Block, Block, Block, Block):boolean
            distinctFrom = insertArguments(distinctFrom, 2, fieldId, fieldDistinctFromOperator);

            // (Block, Block):boolean
            distinctFrom = permuteArguments(distinctFrom, methodType(boolean.class, Block.class, Block.class), 0, 1, 0, 1);
        }
        distinctFrom = CHAIN_DISTINCT_FROM_START.bindTo(distinctFrom);

        return singletonList(new OperatorMethodHandle(DISTINCT_FROM_CONVENTION, distinctFrom));
    }

    private static boolean megamorphicDistinctFromOperator(List<MethodHandle> distinctFromOperators, Block leftRow, Block rightRow)
            throws Throwable
    {
        boolean leftIsNull = leftRow == null;
        boolean rightIsNull = rightRow == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }

        for (int fieldIndex = 0; fieldIndex < distinctFromOperators.size(); fieldIndex++) {
            MethodHandle equalOperator = distinctFromOperators.get(fieldIndex);
            boolean result = (boolean) equalOperator.invoke(leftRow, fieldIndex, rightRow, fieldIndex);
            if (result) {
                return true;
            }
        }

        return false;
    }

    private static boolean chainDistinctFromStart(MethodHandle chain, Block rightRow, Block leftRow)
            throws Throwable
    {
        boolean leftIsNull = leftRow == null;
        boolean rightIsNull = rightRow == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }
        return (boolean) chain.invokeExact(rightRow, leftRow);
    }

    private static boolean chainDistinctFrom(boolean previousFieldsDistinctFrom, int currentFieldIndex, MethodHandle currentFieldDistinctFrom, Block rightRow, Block leftRow)
            throws Throwable
    {
        if (previousFieldsDistinctFrom) {
            return true;
        }
        return (boolean) currentFieldDistinctFrom.invokeExact(rightRow, currentFieldIndex, leftRow, currentFieldIndex);
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, List<Field> fields)
    {
        boolean comparable = fields.stream().allMatch(field -> field.getType().isComparable());
        if (!comparable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> indeterminateOperators = Collections.unmodifiableList(fields.stream()
                    .map(field -> typeOperators.getIndeterminateOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION)))
                    .collect(toList()));
            return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(indeterminateOperators)));
        }

        // (Block):long
        MethodHandle indeterminate = dropArguments(constant(boolean.class, false), 0, Block.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (Block, int, MethodHandle, Block):boolean
            indeterminate = collectArguments(
                    CHAIN_INDETERMINATE,
                    0,
                    indeterminate);

            // field indeterminate
            MethodHandle fieldIndeterminateOperator = typeOperators.getIndeterminateOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));

            // (Block, Block):boolean
            indeterminate = insertArguments(indeterminate, 1, fieldId, fieldIndeterminateOperator);

            // (Block):boolean
            indeterminate = permuteArguments(indeterminate, methodType(boolean.class, Block.class), 0, 0);
        }
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, indeterminate));
    }

    private static boolean megamorphicIndeterminateOperator(List<MethodHandle> indeterminateOperators, Block rowBlock)
            throws Throwable
    {
        if (rowBlock == null) {
            return true;
        }
        for (int fieldIndex = 0; fieldIndex < indeterminateOperators.size(); fieldIndex++) {
            if (!rowBlock.isNull(fieldIndex)) {
                MethodHandle indeterminateOperator = indeterminateOperators.get(fieldIndex);
                if ((boolean) indeterminateOperator.invokeExact(rowBlock, fieldIndex)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean chainIndeterminate(boolean previousFieldIndeterminate, int currentFieldIndex, MethodHandle currentFieldIndeterminateOperator, Block row)
            throws Throwable
    {
        if (row == null || previousFieldIndeterminate) {
            return true;
        }
        return (boolean) currentFieldIndeterminateOperator.invokeExact(row, currentFieldIndex);
    }

    private static List<OperatorMethodHandle> getComparisonOperatorInvokers(TypeOperators typeOperators, List<Field> fields)
    {
        boolean orderable = fields.stream().allMatch(field -> field.getType().isOrderable());
        if (!orderable) {
            return emptyList();
        }

        // for large rows, use a generic loop with a megamorphic call site
        if (fields.size() > MEGAMORPHIC_FIELD_COUNT) {
            List<MethodHandle> comparisonOperators = Collections.unmodifiableList(fields.stream()
                    .map(field -> typeOperators.getComparisonOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)))
                    .collect(toList()));
            return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, COMPARISON.bindTo(comparisonOperators)));
        }

        // (Block, Block):Boolean
        MethodHandle comparison = dropArguments(constant(long.class, 0), 0, Block.class, Block.class);
        for (int fieldId = 0; fieldId < fields.size(); fieldId++) {
            Field field = fields.get(fieldId);
            // (Block, Block, int, MethodHandle, Block, Block):Boolean
            comparison = collectArguments(
                    CHAIN_COMPARISON,
                    0,
                    comparison);

            // field comparison
            MethodHandle fieldComparisonOperator = typeOperators.getComparisonOperator(field.getType(), simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));

            // (Block, Block, Block, Block):Boolean
            comparison = insertArguments(comparison, 2, fieldId, fieldComparisonOperator);

            // (Block, Block):Boolean
            comparison = permuteArguments(comparison, methodType(long.class, Block.class, Block.class), 0, 1, 0, 1);
        }
        return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, comparison));
    }

    private static long megamorphicComparisonOperator(List<MethodHandle> comparisonOperators, Block leftRow, Block rightRow)
            throws Throwable
    {
        for (int fieldIndex = 0; fieldIndex < comparisonOperators.size(); fieldIndex++) {
            checkElementNotNull(leftRow.isNull(fieldIndex));
            checkElementNotNull(rightRow.isNull(fieldIndex));

            MethodHandle comparisonOperator = comparisonOperators.get(fieldIndex);
            long result = (long) comparisonOperator.invoke(leftRow, fieldIndex, rightRow, fieldIndex);
            if (result == 0) {
                return result;
            }
        }
        return 0;
    }

    private static long chainComparison(long previousFieldsResult, int fieldIndex, MethodHandle nextFieldComparison, Block rightRow, Block leftRow)
            throws Throwable
    {
        if (previousFieldsResult != 0) {
            return previousFieldsResult;
        }

        checkElementNotNull(leftRow.isNull(fieldIndex));
        checkElementNotNull(rightRow.isNull(fieldIndex));

        return (long) nextFieldComparison.invokeExact(rightRow, fieldIndex, leftRow, fieldIndex);
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
