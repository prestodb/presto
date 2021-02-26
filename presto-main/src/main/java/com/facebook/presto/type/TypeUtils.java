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

import com.facebook.presto.common.NotSupportedException;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.CharType.createCharType;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.common.type.VarcharType.createVarcharType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class TypeUtils
{
    public static final long NULL_HASH_CODE = 0;
    public static final TypeWithName BOOLEAN_TYPE = new TypeWithName(BOOLEAN);
    public static final TypeWithName BIGINT_TYPE = new TypeWithName(BIGINT);
    public static final TypeWithName COLOR_TYPE = new TypeWithName(COLOR);
    public static final TypeWithName DATE_TYPE = new TypeWithName(DATE);
    public static final TypeWithName DOUBLE_TYPE = new TypeWithName(DOUBLE);
    public static final TypeWithName HYPER_LOG_LOG_TYPE = new TypeWithName(HYPER_LOG_LOG);
    public static final TypeWithName INTEGER_TYPE = new TypeWithName(INTEGER);
    public static final TypeWithName INTERVAL_DAY_TIME_TYPE = new TypeWithName(INTERVAL_DAY_TIME);
    public static final TypeWithName INTERVAL_YEAR_MONTH_TYPE = new TypeWithName(INTERVAL_YEAR_MONTH);
    public static final TypeWithName JSON_TYPE = new TypeWithName(JSON);
    public static final TypeWithName REAL_TYPE = new TypeWithName(REAL);
    public static final TypeWithName SMALLINT_TYPE = new TypeWithName(SMALLINT);
    public static final TypeWithName TINYINT_TYPE = new TypeWithName(TINYINT);
    public static final TypeWithName TIME_TYPE = new TypeWithName(TIME);
    public static final TypeWithName TIME_WITH_TIME_ZONE_TYPE = new TypeWithName(TIME_WITH_TIME_ZONE);
    public static final TypeWithName TIMESTAMP_TYPE = new TypeWithName(TIMESTAMP);
    public static final TypeWithName TIMESTAMP_WITH_TIME_ZONE_TYPE = new TypeWithName(TIMESTAMP_WITH_TIME_ZONE);
    public static final TypeWithName UNKNOWN_TYPE = new TypeWithName(UNKNOWN);
    public static final TypeWithName VARCHAR_TYPE = new TypeWithName(VARCHAR);
    public static final TypeWithName VARBINARY_TYPE = new TypeWithName(VARBINARY);

    private TypeUtils()
    {
    }

    public static TypeWithName createDecimalSemanticType(int precision, int scale)
    {
        return new TypeWithName(createDecimalType(precision, scale));
    }

    public static TypeWithName createVarcharSemanticType(int length)
    {
        return new TypeWithName(createVarcharType(length));
    }

    public static TypeWithName createCharSemanticType(long length)
    {
        return new TypeWithName(createCharType(length));
    }

    public static int expectedValueSize(Type type, int defaultSize)
    {
        if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        return defaultSize;
    }

    public static long hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return type.hash(block, position);
    }

    public static long hashPosition(MethodHandle methodHandle, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        try {
            if (type.getJavaType() == boolean.class) {
                return (long) methodHandle.invoke(type.getBoolean(block, position));
            }
            else if (type.getJavaType() == long.class) {
                return (long) methodHandle.invoke(type.getLong(block, position));
            }
            else if (type.getJavaType() == double.class) {
                return (long) methodHandle.invoke(type.getDouble(block, position));
            }
            else if (type.getJavaType() == Slice.class) {
                return (long) methodHandle.invoke(type.getSlice(block, position));
            }
            else if (!type.getJavaType().isPrimitive()) {
                return (long) methodHandle.invoke(type.getObject(block, position));
            }
            else {
                throw new UnsupportedOperationException("Unsupported native container type: " + type.getJavaType() + " with type " + type.getTypeSignature());
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    public static boolean positionEqualsPosition(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }

        try {
            return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
        }
        catch (NotSupportedException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage(), e);
        }
    }

    public static Type resolveType(TypeSignature typeName, TypeManager typeManager)
    {
        return typeManager.getType(typeName);
    }

    public static boolean isIntegralType(TypeSignature typeName, FunctionAndTypeManager functionAndTypeManager)
    {
        switch (typeName.getBase()) {
            case StandardTypes.BIGINT:
            case StandardTypes.INTEGER:
            case StandardTypes.SMALLINT:
            case StandardTypes.TINYINT:
                return true;
            case StandardTypes.DECIMAL:
                DecimalType decimalType = (DecimalType) resolveType(typeName, functionAndTypeManager);
                return decimalType.getScale() == 0;
            default:
                return false;
        }
    }

    public static List<Type> resolveTypes(List<TypeSignature> typeNames, FunctionAndTypeManager functionAndTypeManager)
    {
        return typeNames.stream()
                .map((TypeSignature type) -> resolveType(type, functionAndTypeManager))
                .collect(toImmutableList());
    }

    public static long getHashPosition(List<? extends Type> hashTypes, Block[] hashBlocks, int position)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        Page page = new Page(hashBlocks);
        return hashGenerator.hashPosition(position, page);
    }

    public static Block getHashBlock(List<? extends Type> hashTypes, Block... hashBlocks)
    {
        checkArgument(hashTypes.size() == hashBlocks.length);
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            BIGINT.writeLong(builder, hashGenerator.hashPosition(i, page));
        }
        return builder.build();
    }

    public static Page getHashPage(Page page, List<? extends Type> types, List<Integer> hashChannels)
    {
        ImmutableList.Builder<Type> hashTypes = ImmutableList.builder();
        Block[] hashBlocks = new Block[hashChannels.size()];
        int hashBlockIndex = 0;

        for (int channel : hashChannels) {
            hashTypes.add(types.get(channel));
            hashBlocks[hashBlockIndex++] = page.getBlock(channel);
        }
        return page.appendColumn(getHashBlock(hashTypes.build(), hashBlocks));
    }

    public static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new PrestoException(NOT_SUPPORTED, errorMsg);
        }
    }
}
