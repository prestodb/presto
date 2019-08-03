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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createIntsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongDecimalsBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.block.BlockAssertions.createTypedLongsBlock;
import static com.facebook.presto.spi.block.ArrayBlock.fromElementBlock;
import static com.facebook.presto.spi.block.MapBlock.fromKeyValueBlock;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.spi.block.RowBlock.fromFieldBlocks;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.TestingEnvironment.TYPE_MANAGER;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class TestingBlockBuilders
{
    private static final int ENTRY_SIZE = 4;
    private static final int MAX_STRING_SIZE = 10;

    private final Random random = new Random(0);

    Block buildNullBlock(Type type, int positionCount)
    {
        return new RunLengthEncodedBlock(type.createBlockBuilder(null, 1).appendNull().build(), positionCount);
    }

    Block buildBigintBlock(int positionCount, boolean allowNulls)
    {
        return createLongsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : random.nextLong())
                        .collect(Collectors.toList()));
    }

    Block buildLongDecimalBlock(int positionCount, boolean allowNulls)
    {
        return createLongDecimalsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : Double.toString(random.nextDouble() * random.nextInt()))
                        .collect(Collectors.toList()));
    }

    Block buildIntegerBlock(int positionCount, boolean allowNulls)
    {
        return createIntsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : random.nextInt())
                        .collect(Collectors.toList()));
    }

    Block buildSmallintBlock(int positionCount, boolean allowNulls)
    {
        return createTypedLongsBlock(
                SMALLINT,
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : random.nextLong() % Short.MIN_VALUE)
                        .collect(Collectors.toList()));
    }

    Block buildBooleanBlock(int positionCount, boolean allowNulls)
    {
        return createBooleansBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : random.nextBoolean())
                        .collect(Collectors.toList()));
    }

    Block buildVarcharBlock(int positionCount, boolean allowNulls, int maxStringLength)
    {
        return createStringsBlock(
                IntStream.range(0, positionCount)
                        .mapToObj(i -> allowNulls && i % 7 == 0 ? null : getRandomString(maxStringLength))
                        .collect(Collectors.toList()));
    }

    DictionaryBlock buildDictionaryBlock(Block dictionary, int positionCount)
    {
        int[] ids = IntStream.range(0, positionCount).map(i -> random.nextInt(dictionary.getPositionCount() / 10)).toArray();
        return new DictionaryBlock(dictionary, ids);
    }

    RunLengthEncodedBlock buildRleBlock(Block block, int positionCount)
    {
        checkArgument(block.getPositionCount() > 0);
        return new RunLengthEncodedBlock(block.getRegion(block.getPositionCount() / 2, 1), positionCount);
    }

    Block buildDictRleBlock(Block block, int positionCount, Optional<String> wrappings)
    {
        if (!wrappings.isPresent()) {
            return null;
        }

        for (int i = wrappings.get().length() - 1; i >= 0; i--) {
            if (wrappings.get().charAt(i) == 'D') {
                block = buildDictionaryBlock(block, positionCount);
            }
            else if (wrappings.get().charAt(i) == 'R') {
                block = buildRleBlock(block, positionCount);
            }
            else {
                throw new IllegalArgumentException(format("wrappings %s is incorrect", wrappings));
            }
        }
        return block;
    }

    Block buildBlockWithType(Type type, int positionCount, boolean allowNulls, boolean isView, Optional<String> wrappings)
    {
        Block block = null;

        if (isView) {
            positionCount *= 2;
        }

        if (type == BIGINT || type instanceof DecimalType && ((DecimalType) type).isShort()) {
            block = buildBigintBlock(positionCount, allowNulls);
        }
        else if (type instanceof DecimalType && !((DecimalType) type).isShort()) {
            block = buildLongDecimalBlock(positionCount, allowNulls);
        }
        else if (type == INTEGER) {
            block = buildIntegerBlock(positionCount, allowNulls);
        }
        else if (type == SMALLINT) {
            block = buildSmallintBlock(positionCount, allowNulls);
        }
        else if (type == BOOLEAN) {
            block = buildBooleanBlock(positionCount, allowNulls);
        }
        else if (type == VARCHAR) {
            block = buildVarcharBlock(positionCount, allowNulls, MAX_STRING_SIZE);
        }
        else {
            // Nested types
            // Build isNull and offsets of size positionCount
            boolean[] isNull = new boolean[positionCount];
            int[] offsets = new int[positionCount + 1];
            for (int position = 0; position < positionCount; position++) {
                if (allowNulls && position % 7 == 0) {
                    isNull[position] = true;
                    offsets[position + 1] = offsets[position];
                }
                else {
                    offsets[position + 1] = offsets[position] + (type instanceof RowType ? 1 : random.nextInt(ENTRY_SIZE) + 1);
                }
            }

            // Build the nested block of size offsets[positionCount].
            if (type instanceof ArrayType) {
                Block valuesBlock = buildBlockWithType(
                        ((ArrayType) type).getElementType(),
                        offsets[positionCount],
                        allowNulls, isView,
                        wrappings);

                block = fromElementBlock(positionCount, Optional.of(isNull), offsets, valuesBlock);
            }
            else if (type instanceof MapType) {
                Block keyBlock = buildBlockWithType(
                        ((MapType) type).getKeyType(),
                        offsets[positionCount],
                        false, isView,
                        wrappings);
                Block valueBlock = buildBlockWithType(
                        ((MapType) type).getValueType(),
                        offsets[positionCount],
                        allowNulls, isView,
                        wrappings);

                Type keyType = ((MapType) type).getKeyType();
                MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
                MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
                MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
                MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));

                block = fromKeyValueBlock(
                        Optional.of(isNull),
                        offsets,
                        keyBlock,
                        valueBlock,
                        ((MapType) type),
                        keyBlockNativeEquals,
                        keyNativeHashCode,
                        keyBlockHashCode);
            }
            else if (type instanceof RowType) {
                List<Type> fieldTypes = ((RowType) type).getTypeParameters();
                Block[] fieldBlocks = new Block[fieldTypes.size()];

                for (int i = 0; i < fieldBlocks.length; i++) {
                    Block fieldBlock = buildBlockWithType(
                            fieldTypes.get(i),
                            positionCount,
                            allowNulls, isView,
                            wrappings);
                    fieldBlocks[i] = fieldBlock;
                }

                block = fromFieldBlocks(positionCount, Optional.of(isNull), fieldBlocks);
            }
            else {
                throw new UnsupportedOperationException(format("type %s is not supported.", type));
            }
        }

        if (isView) {
            positionCount /= 2;
            int offset = positionCount / 2;
            block = block.getRegion(offset, positionCount);
        }

        if (wrappings.isPresent()) {
            block = buildDictRleBlock(block, positionCount, wrappings);
        }

        return block;
    }

    static MapType createMapType(Type keyType, Type valueType)
    {
        MethodHandle keyNativeEquals = TYPE_MANAGER.resolveOperator(OperatorType.EQUAL, ImmutableList.of(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = TYPE_MANAGER.resolveOperator(OperatorType.HASH_CODE, ImmutableList.of(keyType));
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        return new MapType(
                keyType,
                valueType,
                keyBlockNativeEquals,
                keyBlockEquals,
                keyNativeHashCode,
                keyBlockHashCode);
    }

    private String getRandomString(int maxStringLength)
    {
        byte[] array = new byte[maxStringLength];
        random.nextBytes(array);
        return new String(array, Charset.forName("UTF-8"));
    }
}
