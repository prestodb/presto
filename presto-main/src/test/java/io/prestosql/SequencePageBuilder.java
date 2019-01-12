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
package io.prestosql;

import io.prestosql.block.BlockAssertions;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public final class SequencePageBuilder
{
    private SequencePageBuilder()
    {
    }

    public static Page createSequencePage(List<? extends Type> types, int length)
    {
        return createSequencePage(types, length, new int[types.size()]);
    }

    public static Page createSequencePage(List<? extends Type> types, int length, int... initialValues)
    {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            int initialValue = initialValues[i];

            if (type.equals(BIGINT)) {
                blocks[i] = BlockAssertions.createLongSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(REAL)) {
                blocks[i] = BlockAssertions.createSequenceBlockOfReal(initialValue, initialValue + length);
            }
            else if (type.equals(DOUBLE)) {
                blocks[i] = BlockAssertions.createDoubleSequenceBlock(initialValue, initialValue + length);
            }
            else if (type instanceof VarcharType) {
                blocks[i] = BlockAssertions.createStringSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(BOOLEAN)) {
                blocks[i] = BlockAssertions.createBooleanSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(DATE)) {
                blocks[i] = BlockAssertions.createDateSequenceBlock(initialValue, initialValue + length);
            }
            else if (type.equals(TIMESTAMP)) {
                blocks[i] = BlockAssertions.createTimestampSequenceBlock(initialValue, initialValue + length);
            }
            else if (isShortDecimal(type)) {
                blocks[i] = BlockAssertions.createShortDecimalSequenceBlock(initialValue, initialValue + length, (DecimalType) type);
            }
            else if (isLongDecimal(type)) {
                blocks[i] = BlockAssertions.createLongDecimalSequenceBlock(initialValue, initialValue + length, (DecimalType) type);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }

    public static Page createSequencePageWithDictionaryBlocks(List<? extends Type> types, int length)
    {
        return createSequencePageWithDictionaryBlocks(types, length, new int[types.size()]);
    }

    public static Page createSequencePageWithDictionaryBlocks(List<? extends Type> types, int length, int... initialValues)
    {
        Block[] blocks = new Block[initialValues.length];
        for (int i = 0; i < blocks.length; i++) {
            Type type = types.get(i);
            int initialValue = initialValues[i];
            if (type.equals(VARCHAR)) {
                blocks[i] = BlockAssertions.createStringDictionaryBlock(initialValue, initialValue + length);
            }
            else if (type.equals(BIGINT)) {
                blocks[i] = BlockAssertions.createLongDictionaryBlock(initialValue, initialValue + length);
            }
            else {
                throw new IllegalStateException("Unsupported type " + type);
            }
        }

        return new Page(blocks);
    }
}
