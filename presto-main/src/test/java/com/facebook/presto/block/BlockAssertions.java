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
package com.facebook.presto.block;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public final class BlockAssertions
{
    public static final ConnectorSession SESSION = new ConnectorSession("user", UTC_KEY, ENGLISH, System.currentTimeMillis(), null);

    private BlockAssertions()
    {
    }

    public static Object getOnlyValue(Type type, Block block)
    {
        assertEquals(block.getPositionCount(), 1, "Block positions");
        return type.getObjectValue(SESSION, block, 0);
    }

    public static List<Object> toValues(Type type, Iterable<Block> blocks)
    {
        List<Object> values = new ArrayList<>();
        for (Block block : blocks) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                values.add(type.getObjectValue(SESSION, block, position));
            }
        }
        return Collections.unmodifiableList(values);
    }

    public static List<Object> toValues(Type type, Block block)
    {
        List<Object> values = new ArrayList<>();
        for (int position = 0; position < block.getPositionCount(); position++) {
            values.add(type.getObjectValue(SESSION, block, position));
        }
        return Collections.unmodifiableList(values);
    }

    public static void assertBlockEquals(Type type, Block actual, Block expected)
    {
        for (int position = 0; position < actual.getPositionCount(); position++) {
            assertEquals(type.getObjectValue(SESSION, actual, position), type.getObjectValue(SESSION, expected, position));
        }
    }

    public static Block createStringsBlock(String... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createStringsBlock(Arrays.asList(values));
    }

    public static Block createStringsBlock(Iterable<String> values)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (String value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                VARCHAR.writeString(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createStringSequenceBlock(int start, int end)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int i = start; i < end; i++) {
            VARCHAR.writeString(builder, String.valueOf(i));
        }

        return builder.build();
    }

    public static Block createBooleansBlock(Boolean... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createBooleansBlock(Arrays.asList(values));
    }

    public static Block createBooleansBlock(Boolean value, int count)
    {
        return createBooleansBlock(Collections.nCopies(count, value));
    }

    public static Block createBooleansBlock(Iterable<Boolean> values)
    {
        BlockBuilder builder = BOOLEAN.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Boolean value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BOOLEAN.writeBoolean(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createEmptyLongsBlock()
    {
        return BIGINT.createFixedSizeBlockBuilder(0).build();
    }

    // This method makes it easy to create blocks without having to add an L to every value
    public static Block createLongsBlock(int... values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (int value : values) {
            BIGINT.writeLong(builder, (long) value);
        }

        return builder.build();
    }

    public static Block createLongsBlock(Long... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createLongsBlock(Arrays.asList(values));
    }

    public static Block createLongsBlock(Iterable<Long> values)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Long value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                BIGINT.writeLong(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createLongSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BIGINT.writeLong(builder, i);
        }

        return builder.build();
    }

    public static Block createBooleanSequenceBlock(int start, int end)
    {
        BlockBuilder builder = BOOLEAN.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            BOOLEAN.writeBoolean(builder, i % 2 == 0);
        }

        return builder.build();
    }

    public static Block createDoublesBlock(Double... values)
    {
        checkNotNull(values, "varargs 'values' is null");

        return createDoublesBlock(Arrays.asList(values));
    }

    public static Block createDoublesBlock(Iterable<Double> values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(new BlockBuilderStatus(), 100);

        for (Double value : values) {
            if (value == null) {
                builder.appendNull();
            }
            else {
                DOUBLE.writeDouble(builder, value);
            }
        }

        return builder.build();
    }

    public static Block createDoubleSequenceBlock(int start, int end)
    {
        BlockBuilder builder = DOUBLE.createFixedSizeBlockBuilder(end - start);

        for (int i = start; i < end; i++) {
            DOUBLE.writeDouble(builder, (double) i);
        }

        return builder.build();
    }
}
