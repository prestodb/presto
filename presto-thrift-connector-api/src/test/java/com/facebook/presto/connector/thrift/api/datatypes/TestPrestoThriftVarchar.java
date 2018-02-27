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
package com.facebook.presto.connector.thrift.api.datatypes;

import com.facebook.presto.connector.thrift.api.PrestoThriftBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import org.testng.annotations.Test;

import static com.facebook.presto.connector.thrift.api.datatypes.PrestoThriftVarchar.fromBlock;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

public class TestPrestoThriftVarchar
{
    @Test
    public void testReadSingleValue()
    {
        PrestoThriftBlock column = fromBlock(varcharBlocks("a string"), VARCHAR);
        assertNotNull(column.getVarcharData());
        assertEquals(column.getVarcharData().getSingleValue(), "a string");
    }

    @Test
    public void testReadSingleNullValue()
    {
        PrestoThriftBlock column = fromBlock(nullValueBlocks(1), VARCHAR);
        assertNotNull(column.getVarcharData());
        assertNull(column.getVarcharData().getSingleValue());
    }

    @Test
    public void testReadSingleValueFromMultiValueBlock()
    {
        PrestoThriftBlock column = fromBlock(varcharBlocks("string 1", "string 2"), VARCHAR);
        assertNotNull(column.getVarcharData());
        assertThrows(() -> column.getVarcharData().getSingleValue());

        PrestoThriftBlock column2 = fromBlock(nullValueBlocks(2), VARCHAR);
        assertNotNull(column2.getVarcharData());
        assertThrows(() -> column2.getVarcharData().getSingleValue());
    }

    private static Block nullValueBlocks(int length)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), length);
        for (int i = 0; i < length; i++) {
            blockBuilder.appendNull();
        }
        return blockBuilder.build();
    }

    private static Block varcharBlocks(String... values)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), values.length);
        for (String value : values) {
            if (value == null) {
                blockBuilder.appendNull();
            }
            else {
                VARCHAR.writeString(blockBuilder, value);
            }
        }
        return blockBuilder.build();
    }
}
