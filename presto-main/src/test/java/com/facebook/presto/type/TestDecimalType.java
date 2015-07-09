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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDecimal;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestDecimalType
        extends AbstractTestType
{
    private static final DecimalType DECIMAL_TYPE = DecimalType.createDecimalType(4, 2);

    public TestDecimalType()
    {
        super(DECIMAL_TYPE, SqlDecimal.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 15);
        DECIMAL_TYPE.writeLong(blockBuilder, 1234);
        DECIMAL_TYPE.writeLong(blockBuilder, 1234);
        DECIMAL_TYPE.writeLong(blockBuilder, 1234);
        DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        DECIMAL_TYPE.writeLong(blockBuilder, 2321);
        DECIMAL_TYPE.writeLong(blockBuilder, 3321);
        DECIMAL_TYPE.writeLong(blockBuilder, 3321);
        DECIMAL_TYPE.writeLong(blockBuilder, 4321);
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return ((long) value) + 1;
    }
}
