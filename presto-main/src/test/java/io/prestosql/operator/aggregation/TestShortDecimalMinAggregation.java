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
package io.prestosql.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.SqlDecimal;

import java.util.List;

public class TestShortDecimalMinAggregation
        extends AbstractTestAggregationFunction
{
    public static final DecimalType SHORT_DECIMAL = DecimalType.createDecimalType(10, 5);

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = SHORT_DECIMAL.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            SHORT_DECIMAL.writeLong(blockBuilder, i);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public SqlDecimal getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return SqlDecimal.of(start, 10, 5);
    }

    @Override
    protected String getFunctionName()
    {
        return "min";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(SHORT_DECIMAL.getTypeSignature().toString());
    }
}
