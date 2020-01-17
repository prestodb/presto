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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.util.List;

import static com.facebook.presto.spi.type.Decimals.MAX_SHORT_PRECISION;
import static com.facebook.presto.spi.type.Decimals.encodeScaledValue;

public class TestLongDecimalAverageAggregation
        extends AbstractTestDecimalAverageAggregation
{
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1);

    @Override
    protected DecimalType getDecimalType()
    {
        return LONG_DECIMAL_TYPE;
    }

    @Override
    protected void writeDecimalToBlock(BigDecimal decimal, BlockBuilder blockBuilder)
    {
        LONG_DECIMAL_TYPE.writeSlice(blockBuilder, encodeScaledValue(decimal));
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(DecimalType.createDecimalType(MAX_SHORT_PRECISION + 1, 2).getTypeSignature().toString());
    }
}
