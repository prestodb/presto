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
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;

import java.math.BigDecimal;
import java.util.List;

import static io.prestosql.spi.type.Decimals.MAX_SHORT_PRECISION;

public class TestShortDecimalAverageAggregation
        extends AbstractTestDecimalAverageAggregation
{
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(MAX_SHORT_PRECISION - 1);

    @Override
    protected DecimalType getDecimalType()
    {
        return SHORT_DECIMAL_TYPE;
    }

    @Override
    protected void writeDecimalToBlock(BigDecimal decimal, BlockBuilder blockBuilder)
    {
        SHORT_DECIMAL_TYPE.writeLong(blockBuilder, decimal.unscaledValue().longValue());
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of("decimal(16,2)");
    }
}
