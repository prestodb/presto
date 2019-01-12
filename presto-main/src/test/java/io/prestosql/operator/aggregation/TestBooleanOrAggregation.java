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
import io.prestosql.spi.type.StandardTypes;

import java.util.List;

import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class TestBooleanOrAggregation
        extends AbstractTestAggregationFunction
{
    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        BlockBuilder blockBuilder = BOOLEAN.createBlockBuilder(null, length);
        for (int i = start; i < start + length; i++) {
            // false, true, false, true...
            BOOLEAN.writeBoolean(blockBuilder, i % 2 != 0);
        }
        return new Block[] {blockBuilder.build()};
    }

    @Override
    public Boolean getExpectedValue(int start, int length)
    {
        if (length == 0) {
            return null;
        }
        return length > 1 ? TRUE : FALSE;
    }

    @Override
    protected String getFunctionName()
    {
        return "bool_or";
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.BOOLEAN);
    }
}
