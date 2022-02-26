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
package com.facebook.presto.orc.metadata.statistics;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.VariableWidthBlockBuilder;
import io.airlift.slice.Slice;

/**
 * This test uses blocks as values.
 */
public class TestStringStatisticsBuilderBlockValue
        extends TestStringStatisticsBuilder
{
    public TestStringStatisticsBuilderBlockValue()
    {
        super(TestStringStatisticsBuilderBlockValue::addValueStatic);
    }

    @Override
    protected void addValue(StringStatisticsBuilder builder, Slice value)
    {
        addValueStatic(builder, value);
    }

    private static void addValueStatic(StringStatisticsBuilder builder, Slice value)
    {
        VariableWidthBlockBuilder blockBuilder = new VariableWidthBlockBuilder(null, 1, 1);
        if (value != null) {
            blockBuilder.writeBytes(value, 0, value.length()).closeEntry();
        }
        else {
            blockBuilder.appendNull();
        }
        Block block = blockBuilder.build();
        builder.addValue(block, 0);
    }
}
