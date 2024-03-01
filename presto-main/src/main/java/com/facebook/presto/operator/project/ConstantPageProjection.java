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
package com.facebook.presto.operator.project;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.common.type.TypeUtils.writeNativeValue;

public class ConstantPageProjection
        implements PageProjection
{
    private static final InputChannels INPUT_PARAMETERS = new InputChannels(ImmutableList.of());

    private final Block value;

    public ConstantPageProjection(Object value, Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, blockBuilder, value);
        this.value = blockBuilder.build();
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return INPUT_PARAMETERS;
    }

    @Override
    public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new CompletedWork<>(ImmutableList.of(new RunLengthEncodedBlock(value, selectedPositions.size())));
    }
}
