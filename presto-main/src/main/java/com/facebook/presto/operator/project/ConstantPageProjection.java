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

import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.TypeUtils.writeNativeValue;

public class ConstantPageProjection
        implements PageProjection
{
    private static final InputChannels INPUT_PARAMETERS = new InputChannels(ImmutableList.of());

    private final Type type;
    private final Block value;

    public ConstantPageProjection(Object value, Type type)
    {
        this.type = type;
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1);
        writeNativeValue(type, blockBuilder, value);
        this.value = blockBuilder.build();
    }

    @Override
    public Type getType()
    {
        return type;
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
    public PageProjectionOutput project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return new ConstantPageProjectionOutput(value, selectedPositions.size());
    }

    private class ConstantPageProjectionOutput
            implements PageProjectionOutput
    {
        private final Block value;
        private final int size;

        public ConstantPageProjectionOutput(Block value, int size)
        {
            this.value = value;
            this.size = size;
        }

        @Override
        public Optional<Block> compute()
        {
            return Optional.of(new RunLengthEncodedBlock(value, size));
        }
    }
}
