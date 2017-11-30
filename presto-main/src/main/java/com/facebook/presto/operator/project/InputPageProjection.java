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

import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class InputPageProjection
        implements PageProjection
{
    private final Type type;
    private final InputChannels inputChannels;

    public InputPageProjection(int inputChannel, Type type)
    {
        this.type = type;
        this.inputChannels = new InputChannels(inputChannel);
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
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        Block block = requireNonNull(page, "page is null").getBlock(0);
        requireNonNull(selectedPositions, "selectedPositions is null");

        Block result;
        if (selectedPositions.isList()) {
            List<Integer> positionList = Ints.asList(selectedPositions.getPositions())
                    .subList(selectedPositions.getOffset(), selectedPositions.getOffset() + selectedPositions.size());
            result = block.copyPositions(positionList);
        }
        else {
            result = block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
        return new CompletedWork<>(result);
    }
}
