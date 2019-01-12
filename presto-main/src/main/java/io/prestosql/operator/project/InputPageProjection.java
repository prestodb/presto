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
package io.prestosql.operator.project;

import io.prestosql.operator.CompletedWork;
import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.operator.Work;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

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
            result = block.copyPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
        }
        else {
            result = block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
        return new CompletedWork<>(result);
    }
}
