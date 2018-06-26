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
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.relational.RowExpression;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class GeneratedPageProjection
        implements PageProjection
{
    private final RowExpression projection;
    private final boolean isDeterministic;
    private final InputChannels inputChannels;
    private final MethodHandle pageProjectionWorkFactory;

    private BlockBuilder blockBuilder;

    public GeneratedPageProjection(RowExpression projection, boolean isDeterministic, InputChannels inputChannels, MethodHandle pageProjectionWorkFactory)
    {
        this.projection = requireNonNull(projection, "projection is null");
        this.isDeterministic = isDeterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.pageProjectionWorkFactory = requireNonNull(pageProjectionWorkFactory, "pageProjectionWorkFactory is null");
        this.blockBuilder = projection.getType().createBlockBuilder(null, 1);
    }

    @Override
    public Type getType()
    {
        return projection.getType();
    }

    @Override
    public boolean isDeterministic()
    {
        return isDeterministic;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        blockBuilder = blockBuilder.newBlockBuilderLike(null);
        try {
            return (Work<Block>) pageProjectionWorkFactory.invoke(blockBuilder, session, yieldSignal, page, selectedPositions);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projection", projection)
                .toString();
    }
}
