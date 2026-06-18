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
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class GeneratedPageProjection
        implements PageProjection
{
    private final List<RowExpression> projections;
    private final boolean isDeterministic;
    private final InputChannels inputChannels;
    private final MethodHandle pageProjectionWorkFactory;

    private List<BlockBuilder> blockBuilders;

    public GeneratedPageProjection(List<RowExpression> projections, boolean isDeterministic, InputChannels inputChannels, MethodHandle pageProjectionWorkFactory)
    {
        this.projections = ImmutableList.copyOf(requireNonNull(projections, "projections is null"));
        this.isDeterministic = isDeterministic;
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
        this.pageProjectionWorkFactory = requireNonNull(pageProjectionWorkFactory, "pageProjectionWorkFactory is null");
        this.blockBuilders = projections.stream().map(RowExpression::getType).map(type -> type.createBlockBuilder(null, 1)).collect(toImmutableList());
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
    public Work<List<Block>> project(SqlFunctionProperties properties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        blockBuilders = blockBuilders.stream().map(blockBuilder -> blockBuilder.newBlockBuilderLike(null, selectedPositions.size())).collect(toImmutableList());
        try {
            return (Work<List<Block>>) pageProjectionWorkFactory.invoke(blockBuilders, properties, page, selectedPositions);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("projections", projections)
                .toString();
    }
}
