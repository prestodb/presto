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
package com.facebook.presto.operator;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Arrays.copyOf;
import static java.util.stream.Collectors.toList;

public class GenericPageProcessor
        implements PageProcessor
{
    private final FilterFunction filterFunction;
    private final List<ProjectionFunction> projections;

    public GenericPageProcessor(FilterFunction filterFunction, Iterable<? extends ProjectionFunction> projections)
    {
        this.filterFunction = filterFunction;
        this.projections = ImmutableList.copyOf(projections);
    }

    @Override
    public int process(ConnectorSession session, Page page, int start, int end, PageBuilder pageBuilder)
    {
        int position = start;
        for (; position < end && !pageBuilder.isFull(); position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                pageBuilder.declarePosition();
                for (int i = 0; i < projections.size(); i++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(i).project(position, page.getBlocks(), pageBuilder.getBlockBuilder(i));
                }
            }
        }
        return position;
    }

    @Override
    public Page processColumnar(ConnectorSession session, Page page, List<? extends Type> types)
    {
        int[] selectedPositions = filterPage(page);
        if (selectedPositions.length == 0) {
            return null;
        }

        PageBuilder pageBuilder = new PageBuilder(types);
        Block[] inputBlocks = page.getBlocks();

        for (int projectionIndex = 0; projectionIndex < projections.size(); projectionIndex++) {
            ProjectionFunction projection = projections.get(projectionIndex);
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(projectionIndex);
            for (int position : selectedPositions) {
                projection.project(position, inputBlocks, blockBuilder);
            }
        }
        pageBuilder.declarePositions(selectedPositions.length);
        return pageBuilder.build();
    }

    private int[] filterPage(Page page)
    {
        int[] selected = new int[page.getPositionCount()];
        int index = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                selected[index] = position;
                index++;
            }
        }
        return copyOf(selected, index);
    }

    private PageBuilder createPageBuilder()
    {
        List<Type> types = projections.stream().map(ProjectionFunction::getType).collect(toList());
        return new PageBuilder(types);
    }
}
