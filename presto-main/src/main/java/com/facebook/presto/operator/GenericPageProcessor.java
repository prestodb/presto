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
import com.google.common.collect.ImmutableList;

import java.util.List;

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
        for (int position = start; position < end; position++) {
            if (filterFunction.filter(position, page.getBlocks())) {
                pageBuilder.declarePosition();
                for (int i = 0; i < projections.size(); i++) {
                    // todo: if the projection function increases the size of the data significantly, this could cause the servers to OOM
                    projections.get(i).project(position, page.getBlocks(), pageBuilder.getBlockBuilder(i));
                }
            }
        }

        return end;
    }
}
