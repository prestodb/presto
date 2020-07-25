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
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class PageProjectionWithOutputs
{
    private final PageProjection pageProjection;
    private final int[] outputChannels;

    public PageProjectionWithOutputs(PageProjection pageProjection, int[] outputChannels)
    {
        this.pageProjection = requireNonNull(pageProjection, "pageProjection is null");
        this.outputChannels = Arrays.copyOf(requireNonNull(outputChannels, "outputChannels is null"), outputChannels.length);
    }

    public PageProjection getPageProjection()
    {
        return pageProjection;
    }

    public int getOutputCount()
    {
        return outputChannels.length;
    }

    int[] getOutputChannels()
    {
        return outputChannels;
    }

    Work<List<Block>> project(SqlFunctionProperties sqlFunctionProperties, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        return pageProjection.project(sqlFunctionProperties, yieldSignal, page, selectedPositions);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("pageProjection", pageProjection)
                .add("outputChannels", outputChannels)
                .toString();
    }
}
