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
package com.facebook.presto.operator.index;

import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.LookupSource;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PagesIndex;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.type.Type;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class PagesIndexBuilder
{
    private final List<Type> types;
    private final int expectedPositions;
    private final List<Integer> indexChannels;
    private final long maxMemoryUsage;

    private final OperatorContext bogusOperatorContext;
    private PagesIndex currentPagesIndex;

    private List<Page> pages = new ArrayList<>();
    private long memoryUsage;

    public PagesIndexBuilder(List<Type> types, int expectedPositions, List<Integer> indexChannels, OperatorContext operatorContext, DataSize maxMemoryUsage)
    {
        this.types = types;
        this.expectedPositions = expectedPositions;
        this.indexChannels = indexChannels;
        this.maxMemoryUsage = maxMemoryUsage.toBytes();

        // create a bogus operator context with unlimited memory for the pages index
        DriverContext driverContext = operatorContext.getDriverContext();
        bogusOperatorContext = new TaskContext(driverContext.getTaskId(), driverContext.getExecutor(), driverContext.getSession(), new DataSize(Long.MAX_VALUE, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext()
                .addOperatorContext(0, "operator");

        this.currentPagesIndex = new PagesIndex(types, expectedPositions, bogusOperatorContext);
    }

    public boolean isEmpty()
    {
        return currentPagesIndex.getPositionCount() == 0 && pages.isEmpty();
    }

    public boolean isMemoryExceeded()
    {
        return memoryUsage > maxMemoryUsage;
    }

    public boolean tryAddPage(Page page)
    {
        memoryUsage += page.getDataSize().toBytes();
        if (isMemoryExceeded()) {
            return false;
        }
        currentPagesIndex.addPage(page);
        return true;
    }

    public LookupSource createLookupSource()
    {
        checkState(!isMemoryExceeded(), "Max memory exceeded");
        for (Page page : pages) {
            currentPagesIndex.addPage(page);
        }
        pages.clear();
        return currentPagesIndex.createLookupSource(indexChannels);
    }

    public void reset()
    {
        memoryUsage = 0;
        pages.clear();
        currentPagesIndex = new PagesIndex(types, expectedPositions, bogusOperatorContext);
    }
}
