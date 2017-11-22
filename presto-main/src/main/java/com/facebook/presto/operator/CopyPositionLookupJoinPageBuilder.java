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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class CopyPositionLookupJoinPageBuilder
        implements LookupJoinPageBuilder
{
    private final PageBuilder pageBuilder;
    private final int buildOutputChannelCount;

    public CopyPositionLookupJoinPageBuilder(List<Type> allTypes, int buildOutputChannelCount)
    {
        this.pageBuilder = new PageBuilder(requireNonNull(allTypes, "allTypes is null"));
        this.buildOutputChannelCount = buildOutputChannelCount;
    }

    @Override
    public boolean isFull()
    {
        return pageBuilder.isFull();
    }

    @Override
    public boolean isEmpty()
    {
        return pageBuilder.isEmpty();
    }

    @Override
    public void reset()
    {
        pageBuilder.reset();
    }

    @Override
    public void appendRow(JoinProbe probe, LookupSource lookupSource, long joinPosition)
    {
        pageBuilder.declarePosition();
        // write probe columns
        probe.appendTo(pageBuilder);
        // write build columns
        lookupSource.appendTo(joinPosition, pageBuilder, probe.getOutputChannelCount());
    }

    @Override
    public void appendNullForBuild(JoinProbe probe)
    {
        // write probe columns
        pageBuilder.declarePosition();
        probe.appendTo(pageBuilder);

        // write nulls into build columns
        int outputIndex = probe.getOutputChannelCount();
        for (int buildChannel = 0; buildChannel < buildOutputChannelCount; buildChannel++) {
            pageBuilder.getBlockBuilder(outputIndex).appendNull();
            outputIndex++;
        }
    }

    @Override
    public Page build(JoinProbe probe)
    {
        return pageBuilder.build();
    }
}
