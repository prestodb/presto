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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.List;

import static io.prestosql.spi.type.BigintType.BIGINT;

public class NoChannelGroupByHash
        implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NoChannelGroupByHash.class).instanceSize();

    private int groupCount;

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE;
    }

    @Override
    public long getHashCollisions()
    {
        return 0;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return 0;
    }

    @Override
    public List<Type> getTypes()
    {
        return ImmutableList.of();
    }

    @Override
    public int getGroupCount()
    {
        return groupCount;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        throw new UnsupportedOperationException("NoChannelGroupByHash does not support appendValuesTo");
    }

    @Override
    public Work<?> addPage(Page page)
    {
        updateGroupCount(page);
        // create a dump work whose result is never used.
        return new CompletedWork<>(0);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        updateGroupCount(page);
        return new CompletedWork<>(new GroupByIdBlock(page.getPositionCount() > 0 ? 1 : 0, RunLengthEncodedBlock.create(BIGINT, 0L, page.getPositionCount())));
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        throw new UnsupportedOperationException("NoChannelGroupByHash does not support getHashCollisions");
    }

    @Override
    public long getRawHash(int groupyId)
    {
        throw new UnsupportedOperationException("NoChannelGroupByHash does not support getHashCollisions");
    }

    @Override
    public int getCapacity()
    {
        return 2;
    }

    private void updateGroupCount(Page page)
    {
        if (page.getPositionCount() > 0 && groupCount == 0) {
            groupCount = 1;
        }
    }
}
