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
package io.prestosql.operator.index;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.LookupSource;
import io.prestosql.operator.PagesIndex;
import io.prestosql.operator.index.UnloadedIndexKeyRecordSet.UnloadedIndexKeyRecordCursor;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class IndexSnapshotBuilder
{
    private final Session session;
    private final int expectedPositions;
    private final List<Type> outputTypes;
    private final List<Type> missingKeysTypes;
    private final List<Integer> keyOutputChannels;
    private final OptionalInt keyOutputHashChannel;
    private final List<Integer> missingKeysChannels;
    private final PagesIndex.Factory pagesIndexFactory;

    private final long maxMemoryInBytes;
    private PagesIndex outputPagesIndex;
    private PagesIndex missingKeysIndex;
    private LookupSource missingKeys;

    private final List<Page> pages = new ArrayList<>();
    private long memoryInBytes;

    private final PageBuilder missingKeysPageBuilder;

    public IndexSnapshotBuilder(List<Type> outputTypes,
            List<Integer> keyOutputChannels,
            OptionalInt keyOutputHashChannel,
            DriverContext driverContext,
            DataSize maxMemoryInBytes,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory)
    {
        requireNonNull(outputTypes, "outputTypes is null");
        requireNonNull(keyOutputChannels, "keyOutputChannels is null");
        requireNonNull(keyOutputHashChannel, "keyOutputHashChannel is null");
        requireNonNull(driverContext, "driverContext is null");
        requireNonNull(maxMemoryInBytes, "maxMemoryInBytes is null");
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        checkArgument(expectedPositions > 0, "expectedPositions must be greater than zero");

        this.pagesIndexFactory = pagesIndexFactory;
        this.session = driverContext.getSession();
        this.outputTypes = ImmutableList.copyOf(outputTypes);
        this.expectedPositions = expectedPositions;
        this.keyOutputChannels = ImmutableList.copyOf(keyOutputChannels);
        this.keyOutputHashChannel = keyOutputHashChannel;
        this.maxMemoryInBytes = maxMemoryInBytes.toBytes();

        ImmutableList.Builder<Type> missingKeysTypes = ImmutableList.builder();
        ImmutableList.Builder<Integer> missingKeysChannels = ImmutableList.builder();
        for (int i = 0; i < keyOutputChannels.size(); i++) {
            Integer keyOutputChannel = keyOutputChannels.get(i);
            missingKeysTypes.add(outputTypes.get(keyOutputChannel));
            missingKeysChannels.add(i);
        }
        this.missingKeysTypes = missingKeysTypes.build();
        this.missingKeysChannels = missingKeysChannels.build();

        this.outputPagesIndex = pagesIndexFactory.newPagesIndex(outputTypes, expectedPositions);
        this.missingKeysIndex = pagesIndexFactory.newPagesIndex(missingKeysTypes.build(), expectedPositions);
        this.missingKeys = missingKeysIndex.createLookupSourceSupplier(session, this.missingKeysChannels).get();

        this.missingKeysPageBuilder = new PageBuilder(missingKeysIndex.getTypes());
    }

    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    public long getMemoryInBytes()
    {
        return memoryInBytes;
    }

    public boolean isMemoryExceeded()
    {
        return memoryInBytes > maxMemoryInBytes;
    }

    public boolean tryAddPage(Page page)
    {
        memoryInBytes += page.getSizeInBytes();
        if (isMemoryExceeded()) {
            return false;
        }
        pages.add(page);
        return true;
    }

    public IndexSnapshot createIndexSnapshot(UnloadedIndexKeyRecordSet indexKeysRecordSet)
    {
        checkArgument(indexKeysRecordSet.getColumnTypes().equals(missingKeysTypes), "indexKeysRecordSet must have same schema as missingKeys");
        checkState(!isMemoryExceeded(), "Max memory exceeded");
        for (Page page : pages) {
            outputPagesIndex.addPage(page);
        }
        pages.clear();

        LookupSource lookupSource = outputPagesIndex.createLookupSourceSupplier(session, keyOutputChannels, keyOutputHashChannel, Optional.empty(), Optional.empty(), ImmutableList.of()).get();

        // Build a page containing the keys that produced no output rows, so in future requests can skip these keys
        verify(missingKeysPageBuilder.isEmpty());
        UnloadedIndexKeyRecordCursor indexKeysRecordCursor = indexKeysRecordSet.cursor();
        while (indexKeysRecordCursor.advanceNextPosition()) {
            Page page = indexKeysRecordCursor.getPage();
            int position = indexKeysRecordCursor.getPosition();
            if (lookupSource.getJoinPosition(position, page, page) < 0) {
                missingKeysPageBuilder.declarePosition();
                for (int i = 0; i < page.getChannelCount(); i++) {
                    Block block = page.getBlock(i);
                    Type type = indexKeysRecordCursor.getType(i);
                    type.appendTo(block, position, missingKeysPageBuilder.getBlockBuilder(i));
                }
            }
        }
        Page missingKeysPage = missingKeysPageBuilder.build();
        if (!missingKeysPageBuilder.isEmpty()) {
            missingKeysPageBuilder.reset();
        }

        memoryInBytes += missingKeysPage.getSizeInBytes();
        if (isMemoryExceeded()) {
            return null;
        }

        // only update missing keys if we have new missing keys
        if (missingKeysPage.getPositionCount() != 0) {
            missingKeysIndex.addPage(missingKeysPage);
            missingKeys = missingKeysIndex.createLookupSourceSupplier(session, missingKeysChannels).get();
        }

        return new IndexSnapshot(lookupSource, missingKeys);
    }

    public void reset()
    {
        memoryInBytes = 0;
        pages.clear();
        outputPagesIndex = pagesIndexFactory.newPagesIndex(outputTypes, expectedPositions);
        missingKeysIndex = pagesIndexFactory.newPagesIndex(missingKeysTypes, expectedPositions);
    }
}
