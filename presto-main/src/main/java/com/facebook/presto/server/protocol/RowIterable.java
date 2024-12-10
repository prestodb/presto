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
package com.facebook.presto.server.protocol;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class RowIterable
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final List<Type> types;
    private final Page page;

    RowIterable(ConnectorSession session, List<Type> types, Page page)
    {
        this.session = session;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.page = requireNonNull(page, "page is null");

        validateBlockSizesInPage(page);
    }

    @Override
    public Iterator<List<Object>> iterator()
    {
        return new RowIterator(session, types, page);
    }

    private static class RowIterator
            extends AbstractIterator<List<Object>>
    {
        private final ConnectorSession session;
        private final List<Type> types;
        private final Page page;
        private int position = -1;

        private RowIterator(ConnectorSession session, List<Type> types, Page page)
        {
            this.session = session;
            this.types = types;
            this.page = page;

            validateBlockSizesInPage(page);
        }

        @Override
        protected List<Object> computeNext()
        {
            position++;
            if (position >= page.getPositionCount()) {
                return endOfData();
            }

            List<Object> values = new ArrayList<>(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                Type type = types.get(channel);
                Block block = page.getBlock(channel);
                // Caused by: java.lang.IllegalArgumentException: Invalid position 1 in block with 1 positions
                // why does block onlly have one position when we're asking for at least 12?
                // It's not an off by one error. It's that there's only one row ID in the block.
                // That's why it works with LIMIT 1 and not LIMIT 2
                // maybe an issue with compressed dictionary blocks or something like that?
                // only some tables/rows use dictionary blocks
                // and exception changes from LIMIT 2 to 3
                values.add(type.getObjectValue(session.getSqlFunctionProperties(), block, position));
            }
            return Collections.unmodifiableList(values);
        }
    }

    private static void validateBlockSizesInPage(Page page)
    {
        // debugging
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            if (block.getPositionCount() != page.getPositionCount()) {
                throw new IllegalArgumentException(
                    format("Wrongly sized block: channel: %d block poition count: %d page position count: %d %s",
                    channel, block.getPositionCount(), page.getPositionCount(), block.getClass().getCanonicalName()));
            }
        }
    }
}
