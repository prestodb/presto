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
package io.prestosql.server.protocol;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static java.util.Objects.requireNonNull;

class RowIterable
        implements Iterable<List<Object>>
{
    private final ConnectorSession session;
    private final List<Type> types;
    private final Page page;

    public RowIterable(ConnectorSession session, List<Type> types, Page page)
    {
        this.session = session;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.page = requireNonNull(page, "page is null");
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
                values.add(type.getObjectValue(session, block, position));
            }
            return Collections.unmodifiableList(values);
        }
    }
}
